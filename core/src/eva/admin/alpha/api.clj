;; Copyright 2015-2019 Workiva Inc.
;; 
;; Licensed under the Eclipse Public License 1.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;; 
;;      http://opensource.org/licenses/eclipse-1.0.php
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns eva.admin.alpha.api
  "Namespace for collecting a suite of tools for administrative tasks on eva
   databases, such as migration and backup."
  (:require [eva.v2.storage.value-store.concurrent :as vs]
            [eva.v2.database.log :as log]
            [eva.admin.alpha.traversal :as t]
            [eva.admin.graph.alpha :as graph]
            [flowgraph.protocols :as decurp]
            [eva.v2.storage.value-store :as vs-api :refer [create-key]]
            [quartermaster.core :as quartermaster]
            [recide.sanex.logging :as logging]
            [recide.core :as rc])
  (:import (java.util Date)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn cas-node! [store cur-n new-n]
  (if (nil? cur-n)
    (let [created? @(create-key store (t/k new-n) (t/v new-n))]
      (when-not created?
        (throw (IllegalStateException. (format "Failed to create key %s" (t/k new-n))))))
    (if-not (= (t/k cur-n) (t/k new-n))
      (throw (IllegalStateException. (format "Failed to cas, current and new keys differ.")))
      (let [replaced? @(vs-api/replace-value store (t/k new-n) (t/v cur-n) (t/v new-n))]
        (when-not replaced?
          (throw (IllegalStateException. (format "Failed to update key %s" (t/k new-n)))))))))

(defrecord MigrationStatus [key
                            status
                            tx-num
                            last-updated]
  t/Traversable
  (k [_] key)
  (v [this] this))

(defn now [] (Date.))
(defn migration-status [key status tx-num] (->MigrationStatus key status tx-num (now)))

(defn migrate-batch [{:as migration-info-blob
                      :keys [source-tx-log dest-store graph context
                             cur-migration-status
                             status-key]} tx-nums]
  (assert (apply < tx-nums))
  (logging/debugf "migrating log entries %s" (into [] tx-nums))
  (let [before @context
        log-entries (doall (map (partial t/shim-log-entry source-tx-log) tx-nums))]
    @(decurp/submit graph log-entries)
    (let [new-status (migration-status status-key :in-progress (last tx-nums))]
      (cas-node! @dest-store cur-migration-status new-status)
      (assoc migration-info-blob :cur-migration-status new-status))))

(defn stateful-migration!
  "ALPHA

  Performs a stateful migration of all data from the source store to
  the destination store. Will actively read from both the source and
  destination stores to provide both continuous and iterative migrations.

  Periodically records the migration progress using a specific key-value
  pair in the destination store.

  Options:

    :log-every
    specifies that the status of the migration should be
    persisted to the destination store every <log-every> tx-log-entries that
    are migrated.
    Defaults to 25.

    :migration-status-key-suffix
    provide a suffix for looking up and recording the state of a migration. The
    key will be of the form: <database-id>.<migration-status-key-suffix>
    Defaults to 'migration-status'.

    :write-xform (NOT YET IMPLEMENTED)
    an optional xform on Traversable->Traversable that will be applied
    before nodes are written. Value store operations will be applied to the
    post-xform value, so create and replace in particular will have their
    restrictions applied to the post-transformation object.
    Great Caution *MUST* be exercised with xforms that alter the keys or values
    of traversable objects.
    Defaults to (map identity).

    :read-xform (NOT YET IMPLEMENTED)
    an optional xform that is passed to the underlying traversals.
    Defaults to (traversal/unique-on traversal/k)

  Properties:
  1. Performs a lazy DFS traversal of the transaction log from either the
     start of the tx-log, or the last transaction log entry noted in the
     recorded migration state.
  2. Stateful migrations are safe to run against an active source database.
  3. Stateful migrations should *not* be run against an active destination
     database. They will likely fail in unexpected ways.
  4. Stateful migrations can safely resume when unexpectedly halted part-way
     through, and will continue / resume migrations that have completed if the
     source database has advanced since the previous migration. If operating on
     a previously-completed stateful migration, will CAS on the transaction log
     head and database-info to ensure that no constraints are violated.
  5. Will *not* overwrite extant nodes. To ensure this property is not violated,
     create-key with a try-catch pattern is used. If the create fails, we assert
     that the persisted node is equivalent to what we attempted to write.
  6. Simple migrations are agnostic to storage location but are *not* agnostic
     to the database constructs. To provide iterative and continuous migrations,
     a working knowledge of the transaction log representation is required.
  7. Writes occur in batches and follow a postordering traversal of stored data.
     This ordering ensures that all references under a node must exist before
     the node itself exists. This property is slightly violated when a completed
     migration is sourced against a database that has advanced: the tx-log
     root will exist and will not be updated until all new source tx-log
     entries have been migrated."
  [source-storage-config destination-storage-config database-id
   & {:as options
      :keys [log-every
             migration-status-key-suffix
             #_read-xform
             #_write-xform
             active-parents]
      :or {log-every 25
           migration-status-key-suffix "migration-status"
           #_read-xform #_(t/unique-on t/k)
           #_write-xform #_(map identity)}}]
  (quartermaster/acquiring
   [source-store (quartermaster/acquire vs/concurrent-value-store-manager
                                   (gensym ::flowgraph-migration-source)
                                   source-storage-config)
    dest-store (quartermaster/acquire vs/concurrent-value-store-manager
                                 (gensym ::flowgraph-migration-destination)
                                 destination-storage-config)

    status-key (format "%s.%s" database-id migration-status-key-suffix)
    cur-migration-status @(vs-api/get-value @dest-store status-key)

    source-database-info (t/shim-database-info source-store database-id)
    source-tx-log (log/open-transaction-log source-store source-database-info)

    dest-database-info (rc/try* (t/shim-database-info dest-store database-id)
                                (catch :database-catalogue/no-such-database-info t nil))
    dest-tx-log (rc/try* (log/open-transaction-log dest-store source-database-info)
                         (catch :datastructures/no-such-vector t nil))

    max-known-completed-tx (or (:tx-num cur-migration-status) -1)
    start-t (inc max-known-completed-tx)
    source-tx-count (count source-tx-log)

    context (graph/migration-context dest-store)
    graph (graph/migration-graph context)

    init-migration-status (migration-status status-key :in-progress max-known-completed-tx)

    migration-info-blob {:graph graph
                         :dest-store dest-store
                         :source-tx-log source-tx-log
                         :cur-migration-status init-migration-status
                         :status-key status-key
                         :context context}]

   (try
     (do
       (logging/debugf "found migration status %s for database %s" (into {} cur-migration-status) database-id)
       (logging/debugf "starting migration for %s from tx %s to tx %s" database-id start-t source-tx-count)

       (cas-node! @dest-store cur-migration-status init-migration-status)

       (let [final-info (reduce migrate-batch
                                migration-info-blob
                                (partition-all log-every (range start-t source-tx-count)))
             final-migration-status (:cur-migration-status final-info)]
         (cas-node! @dest-store dest-database-info source-database-info)
         (cas-node! @dest-store dest-tx-log source-tx-log)

         (let [completed-status (migration-status status-key :complete (dec source-tx-count))]
           (cas-node! @dest-store final-migration-status completed-status)
           (logging/debugf "completed migration, wrote status: %s" (into {} completed-status)))
         final-info))
     (finally (quartermaster/release source-store true)
              (quartermaster/release dest-store true)
              (decurp/shutdown graph)))))
