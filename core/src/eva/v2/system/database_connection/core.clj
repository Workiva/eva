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

(ns eva.v2.system.database-connection.core
  (:require [eva.core :refer [entry->datoms] :as core]
            [eva.v2.database.core :as database]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.value-store.gcached :as vs]
            [utiliva.core :refer [locking-vswap!]]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.system.database-catalogue.core :as dbc-core]
            [eva.v2.system.protocols :as p]
            [eva.v2.database.core :refer [log->db]]
            [quartermaster.core :as qu]
            [eva.db-update :as db-api]
            [eva.defaults :as defaults]
            [eva.v2.fressian :refer [eva-write-handlers eva-read-handlers]]
            [eva.v2.database.log :refer [open-transaction-log set-log-count]]
            [eva.v2.datastructures.vector :as dsv]
            [eva.v2.database.index-manager :as im]
            [eva.v2.transaction-pipeline.core :as tx]
            [eva.v2.transaction-pipeline.error :as tx-err]
            [eva.utils :refer [with-retries]]
            [utiliva.uuid :refer [squuid]]
            [eva.error :refer [error? raise insist] :as err]
            [barometer.core :as m]
            [recide.core :refer [try*]]
            [recide.sanex.logging :as log]
            [eva.contextual.core :as c]
            [manifold.deferred :as d]
            [morphe.core :as defm]
            [ichnaie.core :refer [traced]])
  (:import (eva.error.v1 EvaException)
           (eva Database)))

(def ^:dynamic *database-connection-timeout-ms* 10000)

(defm/defn ^{::defm/aspects [traced]} do-log-append! [tx-log log-entry]
  (when-not (= (count tx-log)
               (:tx-num log-entry))
    (raise :database-connection/mismatch-log-entry
           (format "Cannot transact log tx-num %s against tx-log with count %s" (:tx-num log-entry) (count tx-log))
           {:tx-num (:tx-num log-entry)
            :count (count tx-log)}))
  (with-retries (defaults/write-tx-log-retry-strategy)
    "encountered exception while attempting to write to tx log; retrying."
    (conj tx-log log-entry)))

(defn max-item [key-fn items] (first (sort-by key-fn > items)))
(defn index-roots:merge-newer [& ms] (apply merge-with #(max-item :tx-num [%1 %2]) ms))
(defn logentry:with-newer-index-roots
  [log-entry & ms]
  (update log-entry :index-roots (partial apply index-roots:merge-newer) ms))

(def stale-write-failure ::stale-write-failure)
(defn stale-write-failure? [obj]
  (identical? stale-write-failure obj))

(defrecord DatabaseConnectionState [config res-id database-id database-info value-store tx-log db latest-index-roots]
  p/DatabaseConnectionState
  (stale-snapshot? [this db-snapshot]
    (let [tx-num (core/tx-num db-snapshot)
          tx-log (open-transaction-log value-store database-info)]
      (> (count tx-log) (inc tx-num))))
  (update-index-roots [this index-update]
    (assoc this :latest-index-roots (index-roots:merge-newer latest-index-roots index-update)))
  (advance-to-tx-num [this tx-num]
    (if-not (>= tx-num (count tx-log))
      this
      (let [tx-log' (set-log-count tx-log (inc tx-num))
            db' (db-api/advance-db-to-tx db tx-log' tx-num)]
        (assoc this :tx-log tx-log' :db db'))))
  (append-to-log [this log-entry timeout-ms]
    ;; TODO: refactor to work with lower-level timeouts instead of injecting
    ;;       this random one via a future here?
    (let [tx-log' (deref (future (do-log-append! tx-log log-entry))
                         timeout-ms ::timeout)]
      (if (= ::timeout tx-log')
        (raise :tx-log/append-timeout
               "timeout while appending to log"
               :timeout   timeout-ms)
        (assoc this :tx-log tx-log'))))
  (commit-transaction-report [this tx-report]
    (p/commit-transaction-report this tx-report *database-connection-timeout-ms*))

  (advance-db-from-log-entry [this tx-log-entry]
    (update this :db db-api/safe-advance-db tx-log-entry))

  (commit-transaction-report [this {:keys [log-entry db-before db-after]} timeout-ms]
    (when (not= db db-before)
      (raise :database-connection/unexpected-db-state
             "Cannot commit transaction, database in unexpected state"
             {:actual-tx-num (core/tx-num db-before)
              :expected-tx-num (core/tx-num db)}))
    ;; NOTE: The following ordering is **critical** because of the
    ;; caching behavior in database snapshot construction.
    ;;
    ;; We **CANNOT** create a database snapshot involving any index caching
    ;; before we've confirmed that the log has been updated.
    (-> this
        (p/append-to-log log-entry timeout-ms)
        (p/advance-db-from-log-entry log-entry)))
  (repair-log [this]
    (try*
     (update this :tx-log dsv/repair-vector-head value-store)
     (catch :datastructures/concurrent-modification e
       (log/warn "database connection saw concurrent attempts to repair log, ignoring.")
       this)))
  (reload [this]
    (let [tx-log (open-transaction-log value-store database-info)
          db (log->db database-info value-store tx-log)]
      (assoc this
             :db db
             :tx-log tx-log
             :latest-index-roots (-> db :log-entry :index-roots)))))

(def staleness-meter (m/meter "meter of how many times we've had to reload state from staleness"))
(m/register m/DEFAULT (str *ns* ".staleness-meter") staleness-meter)

(defrecord DatabaseConnectionImpl [resource-id database-id config value-store state]
  qu/SharedResource
  (resource-id [this] (some-> resource-id deref))
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [resource-id (qu/new-resource-id)
                     database-id (::database/id config)
                     value-store (qu/acquire vs-manager/value-store-manager resource-id config)
                     database-info (dbc-core/database-info value-store database-id)
                     state (->
                            (DatabaseConnectionState. config resource-id database-id database-info value-store nil nil nil)
                            (p/reload)
                            (volatile!))]
                    (assoc this
                           :resource-id (atom resource-id)
                           :database-id database-id
                           :state state
                           :value-store value-store))))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id @resource-id]
        (reset! resource-id nil)
        ((im/->evict-callback database-id)) ;; TODO: Is this correct? What is ->evict-callback?
        (qu/release value-store true)
        (assoc this
               :state nil
               :value-store nil))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do (qu/force-terminate value-store)
          (qu/terminate this))))
  p/DatabaseConnection
  (db-snapshot [this]
    (qu/ensure-initiated! this "cannot get db-snapshot.")
    (-> @state :db))
  (log [this]
    (qu/ensure-initiated! this "cannot get log.")
    (-> @state :tx-log))
  (update-latest-index-roots [this index-update]
    (qu/ensure-initiated! this "cannot update indices.")
    (locking-vswap! state p/update-index-roots index-update)
    this)
  (process-tx [this tx-data]
    (qu/ensure-initiated! this "cannot process transaction.")
    (let [{:keys [db latest-index-roots]} @state
          [db tx-result] (loop [db db]
                           (let [tx-info (tx/transact db tx-data)]
                             (if-let [res (tx/tx-result tx-info)]
                               [db res]
                               (let [ex (tx/tx-exception tx-info)]
                                 (cond
                                   (p/stale-snapshot? @state db) ;; did we evaluate against a stale db?
                                   (do
                                     (m/update staleness-meter)
                                     (log/warn "found transactor to be in a stale state -- reloading state and retrying transaction.")
                                     (let [hopefully-not-stale-db (:db (locking-vswap! state p/reload))]
                                       (recur hopefully-not-stale-db)))

                                   (instance? EvaException ex)
                                   (throw ex)

                                   :else
                                   (raise :transaction-processing-error
                                          "unexpected error occurred while processing transaction"
                                          {:tx-data tx-data}
                                          ex))))))]
      (when-not (:successful tx-result)
        (raise ::transaction-processing-failed
               "transaction processing failed"
               {:tx-data tx-data
                :tx-result tx-result}))
      ;; NOTE: the db-after snapshot **CANNOT** be created at this point, if
      ;;       the snapshot is constructed through the index root cache.
      ;;       Under concurrent modification of the underlying transaction log
      ;;       it's still possible for this transaction to be invalidated.
      (let [{:keys [log-entry tempids]} tx-result
            log-entry (logentry:with-newer-index-roots log-entry latest-index-roots)]
        {:committed? false
         :log-entry  log-entry
         :db-before  db
         :tempids    tempids
         :tx-data    (:tx-data tx-result)})))
  (repair-and-reload [this]
    (qu/ensure-initiated! this "cannot repair or reload.")
    (locking-vswap! state (comp p/reload p/repair-log))
    this)
  (reload-if-stale [this]
    (:db (locking-vswap! state
                         (fn [state]
                           (if (p/stale-snapshot? state (:db state))
                             (p/reload state)
                             state)))))
  (commit-tx [{:as this :keys [state]} tx-data]
    (qu/ensure-initiated! this "cannot commit transaction.")
    (locking this
      (log/trace "commit-tx: " tx-data)
      (let [tx-report (p/process-tx this tx-data)]
        (insist (:log-entry tx-report) "tx-report missing :log-entry")
        (log/trace "Commiting tx-num:" (-> tx-report :log-entry :tx-num))
        (try
          (let [state-after (locking-vswap! state p/commit-transaction-report tx-report)]
            (assoc tx-report :db-after (:db state-after)))
          (catch java.util.concurrent.ExecutionException e
            (let [e (.getCause e)]
              (if (or (error? e :datastructures/stale)
                      (error? e :database-connection/mismatch-log-entry))
                stale-write-failure
                (throw e))))))))
  (advance-to-tx [this tx-num]
    (qu/ensure-initiated! this "cannot advance.")
    (let [state' (locking-vswap! state p/advance-to-tx-num tx-num)]
      (:db state')))
  (handle-transaction-message [{:as this :keys [state]} tx-msg]
    (qu/ensure-initiated! this "cannot handle messages.")
    (let [tx-num (get-in tx-msg [:log-entry :tx-num])
          db-now ^Database (p/advance-to-tx this tx-num)]
      {:tempids (:tempids tx-msg)
       :tx-data (-> tx-msg :log-entry entry->datoms)
       :db-before (.asOf db-now (dec tx-num))
       :db-after (.asOf db-now tx-num)})))

(qu/defmanager database-connection-manager
  :discriminator
  (fn [user-id config] [user-id (::database/id config) (::values/partition-id config)])
  :constructor
  (fn [[_ db-id] config]
    (map->DatabaseConnectionImpl {:database-id db-id
                                  :config config})))
