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

(ns eva.api
  (:require [eva.core :as core]
            [eva.functions :as functions]
            [eva.query.core :as qdc]
            [eva.readers :refer [ensure-parsed]]
            [eva.entity-id :as entity-id]
            [eva.v2.system.peer-connection.autogenetic :as autogen]
            [eva.v2.system.peer-connection.core :as v2peer]
            [eva.error :as err :refer [eva-ex]]
            [eva.contextual.core :as c]
            [eva.contextual.tags :as ct]
            [eva.utils.logging :refer [logged]]
            [eva.utils.tracing :refer [trace-fn-with-tags]]
            [utiliva.uuid :as uuid]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]
            [clojure.spec.alpha :as s])
  (:import (eva Connection Database Entity Log)))

(err/deferror-group ^:no-doc syntax-error
  :api-syntax-malformed
  (transact-api "Received bad input to the eva.api/transact call")
  (query-api "Received bad input to the eva.api/q call")
  (pull-api "Received bad input to the eva.api/pull or eva.api/pull-many call")
  (connect-api "Received bad input to the eva.api/connect call")
  (resolve-tempid "Received bad input to the eva.api/resolve-tempid call"))

;; Mark `raise-syntax-error` as private
(alter-meta! #'raise-syntax-error assoc :private true)

(err/deferror ^:no-doc nyi
  :api/not-yet-implemented
  "This method not yet implemented or supported"
  [:method])

;; Mark `raise-nyi` as private
(alter-meta! #'raise-nyi assoc :private true)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection APIs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Connection lifecycle
(d/defn ^{::d/aspects [eva-ex
                       traced
                       (logged)
                       (c/capture '(ct/config->tags config))
                       (c/timed [::ct/database-id])]}
  ^Connection  connect*
  [connect-fn config]
  (connect-fn config))


;; Connection lifecycle
(defn ^Connection connect
  "Returns a `Connection`, given a valid connection configuration map."
  [uri]
  (if (instance? java.util.Map uri)
    (let [config           (into {} uri)
          [config conn-fn] (if (s/valid? ::autogen/minimal-config config)
                             [(autogen/expand-config config) autogen/connect]
                             [config v2peer/connect])]
      (connect* conn-fn config))
    (raise-syntax-error :connect-api
                        (format "Expected Map on connect call. Got %s" (type uri))
                        {:connection-map uri})))


(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed [::ct/database-id])]}
  release
  "Releases the resources associated with this `Connection`.

  Connections are intended to be long-lived, so you should release
  a `Connection` only when the entire program is finished with it
  (for example at program shutdown)."
  [^Connection conn] (.release conn))

(d/defn ^{::d/aspects [traced (logged) (c/timed)]} connection-status
  "Returns status information on a given `Connection`"
  [^Connection conn] (.status conn))

;; reified times
(defn latest-t
  "Returns the `t` of the most recent transaction reachable via this `db` value."
  [^eva.Connection conn] (.latestT conn))

;; direct tx-log access
(defn log
  "Returns the current value of the transaction log.

  Communicates with storage, but not with the transactor

  Can be used in conjunction with [`tx-range`](#var-tx-range) or a [`query`](#var-q)."
  [^Connection conn] (.log conn))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/log->tags log))
                       (c/timed [::ct/database-id])]}
  tx-range
  "Given a `Log` object and a start and end `transaction number` or `id`, return all
  applicable datoms that were asserted or retracted in the database."
  [^Log log start end] (.txRange log start end))

;; transaction submission
(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed [::ct/database-id])]}
  transact
  "Submits a transaction, blocking until a result is available.

  `tx-data` is a list of operations to be processed, including assertions,
  retractions, functions, or entity-maps

  Returns a CompletableFuture which can monitor the status of the transaction.
  On successful commit, the future will contain a map of the following keys:

  - `:db-before` database value before transaction was applied
  - `:db-after` database value after transaction was applied
  - `:tx-data` collection of primitive operations performed by the transaction
  - `:temp-ids` can be used with [`resolveTempId`](#var-resolve-tempid) to
  resolve temporary ids  used in the txData

  If the transaction fails or timed out, attempts to .get() the future's value
  will raise an `ExecutionException`.
  When getting the result of the future, catch `ExecutionException` and call
  `ExecutionException#getCause()` to retrieve the underlying error."
  [^Connection conn tx-data] (.transact conn tx-data))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed)]}
  transact-async
  "Like [`transact`](#var-transact) but returns immediately without waiting for
  the transaction to complete."
  [^Connection conn tx-data] (.transactAsync conn tx-data))

;; database snapshot acquisition
(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed)]} ^Database
  db
  "Yields the most recently known database snapshot, which is maintained in memory."
  [^Connection conn] (.db conn))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed [::ct/database-id])]} ^Database
  sync-db
  "Like [`db-snapshot`](#var-db-snapshot), but forces reads from the backing store to assert whether or
  not the in-memory database snapshot is stale before returning. If the
  snapshot is found to be stale, this call will block until the updated
  snapshot is produced from storage.

  Communicates only with storage.

  Intended for use when stale state on a `Peer` is suspected."
  [^Connection conn] (.syncDb conn))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/conn->tags conn))
                       (c/timed [::ct/database-id])]} ^Database
  db-snapshot
  "Yields the most recently known database snapshot, which is maintained in memory."
  [^Connection conn] (.dbSnapshot conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Snapshot APIs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; reified times
(defn as-of-t
  "If `db` isn't the latest snapshot at the time it was constructed, return the
  `tx-num` of the log entry / transaction that `db` was constructed from to reflect; else `nil`"
  [^eva.Database db] (.asOfT db))

(defn snapshot-t
  "Return the `tx-num` of the log entry / transaction that `db` constructed from to reflect; else `nil`"
  [^eva.Database db] (.snapshotT db))

(defn basis-t
  "Return the latest `tx-num` the `Peer` knew about at the time `db` was constructed; else `nil`"
  [^Database db] (.basisT db))

;; Database Snapshot production / 'modification'
(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]}
  as-of
  "Returns a historical database value at some point in time `t`, inclusive.
  `t` can be a transaction number or a transaction id"
  [^Database db t] (.asOf db t))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]}
  history
  "Returns a special view of the database containing **all** datoms, asserted
  or retracted across time. Currently supports the [`datoms`](#var-datoms) and [`q`](#var-q) functions."
  [^Database db] (.history db))

(defn with
  "Simulates a transaction locally without persisting the updated state.
   Returns a map with the same contents as the future from [`transact`](#var-transact)"
  [^eva.Database db tx-data] (.with db tx-data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Snapshot Read APIs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Datoms API
(d/defn ^{::d/aspects [eva-ex
                       traced
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]}
  datoms
  "Provides raw access to the database indexes.
  Must pass the index-name. May pass one or more leading components of the index
  to constrain the results.

  The following indexes may be searched:

  - `:eavt` - contains all datoms sorted by entity-id, attribute-id, value, and transaction
  - `:aevt` - contains all datoms sorted by attribute-id, entity-id, value, and transaction
  - `:avet` - contains datoms with indexed and unique attributes (except for bytes values)
  sorted by attribute-id, value, entity-id, and transaction
  - `:vaet` - contains datoms with attributes of :db.type/ref; VAET acts as the reverse
  index for backwards traversal of refs"
  [^Database db index & components]
  (core/select-datoms db (cons index components)))

;; Entity API
(defn entity
  "Returns an `Entity`, which is a dynamic, lazy-loaded projection of the datoms
  that share the same entity-id"
  [^Database db eid] (.entity db eid))
(defn entity-db
  "Returns the database value that backs this entity"
  [^Entity entity] (.db entity))
(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/db->tags (.db entity)))
                       (c/timed [::ct/database-id])]} touch
  "Loads all attributes of the entity, recursively touching any component entities"
  [^Entity entity] (.touch entity))

;; Pull API
(d/defn ^{::d/aspects [(logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]}
  pull
  "Executes a pull-query returning a hierarchical selection of attributes for entityId"
  [^Database db pattern eid]
  (trace-fn-with-tags "eva.api/pull:[db pattern eid]" #(.pull ^Database %1 %2 %3) db pattern eid))

(d/defn ^{::d/aspects [(logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]}
  pull-many
  "Returns multiple hierarchical selections for the passed entity-identifiers"
  [^Database db pattern eids]
  (trace-fn-with-tags "eva.api/pull-many:[db pattern eids]" #(.pullMany ^Database %1 %2 %3) db pattern eids))

;; Query API
(d/defn ^{::d/aspects [eva-ex
                       (logged)
                       (c/timed)]}
  q
  "[Datomic Documentation](https://docs.datomic.com/on-prem/clojure/index.html#datomic.api/q)"
  [query & inputs]
  (apply trace-fn-with-tags "eva.api/q:[query & inputs]" qdc/q (ensure-parsed query) inputs))

;; Existence API
(d/defn ^{::d/aspects [eva-ex
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed)]}
  extant-entity?
  "Returns true if there exists at least one datom in the database with the
  provided entity identifier."
  [^Database db identifier] (.isExtantEntity db identifier))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Snapshot Coercion APIs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]} entid
  "Coerces any entity-identifier into an entity-id.
  Does **not** confirm existence of an entity id, except incidentally through
  some coercion processes.
  To check for existence of an entity, please use [isExtantEntity](#var-extant-entity.3F)
  "[^Database db identifier]
  (.entid db identifier))

(d/defn ^{::d/aspects [traced
                       (logged)
                       (c/capture '(ct/db->tags db))
                       (c/timed [::ct/database-id])]} entid-strict
  "Behaves the same as [`entid`](#var-entid), but instead throws where `entid` would return nil."
  [^Database db identifier]
  (.entidStrict db identifier))

(defn ident
  "Returns the `keyword-identifier` associated with an id."
  [^Database db id] (.ident db id))
(defn ident-strict
  "Returns the `keyword-identifier` associated with an id.  Exception is thrown if none exists"
  [^Database db id] (.identStrict db id))

(defn attribute
  "Retrieves information about an `Attribute`."
  [^Database db attr-id] (.attribute db attr-id))
(defn attribute-strict
  "Retrieves information about an `Attribute`. Exception is thrown if none exists"
  [^Database db attr-id] (.attributeStrict db attr-id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Miscellaneous APIs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(d/defn ^{::d/aspects [eva-ex]} tempid
  "Construct a temporary id within the specified partition.
  Tempids will be mapped to permanent ids within a single transaction.

  User-created tempids are reserved for values of `n` within the range of -1 to -1000000 inclusive."
  ([part] (eva.entity-id/tempid part))
  ([part n] (eva.entity-id/tempid part n)))

(defn part
  "Return the partition associated with the specified entity id."
  [eid] (entity-id/partition eid))

(d/defn ^{::d/aspects [eva-ex]} resolve-tempid
  "Resolves a temporary-id to a permanent id given a database and a mapping of tempid to permids.

  Intended for use with the results of a [`transact`](#var-transact) call:

  ```clj
  (let [tid (eva/tempid :db.part/user -1)
        result @(eva/transact conn [[:db/add tid :db/ident :foo]])]
      {:temp-id tid
       :perm-id (eva/resolve-tempid (:db-after result) (:tempids result) tid)})
  ```

  If a tempid is not passed, then a function is returned that will resolve tempids
  using the given db and tempids-mapping. This form should be used when resolving
  a collection of tempids.

  ```clj
  (let [t1 (eva/tempid :db.part/user -1)
        t2 (eva/tempid :db.part/user -2)
        t3 (eva/tempid :db.part/user -3)
        result @(eva/transact conn [[:db/add t1 :db/ident :foo]
                                    [:db/add t2 :db/ident :bar]
                                    [:db/add t3 :db/ident :baz]])]
      (map (eva/resolve-tempid (:db-after result) (:tempids result)) [t1 t2 t3]))
  ```"
  ([db tempids]
   (let [tempids (if (map? tempids) tempids (into {} tempids))]
     (fn tempid-resolver [tempid]
       (when-not (entity-id/temp? tempid)
         (raise-syntax-error :resolve-tempid
                             "not a tempid"
                             {:argument tempid}))
       (let [tempid (->> tempid
                         (core/resolve-eid-partition db)
                         (entity-id/->Long))]
         (get tempids tempid)))))
  ([db tempids tempid] ((resolve-tempid db tempids) tempid)))

(defn squuid
  "Constructs a semi-sequential `UUID`.  Can be useful for having a unique
  identifier that does not fragment indexes"
  []
  (uuid/squuid))

(defn squuid-time-millis
  "Returns the time component of a [`squuid`](#var-squuid), in the format of
  `System.currentTimeMillis`"
  [uuid] (uuid/squuid-time-millis uuid))

(d/defn ^{::d/aspects [eva-ex traced]} function
  "[Datomic Documentation](https://docs.datomic.com/on-prem/clojure/index.html#datomic.api/function)"
  [m] (functions/build-db-fn m))

(d/defn ^{::d/aspects [eva-ex]} to-tx-num
  "Takes a `tx-num` or `tx-eid` and returns the equivalent `tx-num`"
  [tx-num-or-eid]
  (entity-id/->tx-num tx-num-or-eid))

(d/defn ^{::d/aspects [eva-ex]} to-tx-eid
  "Takes a `tx-num` or `tx-eid` and returns the equivalent `tx-eid`"
  [tx-num-or-eid]
  (entity-id/->tx-eid tx-num-or-eid))

(defn invoke
  "Looks up a database function identified by the `eid` or `ident` and invokes
  the function with the provided `args`"
  [^Database db eid-or-ident & args]
  (.invoke db eid-or-ident (into-array Object args)))
