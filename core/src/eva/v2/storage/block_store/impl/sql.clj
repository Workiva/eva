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

(ns eva.v2.storage.block-store.impl.sql
  (:require [eva.config :as config]
            [recide.core :refer [try*]]
            [quartermaster.core :as qu]
            [recide.sanex :as sanex :refer [sanitize]]
            [recide.sanex.logging :as log]
            [eva.v2.storage.error :refer [raise-sql-err]]
            [eva.v2.storage.core :as block :refer [BlockStorage ->Block]]
            [eva.v2.storage.block-store.types :as types]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging]
            [again.core :as again])
  (:import [eva ByteString ByteStringInputStream]
           [java.util Map]
           [java.io File]
           [javax.sql DataSource]
           [java.sql Blob DriverManager PreparedStatement SQLFeatureNotSupportedException]
           [java.net URI]
           [com.mchange.v2.c3p0.impl C3P0ImplUtils]
           [com.mchange.v2.c3p0 DataSources]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::db-spec (partial instance? Map))
(s/def ::config map?)

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

;;;;;;;;
;; H2 ;;
;;;;;;;;

(def h2-schema-ddl
  (str "CREATE TABLE IF NOT EXISTS "
       "eva_kv( "
       "namespace varchar(128), "
       "id varchar(128), "
       "attrs varchar(600), "
       "val blob, "
       "primary key (namespace, id)"
       ")"))

(defn h2-db-spec
  [path]
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname     (str path) ;;  ";MVCC=TRUE"
   :user        "sa"})

(def sqlite-schema-ddl
  (str "CREATE TABLE IF NOT EXISTS "
       "eva_kv( "
       "namespace varchar(128), "
       "id varchar(128), "
       "attrs varchar(600), "
       "val blob, "
       "primary key (namespace, id) "
       ")"))

(defn sqlite-db-spec
  [path]
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname (str path)})

(def table-name "eva_kv")

(defrecord DbSpecDataSource [db-spec]
  DataSource
  (getConnection [_] (jdbc/get-connection db-spec))
  (getConnection [_ user pass]
    (if (or (string? db-spec) (instance? URI db-spec))
      (jdbc/get-connection {:connection-uri (str db-spec) :user user :password pass})
      (jdbc/get-connection (assoc db-spec :user user :password pass))))
  (getLoginTimeout [_] (DriverManager/getLoginTimeout))
  (setLoginTimeout [_ n] (DriverManager/setLoginTimeout n))
  (getLogWriter [_] (DriverManager/getLogWriter))
  (setLogWriter [_ w] (DriverManager/setLogWriter w))
  (getParentLogger [_] C3P0ImplUtils/PARENT_LOGGER))

(defn pool-config []
  {"maxPoolSize"                  (config/config-strict :eva.v2.storage.sql.connection-pool.max-pool-size)
   "minPoolSize"                  (config/config-strict :eva.v2.storage.sql.connection-pool.min-pool-size)
   "maxConnectionAge"             (config/config-strict :eva.v2.storage.sql.connection-pool.max-connection-age)
   "maxIdleTimeExcessConnections" (config/config-strict :eva.v2.storage.sql.connection-pool.max-idle-time-excess-connections)
   "maxIdleTime"                  (config/config-strict :eva.v2.storage.sql.connection-pool.max-idle-time)
   "idleConnectionTestPeriod"     (config/config-strict :eva.v2.storage.sql.connection-pool.idle-connection-test-period)
   "testConnectionOnCheckin"      (config/config-strict :eva.v2.storage.sql.connection-pool.test-connection-on-checkin)
   "testConnectionOnCheckout"     (config/config-strict :eva.v2.storage.sql.connection-pool.test-connection-on-checkout)})

(defn- create-pool [db-spec]
  (let [ds (->DbSpecDataSource db-spec)]
    (DataSources/pooledDataSource ^DataSource ds ^Map (pool-config))))

(defn- create-db-pool [db-spec] {:datasource (create-pool db-spec)})

(def ^:dynamic *max-retries* (config/config-strict :eva.v2.storage.sql.max-retries))
(defn exponential-retry-strategy [retries]
  (cons 0 (again/max-retries retries
                             (again/randomize-strategy 0.5
                                                       (again/multiplicative-strategy 10 1.25)))))

(defmacro retrying [& body]
  `(again/with-retries (exponential-retry-strategy *max-retries*) ~@body))

(defn select-for-read-mode [read-mode]
  (format "select %s from %s"
          (case read-mode :read-full "namespace, id, attrs, val"
                :read-attrs "namespace, id, attrs")
          table-name))

(defn sql-in-list [for-params]
  (format "(%s)" (str/join ", " (repeat (count for-params) "?"))))

(defn query-read [read-mode namespace ids]
  (apply vector
         (str (select-for-read-mode read-mode)
              " "
              "where namespace = ? and id in"
              " "
              (sql-in-list ids))
         (str namespace)
         (map str ids)))

(def ^:private byte-array-type (Class/forName "[B"))
(defn- ->byte-string [x]
  (condp instance? x
    byte-array-type
    (ByteString/wrapping x)
    Blob
    (with-open [in (.getBinaryStream ^Blob x)]
      (ByteString/readFrom in))))

(defn db-id-exists? [db-conn read-mode namespace id]
  (when-not db-conn (raise-sql-err :non-extant
                                   "db connection not initialized."
                                   {:db-conn db-conn
                                    ::sanex/sanitary? false}))
  (->> [id] (query-read read-mode namespace) (jdbc/query db-conn) seq boolean))

(defn db-read-blocks [db-conn read-mode namespace ids]
  (when-not db-conn (raise-sql-err :non-extant
                                   "db connection not initialized."
                                   {:db-conn db-conn
                                    ::sanex/sanitary? false}))
  (when-some [results (not-empty (jdbc/query db-conn
                                             (query-read read-mode namespace ids)))]
    (doall (for [{:keys [namespace id attrs val] :as row} results
                 :when row
                 :let [attrs (when attrs (edn/read-string attrs))
                       val (some-> val ->byte-string)]]
             (->Block namespace id attrs val)))))

;; The sqlite-jdbc driver doesn't support PreparedConnection#setBinaryStream
;; Actually, it appears it does in the older jdbc3 classes, but for some reason
;; the jdbc4 classes (which extend the jdbc3 classes) override it and throw
;; SQLFeatureNotSupportedException.
(defn ^:private set-as-byte-stream [^PreparedStatement s ^long i ^ByteString bs]
  (.setBinaryStream s i (ByteStringInputStream. bs)))

(defn ^:private set-as-byte-array [^PreparedStatement s ^long i ^ByteString bs]
  (.setBytes s i (.toByteArray bs)))

;; So instead we'll create this default set-bytestring-parameter function which:
;; 1. first attempts to use #setBinaryStream
;; 2. falls back to using #setBytes if #1 fails.
;; This is also marked `:dynamic` so that one of the above functions can be
;; explicitly bound if you happen to know it's needed.
;; To see how this done, refer to `connection-variations` and `with-bytestring-parameter-behavior`.
(defn ^:private ^:dynamic set-bytestring-parameter [^PreparedStatement s ^long i ^ByteString bs]
  (try (set-as-byte-stream s i bs)
       (catch SQLFeatureNotSupportedException e
         (clojure.tools.logging/warn
          e
          (str "PreparedStatement instance does not support method #setByteStream. "
               "Falling back to #setBytes."))
         (set-as-byte-array s i bs))))

(extend-protocol jdbc/ISQLParameter
  ByteString
  (set-parameter [v ^PreparedStatement s ^long i]
    (set-bytestring-parameter s i v)))

(defn update-or-insert!
  "Updates columns or inserts a new row in the specified table"
  [conn table row where-clause]
  (jdbc/with-db-transaction [tx conn]
    (let [result (jdbc/update! tx table row where-clause)]
      (if (zero? (first result))
        (jdbc/insert! tx table row)
        result))))

;; # Upsert Strategy
;;
;; Implementing Upsert-semantics varies by sql-database.
;; The `upsert-strategy` multimethod is used to select
;; a function that implements Upsert-semantics for a particular
;; sql-database.

(defn block->row
  [block]
  {:namespace (str (block/storage-namespace block))
   :id        (str (block/storage-id block))
   :attrs     (pr-str (block/attributes block))
   :val       (block/value block)})

(defn blocks->rows
  "Converts Blocks into maps suitable for use as row entries."
  [blocks]
  (for [b blocks] (block->row b)))

(defn write-mode-rows
  "If write-mode == :write-attrs, strips the :val from rows"
  [write-mode rows]
  (case write-mode
    :write-attrs (map #(dissoc % :val) rows)
    :write-full rows))

(defn juxt-cols [cols]
  {:pre [(seq cols)]}
  (apply juxt (map (fn [c] #(get % c)) cols)))

(defn rows->vals [cols rows] (map (juxt-cols cols) rows))

;; ## Upsert Strategy using `MERGE` statement
(defn merge-stmt [table cols]
  (let [cols-str (str/join ", " (map name cols))
        vals-str (str/join ", " (map (constantly "?") cols))]
    (str "MERGE INTO " (name table) "(" cols-str ")" " VALUES(" vals-str ")")))

(defn merge-into!
  "merges records into specified table"
  [conn table cols rows]
  (let [row->vals (apply juxt (map (fn [c] #(get % c)) cols))]
    (jdbc/execute! conn (apply vector (merge-stmt table cols) (map row->vals rows)) :multi? true)))

(defn merge-blocks! [conn write-mode blocks]
  (let [rows (->> (blocks->rows blocks)
                  (write-mode-rows write-mode))
        cols (case write-mode
               :write-attrs [:namespace :id :attrs]
               :write-full [:namespace :id :attrs :val])
        sql-results (merge-into! conn table-name cols rows)]
    (map #(select-keys % [:namespace :id]) rows)))

;; ## Upsert Strategy using `INSERT ... ON DUPLICATE KEY UPDATE`
;; This syntax is specific to MySQL or MariaDB
(defn insert-or-update-on-duplicate-stmt [table cols update-cols]
  (let [cols-str (str/join ", " (map name cols))             ;; comma separated list of column names
        vals-str (str/join ", " (map (constantly "?") cols)) ;; comma separated list of value place-holders
        ;; comma separated list of `column = VALUES(column)` entries
        ;; here `VALUES(column)` will return the column-value for the failed insertion attempt
        update-str (->> (for [c update-cols ;; only loop over columns that should be updated
                              :let [c (name c)]]
                          (str c " = " "VALUES(" c ")"))
                        (str/join ", "))]
    (str "INSERT INTO " (name table)
         "(" cols-str ") "
         "VALUES(" vals-str ") "
         "ON DUPLICATE KEY UPDATE "
         update-str)))

(defn insert-or-update-on-duplicate-blocks-stmt+vals [write-mode blocks]
  (let [rows (->> (blocks->rows blocks)
                  (write-mode-rows write-mode))
        [cols update-cols] (case write-mode
                             :write-attrs [[:namespace :id :attrs] [:attrs]]
                             :write-full [[:namespace :id :attrs :val] [:attrs :val]])
        sql-stmt (insert-or-update-on-duplicate-stmt table-name cols update-cols)
        sql-vals (rows->vals cols rows)]
    ;; prepare the vector of [<sql-command-string> <value-arguments>*]
    (into [sql-stmt] sql-vals)))

(defn insert-or-update-on-duplicate-blocks! [conn write-mode blocks]
  (let [stmt+vals (insert-or-update-on-duplicate-blocks-stmt+vals write-mode blocks)
        sql-result (jdbc/execute! conn stmt+vals :multi? true)]
    (for [[r b] (map vector sql-result blocks)
          :when r]
      {:namespace (block/storage-namespace b)
       :id        (block/storage-id b)})))

;; ## Upsert Strategy using `INSERT ... ON CONFLICT DO UPDATE SET`
;; This syntax is available in Postgres 9.5+ and sqlite 3.24+
(defn insert-on-conflict-upsert-stmt [table conflict-target cols update-cols]
  (let [cols-str (str/join ", " (map name cols))                ;; comma separated list of insert column names
        vals-str (str/join ", " (map (constantly "?") cols))    ;; comma separated list of insert value place-holders
        update-cols-str (str/join ", " (map name update-cols)) ;; comma separated list of update column names
        ;; values from a failed insert are available in a virtual table called `EXCLUDED`
        excluded-cols-str (str/join ", " (map #(str "EXCLUDED." (name %)) update-cols))
        ;; create the UPDATE SET arguments as: `(<col1>,<col2>,...) = (<val1>,<val2>,...)
        update-set-str (str "(" update-cols-str ")" " = " "(" excluded-cols-str ")")]
    (str "INSERT INTO " (name table)
         "(" cols-str ") "
         "VALUES(" vals-str ") "
         "ON CONFLICT " conflict-target " "
         "DO UPDATE SET " update-set-str)))


(defn insert-on-conflict-upsert-stmt+vals [write-mode blocks]
  (let [rows (->> (blocks->rows blocks)
                  (write-mode-rows write-mode))
        [cols update-cols] (case write-mode
                             :write-attrs [[:namespace :id :attrs] [:attrs]]
                             :write-full [[:namespace :id :attrs :val] [:attrs :val]])
        sql-stmt (insert-on-conflict-upsert-stmt table-name "(namespace, id)" cols update-cols)
        sql-vals (rows->vals cols rows)]
    (into [sql-stmt] sql-vals)))

(defn insert-on-conflict-upsert-blocks! [conn write-mode blocks]
  (let [stmt+vals (insert-on-conflict-upsert-stmt+vals write-mode blocks)
        sql-result (jdbc/execute! conn stmt+vals :multi? true)]
    (for [[r b] (map vector sql-result blocks)
          :when r]
      {:namespace (block/storage-namespace b)
       :id (block/storage-id b)})))

(def connection-variations
  {"org.h2.jdbc.JdbcConnection"             {:key-violation-error-code 23505
                                             :key-violation-sql-state  "23505"
                                             :upsert-strategy          merge-blocks!}
   "org.postgresql.jdbc42.Jdbc42Connection" {:key-violation-error-code 0
                                             :key-violation-sql-state  "23505"
                                             :upsert-strategy          insert-on-conflict-upsert-blocks!}
   "com.mysql.jdbc.JDBC4Connection"         {:key-violation-error-code 1062
                                             :key-violation-sql-state  "23000"
                                             :upsert-strategy          insert-or-update-on-duplicate-blocks!}
   "org.mariadb.jdbc.MariaDbConnection"     {:key-violation-error-code 1062
                                             :key-violation-sql-state  "23000"
                                             :upsert-strategy          insert-or-update-on-duplicate-blocks!}
   "org.sqlite.jdbc4.JDBC4Connection"       {:key-violation-error-code 19
                                             :key-violation-sql-state  nil
                                             :upsert-strategy          insert-on-conflict-upsert-blocks!
                                             :set-bytestring-parameter set-as-byte-array}})


(comment
  ;; here's how to detect the above error-code and sql-state information for a given database
  (defn detect-key-violation-error-info [conn]
    (let [rand-name (fn rand-name [l] (apply str (take l (repeatedly #(char (+ (rand-int 26) 65))))))
          table-name (rand-name 20)
          init-row {:key (rand-name 50), :val (rand-name 100)}
          violation-row (assoc init-row :val (rand-name 100))]
      (assert (= (:key init-row) (:key violation-row)))
      (assert (not= (:val init-row) (:val violation-row)))
      ;; first create the temporary table
      (jdbc/db-do-commands
       conn
       (str "CREATE TABLE " table-name "(key varchar(256) PRIMARY KEY, val varchar(256))"))
      (try
        (try
          ;; then do an initial insert of a row
          (jdbc/insert! conn table-name init-row)
          (catch java.sql.SQLException e
            (throw (ex-info "first insert failed, try this on an empty table"
                            {:conn       conn
                             :table-name table-name
                             :init-row    init-row
                             :violation-row violation-row}
                            e))))
        (try
          (jdbc/insert! conn table-name violation-row)
          ;; if the insert didn't fail, then no conflict occurred,
          ;; so throw an exception if we get this far
          (throw (ex-info "second insert did not fail with with primary key violation"
                          {:conn conn
                           :table-name table-name
                           :init-row init-row
                           :violation-row violation-row}))
          (catch java.sql.SQLException e
            (-> {:key-violation-error-code (.getErrorCode e)
                 :key-violation-sql-state  (.getSQLState e)}
                (vary-meta assoc :exception e))))
        (finally
          (jdbc/db-do-commands
           conn
           (str "DROP TABLE " table-name))))))

  ;; detect with sqlite
  (detect-key-violation-error-info (sqlite-db-spec (java.io.File/createTempFile "test" "sqlite")))
  ;; detect with h2
  (detect-key-violation-error-info (h2-db-spec (java.io.File/createTempFile "test" "h2"))))

(defn upsert-blocks-strategy
  "Returns the driver-specific function stored as :upsert-strategy in driver-variations.
  The function is expected to accept 3 arguments:
  [database-connection, write-mode, and blocks-sequence]"
  [connection-type]
  (get-in connection-variations [connection-type :upsert-strategy]))

(defn key-violation-error
  "Returns the Error Code to expect given the driver being used. Retrieves
  this error code from :key-violation-error-code in driver-variations."
  [connection-type]
  (get-in connection-variations [connection-type :key-violation-error-code]))

(defn key-violation-state
  "Returns the SQL State to expect from a key violation error given the driver
  being used. Retrieves this state code from :key-violation-sql-state in
  driver-variations."
  [connection-type]
  (get-in connection-variations [connection-type :key-violation-sql-state]))

(defmacro with-bytestring-parameter-behavior [connection-type & body]
  `(let [set-bytestring-fn# (get-in connection-variations
                                    [~connection-type :set-bytestring-parameter]
                                    set-bytestring-parameter)]
     (binding [set-bytestring-parameter set-bytestring-fn#]
       ~@body)))

(defn config-subprotocol? [expected config]
  (= expected (-> config ::db-spec :subprotocol)))

(defn init-h2-db [db-conn] (jdbc/db-do-commands db-conn h2-schema-ddl))
(defn init-sqlite-db [db-conn] (jdbc/db-do-commands db-conn sqlite-schema-ddl))
(comment
  (def sqlite-conn (sqlite-db-spec (java.io.File/createTempFile "tmp" ".sqlite")))
  (init-sqlite-db sqlite-conn))

(defn init-local-db [{:as sql-storage
                      :keys [db-spec
                             db-conn
                             config]}]
  {:pre [(= ::types/sql (::types/storage-type config))]}
  (condp config-subprotocol? config
    "h2" (init-h2-db db-conn)
    "sqlite" (init-sqlite-db db-conn)
    nil))

;; # SQLStorage Adapter
(defrecord SQLStorage [db-spec db-conn config]
  qu/SharedResource
  (resource-id [this] (some-> (::resource-id this) deref))
  (initiate [this]
    (if (qu/initiated? this)
      this
      (let [resource-id (qu/new-resource-id)
            ;; Create database-connection-pool
            db-conn (or db-conn (create-db-pool db-spec))
            ;; Temporarily create a connection based on the raw db-spec
            ;; and capture its type. This is used later to select the correct
            ;; behavior for various SQL dbs.
            connection-type (with-open [conn (jdbc/get-connection db-spec)]
                              (.getName (class conn)))
            initiated-self (assoc this
                                  ::resource-id (atom resource-id)
                                  :db-conn db-conn
                                  :connection-type connection-type)]
        (init-local-db initiated-self)
        initiated-self)))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (terminate [this]
    (if-not (qu/initiated? this)
      this
      (do (reset! (::resource-id this) nil)
          (cond-> this
            (some? db-conn) (as-> this
                                  (let [db-conn (:db-conn this)]
                                    (DataSources/destroy (:datasource db-conn))
                                    (assoc this :db-conn nil)))))))
  (force-terminate [this] (qu/terminate this))
  BlockStorage
  (storage-read-blocks [this read-mode namespace ids]
    (qu/ensure-initiated! this "cannot read blocks.")
    (when-not (some? db-conn) (raise-sql-err :non-extant
                                             "connection not available"
                                             {::sanex/sanitary? true}))
    (try
      (retrying
       (jdbc/with-db-connection [conn db-conn]
         (db-read-blocks conn read-mode namespace ids)))
      (catch Throwable e
        (log/error (sanitize e))
        (raise-sql-err :unknown
                       "storage-read-blocks"
                       {::sanex/sanitary? false}
                       e))))
  (storage-write-blocks [{:as this :keys [connection-type]} write-mode blocks]
    (qu/ensure-initiated! this "cannot write blocks.")
    (with-bytestring-parameter-behavior connection-type
      (let [upsert-blocks! (upsert-blocks-strategy connection-type)]
        (when-not (some? db-conn) (raise-sql-err :non-extant
                                                 "connection not available"
                                                 {::sanex/sanitary? true}))
        (when-not (some? upsert-blocks!) (raise-sql-err :non-extant
                                                        "upsert-strategy not found"
                                                        {::sanex/sanitary? true}))
        (try
          (retrying (upsert-blocks! db-conn write-mode blocks))
          (catch Throwable e
            (log/error (sanitize e))
            (raise-sql-err :unknown
                           "storage-write-blocks"
                           {::sanex/sanitary? false}
                           e))))))
  (storage-delete-blocks [this namespace ids]
    (qu/ensure-initiated! this "cannot delete blocks.")
    (when-not (some? db-conn) (raise-sql-err :non-extant
                                             "connection not available"
                                             {::sanex/sanitary? true}))
    (try
      (retrying
       (jdbc/with-db-connection [conn db-conn]
         (jdbc/execute! conn (apply vector (str "DELETE FROM " table-name " WHERE namespace = ? AND id in " (sql-in-list ids)) namespace ids))
         (for [id ids] {:namespace namespace :id id})))
      (catch Throwable e
        (log/error (sanitize e))
        (raise-sql-err :unknown
                       "storage-delete-blocks"
                       {::sanex/sanitary? false}
                       e))))
  (storage-compare-and-set-block [{:as this :keys [connection-type]} expected replacement]
    (qu/ensure-initiated! this "cannot cas.")
    (with-bytestring-parameter-behavior connection-type
      (let [upsert-blocks! (upsert-blocks-strategy connection-type)]
        (when-not (some? db-conn) (raise-sql-err
                                   :non-extant
                                   "connection not available"
                                   {::sanex/sanitary? true}))
        (when-not (some? upsert-blocks!) (raise-sql-err :non-extant "upsert-strategy not found" {::sanex/sanitary? true}))
        (when-not (= (:namespace expected) (:namespace replacement))
          (raise-sql-err :cas-failure
                         "namespaces don't match."
                         {:expected         (:namespace expected)
                          :replacement      (:namespace replacement)
                          ::sanex/sanitary? true}))
        (when-not (= (:id expected) (:id replacement))
          (raise-sql-err :cas-failure
                         "ids don't match."
                         {:expected         (:id expected)
                          :replacement      (:id replacement)
                          ::sanex/sanitary? true}))
        (try
          (retrying
           (jdbc/with-db-transaction [tx db-conn :isolation :serializable]
             (let [replacement-row (block->row replacement)
                   expected-row (block->row expected)
                   [rows-updated] (jdbc/update! tx table-name replacement-row
                                                ["namespace = ? AND id = ? AND attrs = ? AND val = ?"
                                                 (:namespace expected-row)
                                                 (:id expected-row)
                                                 (:attrs expected-row)
                                                 (:val expected-row)])]
               (cond (zero? rows-updated) false
                     (= 1 rows-updated) true
                     :else (raise-sql-err :unexpected-cas-update-result
                                          (format "cas update should affect 0 or 1 rows, but affected %s rows"
                                                  rows-updated)
                                          {:rows-updated     rows-updated
                                           :namespace        (:namespace replacement)
                                           :id               (:id replacement)
                                           ::sanex/sanitary? true})))))
          (catch Throwable e
            (log/error (sanitize e))
            (raise-sql-err :unknown
                           "storage-compare-and-set-block"
                           {::sanex/sanitary? false}
                           e))))))
  (storage-create-block [{:as this :keys [connection-type]} block]
    (qu/ensure-initiated! this "cannot create block.")
    (when-not (some? db-conn)
      (raise-sql-err
       :non-extant
       "connection not available"
       {::sanex/sanitary? true}))
    (with-bytestring-parameter-behavior connection-type
      (try*
       (retrying
        (let [result (jdbc/insert! db-conn table-name (block->row block) :transaction? true)]
          true))
       (catch :and [Exception (:not :storage.sql/*)] e
              (if (and (instance? java.sql.SQLException e)
                       (= (key-violation-error connection-type) (.getErrorCode ^java.sql.SQLException e))
                       (= (key-violation-state connection-type) (.getSQLState ^java.sql.SQLException e)))
                false
                (raise-sql-err :unknown "storage-create-block" {::sanex/sanitary? false} e)))
       (catch Throwable e
         (raise-sql-err :unknown "storage-create-block" {::sanex/sanitary? false} e))))))

(def cntr (atom 1))
(defn temp-file
  []
  (File/createTempFile (str "sql-test-db-" (swap! cntr inc)) "tmpdb"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Construct a SQL-Store ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn build-sql-store
  [{:as config :keys [::db-spec]}]
  (let [sql-store (if db-spec
                    (map->SQLStorage {:db-spec (into {} db-spec)
                                      :config config})
                    (throw (IllegalArgumentException. ":sql storage-config must have :db-spec field")))]

    sql-store))

(defn sql-store-ident
  [{:keys [::db-spec]}]
  (if db-spec
    [::types/sql db-spec]
    (throw (IllegalArgumentException. ":sql storage-config must have :db-spec field"))))
