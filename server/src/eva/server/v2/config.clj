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

(ns eva.server.v2.config
  "This namespace provides the evaluation environment in which server configuration files are
  loaded and evaluated."
  (:require [clojure.spec.alpha :as s]
            [eva.catalog.client.alpha.client :as catalog-client]
            [clojure.string]))

(defn env
  ([name] (System/getenv name))
  ([name default] (or (System/getenv name) default)))

(defn required-env [name] (or (System/getenv name)
                              (throw (IllegalStateException. (str "ENV variable not set: " name)))))

(defn env-parse
  ([name parse] (env-parse name parse nil))
  ([name parse default]
   (if-some [env-val (System/getenv name)]
     (parse env-val)
     default)))

(defn parse-comma-sep-list
  "Splits a list of strings on '/' or ','.
  This is a change forced upon us due to a Rancher-CLI bug - https://github.com/rancher/rancher/issues/14551"
  [s]
  (->> (if (clojure.string/includes? s ",")
         (clojure.string/split s #",")
         (clojure.string/split s #"/"))
       (map clojure.string/trim)))

(defn parse-bool
  [s]
  (contains? #{"true" "1"}
             (-> s (clojure.string/lower-case) (clojure.string/trim))))

(defn broker-uri [uri] {:messenger-node-config/type :broker-uri, :broker-uri uri})

(defn activemq-broker [uri user password]
  {:messenger-node-config/type :broker-uri,
   :broker-uri uri
   :broker-type "org.apache.activemq.ActiveMQConnectionFactory"
   :broker-user user
   :broker-password password})

(defn sql-storage [db-spec]
  {:eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql
   :eva.v2.storage.block-store.impl.sql/db-spec db-spec})

(s/def ::database-conf (s/keys :req [:eva.v2.storage.value-store.core/partition-id
                                     :eva.v2.database.core/id]))

(defn database [partition-id database-id]
  {:eva.v2.storage.value-store.core/partition-id partition-id
   :eva.v2.database.core/id                      database-id})

(defn address [type database-conf]
  {:pre [(contains? database-conf :eva.v2.storage.value-store.core/partition-id)
         (contains? database-conf :eva.v2.database.core/id)]}
  (format "eva.v2.%s.%s.%s"
          (name type)
          (:eva.v2.storage.value-store.core/partition-id database-conf)
          (:eva.v2.database.core/id database-conf)))

(defn ensure-database
  ([storage-conf database-conf]
   (s/assert* ::database-conf database-conf)
   (merge {::type ::ensure-database} storage-conf database-conf)))

(defn transaction-processor [broker-conf
                             storage-conf
                             {:as database-conf :keys [:eva.v2.storage.value-store.core/partition-id
                                                       :eva.v2.database.core/id]}]
  (s/assert* ::database-conf database-conf)
  (let [tc (merge broker-conf
                  storage-conf
                  database-conf
                  {::type ::transaction-processor
                   :eva.v2.system.transactor.core/id                 (java.util.UUID/randomUUID)
                   :eva.v2.system.indexing.core/id                   (java.util.UUID/randomUUID)
                   :eva.v2.messaging.address/transaction-submission  (address "transact" database-conf)
                   :eva.v2.messaging.address/transaction-publication (address "transacted" database-conf)
                   :eva.v2.messaging.address/index-updates           (address "index-updates" database-conf)})]
    (s/assert* :eva.v2.system.transactor.core/config tc)
    tc))


;; # Catalog Based Configuration
(defn fetch-transactor-configs-for-groups
  "Fetches all configs for the specified transactor-groups
  from the catalog server."
  [catalog-address group-ids]
  {:pre [(every? string? group-ids)]}
  (into #{}
        (mapcat
          (fn [group-id]
            (catalog-client/request-flat-configs-in-transactor-group
              catalog-address
              group-id)))
        group-ids))

(defn fetch-transactor-configs-for-unassigned
  "Fetches all configs NOT IN any transactor-group
  from the catalog server."
  [catalog-address]
  (catalog-client/request-flat-configs-without-transactor-group catalog-address))

(defn override-sql-storage-key
  "Given a map containing a sql-storage configuration, will replace the
  specified key-value entry in the sql-storage's db-spec map.
  of the sql-storage"
  [config k v]
  (assert (contains? config :eva.v2.storage.block-store.impl.sql/db-spec)
          "config map does not contain a sql-storage config")
  (assert (map? (:eva.v2.storage.block-store.impl.sql/db-spec config))
          "config map contains a sql-storage db-spec that is not a map")
  (assoc-in config
            [:eva.v2.storage.block-store.impl.sql/db-spec k]
            v))

(defn apply-updates
  "Given a map m, applies every update-vector in updates to m,
  where each update-vector has the form [f & args]
  and each update-vector will be applied as (apply f m args)."
  [m updates]
  {:pre [(map? m)
         (every? sequential? updates)
         (every? #(fn? (first %)) updates)]}
  (reduce
    (fn
      [m [f & args]]
      (apply f m args))
    m
    updates))

(defn catalog-config->ensure-database+transaction-processor
  [config]
  ;; Storage and broker configs are already
  ;; merged in when they come from the catalog,
  ;; so we just repeat them.
  [(ensure-database config config)
   (transaction-processor config config config)])

(defn distinct-without
  ([removed-keys]
   (comp
     (map #(apply dissoc % removed-keys))
     (distinct)))
  ([removed-keys coll]
    (sequence (distinct-without removed-keys) coll)))

(defn enforcing-unique-key
  ([k on-duplicate]
    (let [seen-val (volatile! #{})]
      (map
        (fn check-k [m]
          (if-some [v (and (contains? m k) (get m k))]
            (if (@seen-val v)
              (on-duplicate k v)
              (do
                (vswap! seen-val conj v)
                m))
            m)))))
  )

(defn configure-from-catalog
  "Given the address of the catalog server, fetches
  all requested configurations and applies the specified updates.

  The options map has keys:

  :transactor-groups - a list of strings identifying the transactor-groups
                       for which configs are being requested

  :default-transactor? - if true, then all configs NOT IN ANY transactor-group
                         will also be requested

  :updates - sequence of update-vectors that will be applied to all requested
             configs. Each update vector has the for [f & args] and will be
             applied to each config as (apply f m args).
             See: apply-updates
  "
  [catalog-address
   {:keys [transactor-groups
           default-transactor?
           updates]
    :or {transactor-groups []
         default-transactor? false
         updates []}}]
  {:pre [(every? sequential? updates)
         (every? #(fn? (first %)) updates)]}
  (let [group-configs (fetch-transactor-configs-for-groups
                        catalog-address
                        transactor-groups)
        other-configs (when default-transactor? (fetch-transactor-configs-for-unassigned
                                                  catalog-address))]
    (into []
          (comp cat
                ;; Strip category, label, and tenant to ensure unique database-configs
                (distinct-without
                  [:eva.catalog.common.alpha.config/category
                   :eva.catalog.common.alpha.config/label
                   :eva.catalog.common.alpha.config/tenant])
                ;; Error if multiple configs with same database-id
                (enforcing-unique-key
                  :eva.v2.database.core/id
                  (fn on-duplicate
                    [k v]
                    (throw
                      (IllegalStateException.
                        (str "found multiple, different configs for the same database: "
                             (pr-str [k v]))))))
                (map #(apply-updates % updates))
                (mapcat catalog-config->ensure-database+transaction-processor))
          [group-configs
           other-configs]))
  )

(comment
  ;; testing configure-from-catalog with mock catalog calls
  (let [broker-config (broker-uri (env "EVA_BROKER_URI" "tcp://localhost:61617"))
        storage-config (sql-storage {:dbtype   (env "EVA_STORAGE_SQL_DBTYPE" "mariadb")
                                     :dbname   (env "EVA_STORAGE_SQL_DBNAME" "eva")
                                     :host     (env "EVA_STORAGE_SQL_HOST" "localhost")
                                     :port     (env "EVA_STORAGE_SQL_HOST" 3306)
                                     :user     (env "EVA_STORAGE_SQL_USER" "eva")
                                     :password (env "EVA_STORAGE_SQL_PASSWORD" "notasecret")})
        storage-partition-id #uuid"fe08283a-4a94-4ae8-ac9b-d1530782276c"
        db1-config (merge broker-config
                          storage-config
                          (database storage-partition-id #uuid"fbd9fc2a-8bed-49b9-832e-dfc268ebc0c4"))
        db2-config (merge broker-config
                          storage-config
                          (database storage-partition-id #uuid"4584e4e4-3920-46d7-b0ae-6c8055bc1913"))
        db3-config (merge broker-config
                          storage-config
                          (database storage-partition-id #uuid"5a206fad-3f54-4463-b7fb-dca7ab43fe3d"))]
    (with-redefs [fetch-transactor-configs-for-groups (constantly #{db1-config db2-config})
                  fetch-transactor-configs-for-unassigned (constantly #{db3-config})]
      ;; Fetch only group configs
      (configure-from-catalog
        "fake-address"
        {:transactor-groups ["fake-group"]
         :default-transactor? false
         :updates [[override-sql-storage-key :user "Changed the sql user!"]
                   [override-sql-storage-key :password "Changed the sql password!"]]})
      ))
  )

(comment
  ;; Example of fetching from catalog and overriding sql user and password
  (configure-from-catalog
    ;; Address of Catalog
    (env "EVA_CATALOG_ADDRESS" "http://localhost:8080")

    {
     ;; transactor-groups to manage
     :transactor-groups   (env-parse "EVA_TRANSACTOR_GROUPS"
                                     parse-comma-sep-list)


     ;; also manage any database not in a transactor-group?
     :default-transactor? (env-parse "EVA_DEFAULT_TRANSACTOR"
                                     parse-bool
                                     false)

     ;; Apply these updates to alter *every* config for every database configuration
     ;; retrieved from the catalog
     :updates             [[override-sql-storage-key :user (env "EVA_STORAGE_SQL_USER" "eva")]
                           [override-sql-storage-key :password (env "EVA_STORAGE_SQL_PASSWORD" "notasecret")]]
     })

  )

(comment

  ;; Example content that could be placed in a config file for a fully manual config
  (let [broker-config  {:eva.v2.messaging/config     :eva.v2.messaging.broker/uri
                        :eva.v2.messaging.broker/uri "tcp://localhost:61617"}

        storage-config {:eva.v2.storage/service            :eva.v2.storage.service/sql
                        :eva.v2.storage.service.sql/dbspec {:dbtype   "mariadb"
                                                            :dbname   "eva"
                                                            :host     "localhost"
                                                            :port     3306
                                                            :user     (required-env "EVA_STORAGE_SQL_USER")
                                                            :password (required-env "EVA_STORAGE_SQL_PASSWORD")}}]
    [(merge broker-config storage-config
            {:eva.v2.storage/partition-id                      "partition-1"
             :eva.v2.database/id                               "database-1"
             :eva.v2.messaging.address/transaction-submission  "eva.v2.transact.partition-1.database-1"
             :eva.v2.messaging.address/transaction-publication "eva.v2.transacted.partition-1.database-1"
             :eva.v2.messaging.address/index-updates           "eva.v2.index-updates.partition-1.database-1"
             :eva.v2.system.transactor/id                      (java.util.UUID/randomUUID)
             :eva.v2.system.indexing/id                        (java.util.UUID/randomUUID)
             })

     (merge broker-config storage-config
            {:eva.v2.storage/partition-id                      "partition-1"
             :eva.v2.database/id                               "database-2"
             :eva.v2.messaging.address/transaction-submission  "eva.v2.transact.partition-1.database-2"
             :eva.v2.messaging.address/transaction-publication "eva.v2.transacted.partition-1.database-2"
             :eva.v2.messaging.address/index-updates           "eva.v2.index-updates.partition-1.database-2"
             :eva.v2.system.transactor/id                      (java.util.UUID/randomUUID)
             :eva.v2.system.indexing/id                        (java.util.UUID/randomUUID)
             })
     ])

  )
