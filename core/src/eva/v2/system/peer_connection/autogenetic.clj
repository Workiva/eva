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

(ns eva.v2.system.peer-connection.autogenetic
  "Local connection for an EVA Database. Supports in-memory, h2 and sqlite."
  (:require [eva.core :as core]
            [eva.v2.system.peer-connection.core :as peer]
            [eva.v2.system.transactor.core :as transactor]
            [eva.v2.system.indexing.core :as indexing]
            [eva.v2.system.database-catalogue.core :as catalog]
            [eva.v2.database.core :as database]
            [eva.v2.storage.system :as storage]
            [eva.v2.storage.block-store :as blocks]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.messaging.address :as address]
            [quartermaster.core :as qu]
            [eva.v2.utils.spec :refer [conform-spec]]
            [utiliva.uuid :refer [squuid]]
            [eva.error :refer [insist with-api-error-handling]]
            [eva.print-ext]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as c])
  (:import [eva Connection]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::autogenetic true?)
(s/def ::local true?)

(s/def ::minimal-config (s/keys :req-un [(or ::local
                                             ::autogenetic)]
                                :opt [::database/id]))

(s/def ::storage-type #{::store-type/memory :memory
                        ::store-type/sql :h2
                        :sqlite})
(s/def ::storage-type-specified (s/keys :req-un [::storage-type]))

(s/def ::config
  (s/or :storage-fully-specified (s/merge ::storage/config
                                          ::minimal-config)
        :storage-type-specified (s/merge ::minimal-config
                                         ::storage-type-specified)
        :minimal ::minimal-config))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(def version 2)

(def ^:dynamic *default-database-id* (random-uuid))
(def ^:dynamic *default-partition-id* (random-uuid))

(defn memory-storage-config
  [config]
  {::store-type/storage-type ::store-type/memory
   ::vs-manager/disable-caching? true
   ::memory/store-id (or (::database/id config) *default-database-id*)})

(defn h2-storage-config
  [config]
  {::store-type/storage-type ::store-type/sql
   ::sql/db-spec (sql/h2-db-spec
                  (java.io.File/createTempFile
                   "eva.v2.system.peer-connection.autogenetic."
                   ".h2"))})

(defn sqlite-storage-config
  [config]
  {::store-type/storage-type ::store-type/sql
   ::sql/db-spec (sql/sqlite-db-spec
                  (java.io.File/createTempFile
                   "eva.v2.system.peer-connection.autogenetic."
                   ".sqlite"))})

(defn create-storage-config [config]
  (case (:storage-type config)
    (::store-type/sql :h2) (h2-storage-config config)
    (:sqlite) (sqlite-storage-config config)
    (::store-type/memory :memory) (memory-storage-config config)
    nil (memory-storage-config config) ;; <-- DEFAULT
    (throw (Exception.)))) ;; TODO:

(defn ensure-storage-configuration
  [config]
  ;; If the storage is fully specified, leave as is:
  (if (s/valid? ::storage/config config)
    config
    (merge config (create-storage-config config))))

(defn generate-messaging-configuration
  [config]
  (let [uuid (random-uuid)]
    (merge config
           {:messenger-node-config/type :local-messenger-node
            ::address/transaction-submission (str uuid "-submit-addr")
            ::address/transaction-publication (str uuid "-pub-addr")
            ::address/index-updates (str uuid "-index-addr")})))

(defn generate-peer-configuration [config]
  (merge config {::peer/id (random-uuid)}))

(defn generate-transactor-configuration [config]
  (merge config {::transactor/id (random-uuid)}))

(defn generate-indexing-configuration [config]
  (merge config {::indexing/id (random-uuid)}))

(defrecord AutogeneticConnection [resource-id
                                  config
                                  value-store
                                  transactor
                                  indexer
                                  connection
                                  release-fn]
  qu/SharedResource
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [resource-id (qu/new-resource-id)
                     db-id (::database/id config)
                     value-store (qu/acquire vs-manager/value-store-manager resource-id config)
                     _ (catalog/initialize-database value-store db-id)
                     transactor (qu/acquire transactor/transactor-manager resource-id config)
                     indexer (qu/acquire indexing/indexer-manager resource-id config)
                     connection (qu/acquire peer/peer-connection-manager resource-id config)
                     _ (qu/release value-store true)]
                    (assoc this
                           :resource-id (atom resource-id)
                           :value-store value-store
                           :transactor transactor
                           :indexer indexer
                           :connection connection))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id @resource-id]
        (reset! resource-id nil)
        (qu/release connection true)
        (qu/release value-store true)
        (qu/release indexer true)
        (qu/release transactor true)
        (assoc this
               :value-store nil
               :transactor nil
               :indexer nil
               :connection nil))))
  (force-terminate [this]
    (when-not (qu/terminated? this)
      (qu/force-terminate connection)
      (qu/force-terminate indexer)
      (qu/force-terminate transactor)
      (qu/force-terminate value-store))
    (qu/terminate this))
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  eva.Connection
  (db [_]
    (with-api-error-handling (.db ^Connection @connection)))
  (dbSnapshot [_]
    (with-api-error-handling (.dbSnapshot ^Connection @connection)))
  (syncDb [this]
    (with-api-error-handling (.syncDb ^Connection @connection)))
  (log [_]
    (with-api-error-handling (.log ^Connection @connection)))
  (latestT [_]
    (with-api-error-handling (.latestT ^Connection @connection)))
  (release [this]
    (with-api-error-handling
      (release-fn this true)
      (qu/terminate this)))
  (transact [_ tx-data]
    (with-api-error-handling (.transact ^Connection @connection tx-data)))
  (transactAsync [_ tx-data]
    (with-api-error-handling
      (.transactAsync ^Connection @connection tx-data))))

(defmethod print-method AutogeneticConnection [ac ^java.io.Writer w]
  (.write w (str "#AutogeneticConnection{:version ", version ", :status " (qu/status ac) "}")))

(qu/defmanager autogenetic-connection-manager
  :discriminator
  (fn [user-id config] [(::database/id config) (::values/partition-id config)])
  :constructor
  (fn [user-id config]
    (map->AutogeneticConnection
     {:config config
      :release-fn (qu/auto-releaser user-id config)})))

(defn expand-config [config]
  (-> config
      (update ::database/id (fnil identity *default-database-id*))
      (update ::values/partition-id (fnil identity *default-database-id*))
      (ensure-storage-configuration)
      (generate-messaging-configuration)
      (generate-peer-configuration)
      (generate-transactor-configuration)
      (generate-indexing-configuration)))

(defn connect
  [config]
  {:pre [(conform-spec ::config config)]}
  (let [full-config (expand-config config)]
    @(qu/acquire autogenetic-connection-manager ::local full-config)))
