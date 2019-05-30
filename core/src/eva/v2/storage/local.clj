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

(ns eva.v2.storage.local
  (:require [eva.v2.storage.core :refer [BlockStorage ->Block]]
            [eva.v2.storage.block-store.impl.sql :refer [map->SQLStorage]]
            [eva.v2.storage.error :refer [raise-request-cardinality]]
            [recide.sanex :as sanex]
            [eva.config :refer [config-strict]]
            [eva.error :refer [insist]]
            [clojure.java.jdbc :as jdbc]
            [com.stuartsierra.component :as component :refer [start]])
  (:import [java.io File]))

(def schema-ddl
  (str "CREATE TABLE IF NOT EXISTS "
       "eva_kv( "
       "namespace varchar(128), "
       "id varchar(128), "
       "attrs varchar(600), "
       "val blob, "
       "primary key (namespace, id)"
       ")"))

(defn db-spec
  [path]
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname     (str path) ;;  ";MVCC=TRUE"
   :user        "sa"})

(def cntr (atom 1))
(defn temp-file
  []
  (File/createTempFile (str "sql-test-db-" (swap! cntr inc)) "tmpdb"))

(defn init-h2-db [db-conn] (jdbc/db-do-commands db-conn schema-ddl))


(def ^:private block->key (juxt eva.v2.storage.core/storage-namespace eva.v2.storage.core/storage-id))

(defrecord MemStorage [atom-map max-request-cardinality]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  BlockStorage
  (storage-read-blocks [_ read-mode namespace ids]
    (when (> (count ids) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count ids) max-request-cardinality)
                                 {:count (count ids),
                                  :cardinality max-request-cardinality,
                                  ::sanex/sanitary? true}))
    (seq (sequence (comp (map (juxt (constantly namespace) identity))
                         (map @atom-map)
                         (filter some?))
                   ids)))
  (storage-write-blocks [_ write-mode blocks]
    (when (> (count blocks) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count blocks) max-request-cardinality)
                                 {:count (count blocks),
                                  :cardinality max-request-cardinality,
                                  ::sanex/sanitary? true}))
    (swap! atom-map
           into
           (zipmap (map block->key blocks)
                   blocks))
    (map #(select-keys % [:namespace :id]) blocks))
  (storage-delete-blocks [_ namespace ids]
    (when (> (count ids) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count ids) max-request-cardinality)
                                 {:count (count ids),
                                  :cardinality max-request-cardinality,
                                  ::sanex/sanitary? true}))
    (swap! atom-map
           #(transduce (comp (map (juxt (constantly namespace) identity))
                             (map %))
                       dissoc
                       %
                       ids)))
  (storage-compare-and-set-block [_ expected replacement]
    (let [k-old (block->key expected)
          k-new (block->key replacement)]
      (insist (= k-old k-new))
      (and (= expected (get @atom-map k-old))
           (= replacement
              (get (swap! atom-map
                          #(if (= expected (get % k-old))
                             (assoc % k-old replacement)
                             %))
                   k-new)))))
  (storage-create-block [_ block]
    (let [k (block->key block)
          after (swap! atom-map
                     #(if (nil? (find % k))
                        (assoc % k block)
                        %))]
      (= block (get after k)))))

(defn mem-storage
  ([] (->MemStorage (atom {}) (config-strict :eva.v2.storage.max-request-cardinality)))
  ([max-request-cardinality] (->MemStorage (atom {}) max-request-cardinality)))
