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

(ns eva.v2.system.database-catalogue.core
  (:require [eva.v2.datastructures.vector :refer [create-persisted-vector]]
            [eva.core :as core]
            [eva.datastructures.protocols :as p]
            [eva.error :as err]
            [recide.sanex :as sanex]
            [eva.datom :as dat]
            [eva.v2.database.log :as log]
            [eva.v2.database.index :as idx]
            [eva.comparators :as cmp]
            [utiliva.uuid :refer [squuid]]
            [eva.v2.storage.value-store :as vs]
            [eva.utils :refer [init-log-info]]
            [recide.core :refer [try*]]))

(defrecord DatabaseInfo [database-id tx-log-id version])

(defn init-log->init-datoms [init-log]
  (into (sorted-set-by (cmp/full-proj-cmp :eavt))
        (map (partial dat/unpack-datom (:cur-tx-eid init-log)))
        (:novelty init-log)))

(defn filter-vaet [init-datoms]
  ;; WARNING:
  ;; NOTE: If the initial db ever gets long-valued datoms, this will no longer
  ;;       be correct, and something with more knowledge will be needed!
  (filter #(instance? Long (:v %)) init-datoms))

(defn create-initial-database-constructs!
  "Build the full set of initial database constructs. Returns the database info
   that has been persisted under a newly generated database-id."
  [value-store database-id]
  (let [tx-num -1
        empty-log (create-persisted-vector value-store)
        init-log-info (init-log-info)
        init-datoms (init-log->init-datoms init-log-info)
        vaet-datoms (filter-vaet init-datoms)
        vaet-attrs (into #{} (map :a) vaet-datoms)
        covering-indexes (map idx/create-index (repeat value-store)
                              [:aevt :aevth
                               :avet :aveth
                               :eavt :eavth]
                              (repeat init-datoms))
        vaet-indexes (map idx/create-index (repeat value-store)
                          [:vaet :vaeth]
                          (repeat vaet-datoms))
        indexes (concat covering-indexes vaet-indexes)
        index-roots (into {} (map (juxt :name core/->root) indexes))
        init-log-entry (log/strict-map->TransactionLogEntry
                        (-> init-log-info
                            (assoc :index-roots index-roots
                                   :ref-type-attrs vaet-attrs
                                   :version log/LOG_ENTRY_VERSION)))
        init-log (conj empty-log init-log-entry)
        init-info (map->DatabaseInfo {:database-id database-id
                                      :tx-log-id (p/storage-id init-log)
                                      :version 1})]
    init-info))

(defn initialize-database*
  "1. Do a read to see if there's any extant state, if so, abort early.
   2. Initialize the database constructs.
   3. Attempt to cas the entry key."
  [value-store database-id]
  (when @(vs/get-value @value-store (str database-id))
    (err/raise :database-catalogue/database-already-exists
               (format "Cannot create database %s. Database already exists" database-id)
               {:database-id database-id
                ::sanex/sanitary? true}))
  (let [database-info (create-initial-database-constructs! value-store database-id)]
    (if @(vs/create-key @value-store (str database-id) database-info)
      database-info
      (err/raise :database-catalogue/concurrent-database-creation
                 (format "Aborting creation of database %s. Concurrent database creation probable" database-id)
                 {:database-id database-id
                  :database-info database-info}))))

(defn initialize-database
  "Create initial database state if it does not exist and return DatabaseInfo
   If it does, safely abort and return the current DatabaseInfo"
  ([value-store] (initialize-database value-store (str (squuid))))
  ([value-store database-id]
   (assert (uuid? database-id))
   (let [str-id (str database-id)]
     (try* (initialize-database* value-store database-id)
           (catch :database-catalogue/database-already-exists e
             @(vs/get-value @value-store str-id))))))

(defn database-info [value-store database-id]
  (if-let [info @(vs/get-value @value-store (str database-id))]
    info
    (err/raise :database-catalogue/no-such-database-info
               (format "could not find database info for key %s" database-id)
               {:database-id database-id
                ::sanex/sanitary? true})))
