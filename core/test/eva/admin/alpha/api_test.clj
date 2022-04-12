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

(ns eva.admin.alpha.api-test
  (:require [clojure.test :refer :all]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.v2.storage.value-store.protocols :as p]
            [eva.v2.system.peer-connection.autogenetic :as auto]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.database.core :as database]
            [eva.api :as api]
            [eva.admin.alpha.api :refer :all]
            [eva.admin.alpha.traversal :as t])
  (:import (java.io File)))

(defn example-traversal []
  (qp/testing-for-resource-leaks
   (let [config (auto/expand-config {:local true})
         conn (api/connect config)
         value-store (qu/acquire vsc/concurrent-value-store-manager ::traversal-unit-test config)]
     (try
       (dotimes [i 500] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str i)]]))
       (let [trav (t/traversal value-store (::database/id config))]
         (clojure.pprint/pprint (map t/k trav)))
       (finally (api/release conn)
                (qu/release value-store true))))))

(defn equivalent-connection-test [conn1 conn2]
  (is (= (api/latest-t conn1) (api/latest-t conn2)))
  (is (not= conn1 conn2))
  (doseq [index [:aevt :avet :eavt :vaet]]
    ;; hash to keep the repl from exploding on failure.
    (is (= (hash (api/datoms (api/db conn1) index))
           (hash (api/datoms (api/db conn2) index)))))
  (let [db1 (-> conn1 api/db api/history)
        db2 (-> conn2 api/db api/history)]
    (doseq [index [:aevt :avet :eavt :vaet]]
      ;; hash to keep the repl from exploding on failure.
      (is (= (hash (api/datoms db1 index))
             (hash (api/datoms db2 index)))))))

(deftest unit:migrate
  (qp/testing-for-resource-leaks
   (let [source-config (auto/expand-config {:local true})
         database-id (:eva.v2.database.core/id source-config)
         ^File tmpfile (sql/temp-file)
         path (.getPath tmpfile)
         dest-config {:local true
                      :eva.v2.storage.value-store.core/partition-id
                      (random-uuid)
                      :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
                      :eva.v2.storage.block-store.impl.sql/db-spec
                      {:classname "org.h2.Driver"
                       :subprotocol "h2"
                       :subname path
                       :user "SA"}}
         conn (api/connect source-config)
         dest-conn-config (assoc dest-config :eva.v2.database.core/id database-id)]
     (try
       (stateful-migration! source-config dest-config database-id)

       (let [dest-conn (api/connect dest-conn-config)]
         (try
           (testing "migration on empty db"
             (equivalent-connection-test conn dest-conn))

           ;; advance source state
           (dotimes [n 160] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))

           (testing "continuous migration"
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn))

           (testing "trivial continuous migration"
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn))

           (testing "really trivial continuous migration"
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn))

           ;; advance source state again, for good measure.
           (dotimes [n 160] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))

           (testing "continuous migration"
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn))

           (testing "trivial continuous migration"
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn))

           (finally (api/release dest-conn))))
       (finally (api/release conn))))))


;; case: the destination does *not* have a valid database-info and tx-log written
;;       (there has never been a completed migration on this db.)
(deftest unit:interrupted-migration-1
  (qp/testing-for-resource-leaks
   (let [source-config (auto/expand-config {:local true})
         database-id (:eva.v2.database.core/id source-config)
         ^File tmpfile (sql/temp-file)
         path (.getPath tmpfile)
         dest-config {:local true
                      :eva.v2.storage.value-store.core/partition-id
                      (random-uuid)
                      :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
                      :eva.v2.storage.block-store.impl.sql/db-spec
                      {:classname "org.h2.Driver"
                       :subprotocol "h2"
                       :subname path
                       :user "SA"}}
         conn (api/connect source-config)
         dest-conn-config (assoc dest-config :eva.v2.database.core/id database-id)]
     (try

       (dotimes [n 320] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))

       (testing "start, but interrupt migration"
         (let [migration-thread (Thread. #_#(stateful-migration! source-config dest-config database-id)
                                         #(try (stateful-migration! source-config dest-config database-id)
                                                 (catch Throwable t nil)))]
           (.start migration-thread)
           (Thread/sleep 1000)
           (.interrupt migration-thread)))

       (testing "finish in-progress migration"
         (stateful-migration! source-config dest-config database-id))

       (let [dest-conn (api/connect dest-conn-config)]
         (try
           (equivalent-connection-test conn dest-conn)
           (finally (api/release dest-conn))))
       (finally (api/release conn))))))

;; case: the destination has a valid database-info and tx-log written
;;       (there was previously a complete migration on this db.)
(deftest unit:interrupted-migration-2
  (qp/testing-for-resource-leaks
   (let [source-config (auto/expand-config {:local true})
         database-id (:eva.v2.database.core/id source-config)
         ^File tmpfile (sql/temp-file)
         path (.getPath tmpfile)
         dest-config {:local true
                      :eva.v2.storage.value-store.core/partition-id
                      (random-uuid)
                      :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
                      :eva.v2.storage.block-store.impl.sql/db-spec
                      {:classname "org.h2.Driver"
                       :subprotocol "h2"
                       :subname path
                       :user "SA"}}
         conn (api/connect source-config)
         dest-conn-config (assoc dest-config :eva.v2.database.core/id database-id)]
     (try

       ;; advance source state
       (dotimes [n 160] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))

       ;; seed migration
       (stateful-migration! source-config dest-config database-id)

       (let [dest-conn (api/connect dest-conn-config)]
         (try
           (equivalent-connection-test conn dest-conn)
           ;; advance source state again
           (dotimes [n 200] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))

           (testing "start, but interrupt migration"
             (let [migration-thread (Thread. #(try (stateful-migration! source-config dest-config database-id)
                                                   (catch Throwable t nil)))]
               (.start migration-thread)
               (Thread/sleep 1000)
               (.interrupt migration-thread)))

           (testing "finish in-progress migration"
             (stateful-migration! source-config dest-config database-id))

           (finally (api/release dest-conn))))
       (finally (api/release conn))))))

(deftest unit:migration-with-larger-trees
    (qp/testing-for-resource-leaks
     (let [source-config (auto/expand-config {:local true})
           database-id (:eva.v2.database.core/id source-config)
           ^File tmpfile (sql/temp-file)
           path (.getPath tmpfile)
           dest-config {:local true
                        :eva.v2.storage.value-store.core/partition-id
                        (random-uuid)
                        :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
                        :eva.v2.storage.block-store.impl.sql/db-spec
                        {:classname "org.h2.Driver"
                         :subprotocol "h2"
                         :subname path
                         :user "SA"}}
           conn (api/connect source-config)
           dest-conn-config (assoc dest-config :eva.v2.database.core/id database-id)]
       (try

         (dotimes [n 1000] @(api/transact conn (take 10 (repeatedly #(vector :db/add (api/tempid :db.part/user) :db/doc (str (rand-int n)))))))
         (stateful-migration! source-config dest-config database-id)

         (let [dest-conn (api/connect dest-conn-config)]
           (try
             (equivalent-connection-test conn dest-conn)
             (dotimes [n 10] @(api/transact conn (take 10 (repeatedly #(vector :db/add (api/tempid :db.part/user) :db/doc (str (rand-int n)))))))
             (stateful-migration! source-config dest-config database-id)
             (api/sync-db dest-conn)
             (equivalent-connection-test conn dest-conn)
             (finally (api/release dest-conn))))

       (finally (api/release conn))))))
