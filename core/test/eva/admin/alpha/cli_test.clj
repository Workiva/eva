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

(ns eva.admin.alpha.cli-test
  (:require [clojure.test :refer :all]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.v2.system.peer-connection.autogenetic :as auto]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.database.core :as database]
            [eva.api :as api]
            [eva.admin.alpha.cli :as admin-cli])
  (:import (java.io File)))

(deftest unit:migrate
  (qp/testing-for-resource-leaks
   (let [source-config (auto/expand-config {:local true})
         database-id (:eva.v2.database.core/id source-config)
         ^File tmpfile (sql/temp-file)
         path (.getPath tmpfile)
         dest-config {:local true
                      :eva.v2.storage.value-store.core/partition-id (java.util.UUID/randomUUID)
                      :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
                      :eva.v2.storage.block-store.impl.sql/db-spec
                      {:classname "org.h2.Driver"
                       :subprotocol "h2"
                       :subname path
                       :user "sa"}}
         conn (api/connect source-config)]
     (try
       (dotimes [n 500] @(api/transact conn [[:db/add (api/tempid :db.part/user) :db/doc (str (rand-int n))]]))
       (testing "starting test migration..."
         (admin-cli/-main "simple-migration" (str source-config) (str dest-config) (str database-id)))
       (testing "...test migration complete."
         (let [new-conn (api/connect (assoc dest-config :eva.v2.database.core/id database-id))]
           (try
             (testing "new conn established."
               (is (not= conn new-conn))
               (doseq [index [:aevt :avet :eavt :vaet]]
                 (is (= (api/datoms (api/db conn) index)
                        (api/datoms (api/db new-conn) index)))))
             (finally (api/release new-conn)))))
       (finally (api/release conn))))))
