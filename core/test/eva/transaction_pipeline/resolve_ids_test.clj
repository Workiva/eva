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

(ns eva.transaction-pipeline.resolve-ids-test
  (:require [eva.api :as eva]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [clojure.test :refer :all]))

(defn test-transaction-pipeline-resolve-ids [conn]
  (let [schema [{:db.install/_attribute :db.part/db
                 :db/id                 (eva/tempid :db.part/db)
                 :db/ident              :a1
                 :db/valueType          :db.type/string
                 :db/unique             :db.unique/identity
                 :db/cardinality        :db.cardinality/one}
                {:db.install/_attribute :db.part/db
                 :db/id                 (eva/tempid :db.part/db)
                 :db/ident              :a2
                 :db/valueType          :db.type/string
                 :db/unique             :db.unique/identity
                 :db/cardinality        :db.cardinality/one}]
        _      @(eva/transact conn schema)]

    (testing "temp ids resolution as connected comoenents"
      (let [t1                        (eva/tempid :db.part/db)
            t2                        (eva/tempid :db.part/db)
            t3                        (eva/tempid :db.part/db)
            facts                     [[:db/add t1 :a1 "x"]
                                       [:db/add t2 :a1 "x"]
                                       [:db/add t2 :a2 "y"]
                                       [:db/add t3 :a2 "y"]
                                       [:db/add 42 :a2 "y"]]
            ;; t1 t2 t3 and 42 are connected components in the graph:
            ;; t1 -- [:a1 "x"]
            ;;          |
            ;; t2 -------
            ;;          |
            ;;       [:a2 "y"]
            ;;          |
            ;; t3 -------
            ;;          |
            ;;         42
            ;; t1 == t2 == t3 == 42
            _                         @(eva/transact conn facts)
            query                     '[:find (count ?e) . :where   (or [?e :a1 _] [?e :a2 _])]
            total-facts-with-a1-or-a2 (eva/q query (eva/db conn))]
        (is (= 1 total-facts-with-a1-or-a2))))))

(deftest unit:transaction-pipeline-resolve-ids
  (with-local-mem-connection conn
    (test-transaction-pipeline-resolve-ids conn)))
