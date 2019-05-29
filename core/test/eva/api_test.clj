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

(ns eva.api-test
  (:require [clojure.test :refer :all]
            [eva.v2.database.index-manager :refer [global-index-cache]]
            [eva.v2.server.transactor-test-utils
             :refer [with-local-mem-connection with-local-sql-connection]]
            [eva.api :refer :all]
            [eva.query.datalog.protocols :as dp]))

(defn test-query [conn]
  (let [init-db (db conn)]
    (is (= #{[0]}
           (set (q '[:find ?e
                     :where [?e :db/ident :db.part/db]]
                   init-db))
           (set (q (pr-str '[:find ?e
                             :where [?e :db/ident :db.part/db]])
                   init-db))
           (set (q '[:find ?e
                     :where [?e 3 :db.part/db]]
                   init-db))
           (set (q (pr-str '[:find ?e
                             :where [?e 3 :db.part/db]])
                   init-db))))

    (is (= #{[:db/ident]}
           (set (q '[:find ?a
                     :in $ ?a
                     :where
                     [0 ?a :db.part/db]]
                   init-db
                   :db/ident))))

    (is (= #{[17]
             [19]
             [20]}
           (set (q '[:find ?e
                     :where [?e :db/cardinality :db.cardinality/many]]
                   init-db))))

    (is (= #{[4] [7] [13] [6] [9] [3] [8] [5] [11] [15] [41]}
           (set (q '[:find ?e
                     :where [?e :db/cardinality :db.cardinality/one]]
                   init-db))))

    (is (= #{:db.install/partition
             :db/cardinality
             :db/unique
             :db/index
             :db.install/attribute
             :db/doc
             :db.install/valueType
             :db/txInstant
             :db/valueType
             :db/fn
             :db/isComponent
             :db/ident
             :db/fulltext
             :db/noHistory}
           (into #{} (q '[:find [?attr-ident ...]
                          :where
                          [:db.part/db :db.install/attribute ?a]
                          [?a :db/ident ?attr-ident]]
                        init-db))
           (into #{} (q (pr-str '[:find [?attr-ident ...]
                                  :where
                                  [:db.part/db :db.install/attribute ?a]
                                  [?a :db/ident ?attr-ident]])
                        init-db))))))
(deftest unit:query
  (testing "with in-memory connection"
    (with-local-mem-connection conn
      (test-query conn)))

  (testing "with transactor and local-storage"
    (with-local-sql-connection conn
      (test-query conn))))

(deftest unit:log-tx-range
  (with-local-mem-connection conn
    (let [_ @(transact conn [[:db/add (tempid :db.part/user) :db/doc "new thing1"]])
          _ @(transact conn [[:db/add (tempid :db.part/user) :db/doc "new thing2"]])
          _ @(transact conn [[:db/add (tempid :db.part/user) :db/doc "new thing3"]])
          log (.log conn)
          txr (.txRange log 0 10)
          data (-> txr second :data)]
      (are [x start end] (= (map :t (.txRange log start end)) x)
        [0 1 2 3] nil nil
        [0 1 2 3] 0 4
        [0 1 2 3] (to-tx-eid 0) 4
        [0] 0 (to-tx-eid 1)
        [3] 3 4
        [2 3] 2 nil)
      (is (= 4 (count txr)))
      (is (= 0 (:t (first txr))))
      (is (= 2 (count data))))))

(deftest unit:datoms-and-query-truthiness
  (with-local-mem-connection conn
    (let [schema [{:db/id                 #db/id[:db.part/db]
                   :db.install/_attribute :db.part/db
                   :db/ident              :cone/bool
                   :db/cardinality        :db.cardinality/one
                   :db/valueType          :db.type/boolean}]
          _ @(transact conn schema)
          tid1 (tempid :db.part/user)
          tid2 (tempid :db.part/user)
          tx [{:db/id     tid1
               :db/ident  :foo
               :cone/bool true}
              {:db/id     tid2
               :db/ident  :bar
               :cone/bool false}]
          tres @(transact conn tx)
          db (:db-after tres)
          [pid1 pid2] (map (resolve-tempid db (:tempids tres)) [tid1 tid2])]

      (are [res spec]
           (= res (set (map :v (apply datoms db :avet spec))))
        #{true false} [:cone/bool]
        #{} [:cone/bool nil]
        #{true} [:cone/bool true]
        #{false} [:cone/bool false])

      (are [res spec]
           (= res (set (dp/extensions db [spec])))
        #{[pid1 :cone/bool]
          [pid2 :cone/bool]} '[?e :cone/bool]
        #{[pid1 :cone/bool true]} '[?e :cone/bool true]
        #{[pid2 :cone/bool false]} '[?e :cone/bool false])

      (are [res query]
           (= res (set (q query db)))
        #{[pid1 true]
          [pid2 false]} '[:find ?e ?v :where [?e :cone/bool ?v]]
        #{[pid1]} '[:find ?e :where [?e :cone/bool true]]
        #{[pid2]} '[:find ?e :where [?e :cone/bool false]]
        ;;#{}       nil ;; TODO: do we want to handle this case?
        ))))

(deftest unit:in-mem-db-evicts-indexes
  (with-local-mem-connection conn
    (let [connect-cache-count (count @(:cache-atom global-index-cache))
          _ (release conn)
          release-cache-count (count @(:cache-atom global-index-cache))]
      (is (= (inc release-cache-count)
             connect-cache-count)))))
