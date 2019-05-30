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

(ns eva.transaction.map-expansion-test
  (:require [eva.api :refer [connect transact resolve-tempid]]
            [eva.readers]
            [eva.entity-id :refer [tempid]]
            [eva.datom :refer [datom]]
            [eva.api :as api :refer [datoms]]
            [clojure.test :refer :all]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]))

(def child-temp (tempid :db.part/db))
(def children-attr {:db/id child-temp
                    :db.install/_attribute :db.part/db
                    :db/ident :children
                    :db/valueType :db.type/ref
                    :db/cardinality :db.cardinality/many})

(def label-temp (tempid :db.part/db))
(def label-attr {:db/id label-temp
                 :db.install/_attribute :db.part/db
                 :db/ident :label
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one})

;; TODO: Figure out how to drastically reduce the brute force nature of these tests

(deftest unit:single-entity
  (with-local-mem-connection conn
    (let [schema-tx [(assoc children-attr
                            :db/ident :child
                            :db/cardinality :db.cardinality/one)
                     label-attr]
          n1 (tempid :db.part/user -1)
          n2 (tempid :db.part/user -2)
          maps-tx [{:db/id n1
                    :label "n1"
                    :child {:db/id n2
                            :label "n2"}}]
          init-res @(transact conn schema-tx)
          res @(transact conn maps-tx)

          db (api/db conn)

          child-eid (api/entid db [:db/ident :child])
          label-eid (api/entid db [:db/ident :label])
          [n1 n2] (map (partial resolve-tempid db (:tempids res)) [n1 n2])]
      (is (= 4 (count (:tx-data @(transact conn maps-tx)))))
      (is (clojure.set/subset?
           #{(datom [n1 child-eid n2 4398046511106 true])
             (datom [n1 label-eid "n1" 4398046511106 true])
             (datom [n2 label-eid "n2" 4398046511106 true])}
           (set (:tx-data res)))))))

(deftest unit:children-have-db-id
  (with-local-mem-connection conn
    (let [schema-tx [children-attr
                     label-attr]
          n1 (tempid :db.part/user -1)
          n2 (tempid :db.part/user -2)
          n3 (tempid :db.part/user -3)
          maps-tx [{:db/id    n1
                    :label    "n1"
                    :children [{:label "n2"
                                :db/id n2}
                               {:label "n3"
                                :db/id n3}]}]
          init-res @(transact conn schema-tx)
          res @(transact conn maps-tx)

          db (api/db conn)

          child-eid (api/entid db [:db/ident :children])
          label-eid (api/entid db [:db/ident :label])

          [n1 n2 n3] (map (partial resolve-tempid db (:tempids res)) [n1 n2 n3])]

      (is (= 6 (count (:tx-data res))))
      (is (clojure.set/subset?
           #{(datom [n1 child-eid n2 4398046511106 true])
             (datom [n1 child-eid n3 4398046511106 true])
             (datom [n1 label-eid "n1" 4398046511106 true])
             (datom [n2 label-eid "n2" 4398046511106 true])
             (datom [n3 label-eid "n3" 4398046511106 true])}
           (set (:tx-data res)))))))

(deftest unit:parent-is-component
  (with-local-mem-connection conn
    (let [schema-tx [(assoc children-attr :db/isComponent true)
                     label-attr]
          n1 (tempid :db.part/user -1)
          maps-tx [{:db/id    n1
                    :label    "n1"
                    :children [{:label "n2"}
                               {:label "n3"}]}]
          init-res @(transact conn schema-tx)
          res @(transact conn maps-tx)

          db (api/db conn)

          child-eid (api/entid db [:db/ident :children])
          label-eid (api/entid db [:db/ident :label])

          [n1] (map (partial resolve-tempid db (:tempids res)) [n1])]
      (is (= 6 (count (:tx-data res))))
      (let [{:keys [label children]} (api/pull (api/db conn)
                                               [:label {:children [:label]}]
                                               n1)]
        (is (= "n1" label)
            (= #{{:label "n2"} {:label "n3"}}
               (set children)))))))

(deftest unit:children-have-unique-attr
  (with-local-mem-connection conn
    (let [schema-tx [children-attr
                     (assoc label-attr :db/unique :db.unique/identity)]
          n1 (tempid :db.part/user -1)
          maps-tx [{:db/id    n1
                    :label    "n1"
                    :children [{:label "n2"}
                               {:label "n3"}]}]
          init-res @(transact conn schema-tx)
          res @(transact conn maps-tx)

          db (api/db conn)

          child-eid (api/entid db [:db/ident :children])
          label-eid (api/entid db [:db/ident :label])

          [n1] (map (partial resolve-tempid db (:tempids res)) [n1])]
      (is (= 6 (count (:tx-data res))))
      (let [{:keys [label children]} (api/pull (api/db conn)
                                               [:label {:children [:label]}]
                                               [:label "n1"])]
        (is (= "n1" label))
        (is (= #{{:label "n2"} {:label "n3"}}
               (set children)))))))

(deftest unit:deep-nesting
  (with-local-mem-connection conn
    (let [schema-tx [children-attr
                     (assoc label-attr :db/unique :db.unique/identity)]
          [n1 n2 n3 n4 n5 n6 n7] (map (comp (partial tempid :db.part/user)
                                            - inc)
                                      (range 7))
          maps-tx [{:db/id    n1
                    :label    "n1"  ;; TODO: Have the characteristics of this test changed by having made all db/ids explicit?
                    :children [{:db/id n2
                                :label    "n2"
                                :children [{:db/id n4
                                            :label "n4"}
                                           {:db/id n5
                                            :label    "n5"
                                            :children [{:db/id n6
                                                        :label "n6"}
                                                       {:db/id n7
                                                        :label "n7"}]}]}
                               {:db/id n3
                                :label "n3"}]}]
          init-res @(transact conn schema-tx)
          res  @(transact conn maps-tx)
          db (api/db conn)

          child-eid (api/entid db [:db/ident :children])
          label-eid (api/entid db [:db/ident :label])

          [n1 n2 n3 n4 n5 n6 n7] (map (partial resolve-tempid db (:tempids res)) [n1 n2 n3 n4 n5 n6 n7])]
      (is (= 14 (count (:tx-data res))))
      (is (clojure.set/subset?
           (set (map datom
                     (list [n1 label-eid "n1" 4398046511106 true]
                           [n1 child-eid n2 4398046511106 true]
                           [n1 child-eid n3 4398046511106 true]
                           [n2 child-eid n4 4398046511106 true]
                           [n2 label-eid "n2" 4398046511106 true]
                           [n2 child-eid n5 4398046511106 true]
                           [n3 label-eid "n3" 4398046511106 true]
                           [n7 label-eid "n7" 4398046511106 true]
                           [n6 label-eid "n6" 4398046511106 true]
                           [n5 child-eid n6 4398046511106 true]
                           [n5 label-eid "n5" 4398046511106 true]
                           [n5 child-eid n7 4398046511106 true]
                           [n4 label-eid "n4" 4398046511106 true])))
           (set (:tx-data res)))))))

(defn run-lookup-ref-tx [conn tx]
  (let [res @(transact conn tx)
        eid (-> (:tempids res) vals first)
        db (:db-after res)]
    (->> (datoms db :eavt eid :test-attr)
         (map :v)
         (into #{}))))

(deftest unit:cmany-lookup-ref-interactions
  (with-local-mem-connection conn
    (let [schema [{:db/id #db/id[:db.part/db],
                   :db/ident :test-attr
                   :db/cardinality :db.cardinality/many
                   :db/valueType :db.type/ref
                   :db.install/_attribute :db.part/db}]
          _ @(transact conn schema)
          tx1 [{:db/id #db/id[:db.part/user -1] :test-attr :db.part/db}]
          tx2 [{:db/id #db/id[:db.part/user -1] :test-attr [:db.part/db :db.part/user]}]
          tx3 [{:db/id #db/id[:db.part/user -1] :test-attr [:db/ident :db.part/user]}]
          tx4 [{:db/id #db/id[:db.part/user -1] :test-attr [:db.part/db :db.part/tx :db.part/user]}]
          tx5 [{:db/id #db/id[:db.part/user -1] :test-attr [[:db/ident :db.part/user]]}]]
      (are [x y] (= x (run-lookup-ref-tx conn y))
        #{0}     tx1
        #{0 2}   tx2
        #{3 2}   tx3
        #{0 1 2} tx4
        #{2}     tx5))))

(deftest unit:merge-ents-by-id
  (with-local-mem-connection conn
    (let [schema [{:db/id #db/id[:db.part/db],
                   :db/ident :test-attr
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string
                   :db/unique :db.unique/value
                   :db.install/_attribute :db.part/db}
                  {:db/id #db/id[:db.part/db]
                   :db/ident :count
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/long
                   :db.install/_attribute :db.part/db}]

          _ @(transact conn schema)

          tx1 [{:db/id #db/id[:db.part/user] :test-attr "1" :db/doc "one"}
               {:db/id #db/id[:db.part/user] :test-attr "1" :count 1}]]
      (= 1 (count (:tempids @(transact conn tx1)))))))


(deftest unit:merge-ents-by-unique
  (with-local-mem-connection conn
    (let [schema [{:db/id #db/id[:db.part/db],
                   :db/ident :test-attr
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string
                   :db/unique :db.unique/value
                   :db.install/_attribute :db.part/db}

                  {:db/id #db/id[:db.part/db]
                   :db/ident :has-things
                   :db/cardinality :db.cardinality/many
                   :db/valueType :db.type/ref
                   :db.install/_attribute :db.part/db}

                  {:db/id #db/id[:db.part/db]
                   :db/ident :count
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/long
                   :db.install/_attribute :db.part/db}]
          _ @(transact conn schema)
          tx1 [{:db/id #db/id[:db.part/user -1]
                :has-things [{:test-attr "1"
                              :db/doc "one"}]}
               {:db/id #db/id[:db.part/user -2]
                :has-things [{:test-attr "1"
                              :count 2}]}]]
      (= 6 (count (:tx-data @(transact conn tx1)))))))

(deftest unit:merge-ents-by-unique-2
  (with-local-mem-connection conn
    (let [schema [{:db/id #db/id[:db.part/db],
                   :db/ident :test-attr
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string
                   :db/unique :db.unique/value
                   :db.install/_attribute :db.part/db}

                  {:db/id #db/id[:db.part/db]
                   :db/ident :has-things
                   :db/cardinality :db.cardinality/many
                   :db/valueType :db.type/ref
                   :db.install/_attribute :db.part/db}

                  {:db/id #db/id[:db.part/db]
                   :db/ident :count
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/long
                   :db.install/_attribute :db.part/db}]
          _ @(transact conn schema)
          tx1 [{:db/id #db/id[:db.part/user -1]
                :has-things [{:test-attr "1"
                              :db/doc "one"}]}
               {:db/id #db/id[:db.part/user -2]
                :has-things [{:test-attr "1"
                              :count 2}]}]

          tx2 [{:db/id #db/id[:db.part/user -1]
                :test-attr "2"
                :has-things {:test-attr "2"}}]

          tx3 [{:db/id #db/id[:db.part/user]
                :test-attr "3"
                :has-things {:test-attr "3"}}
               {:db/id #db/id[:db.part/user]
                :test-attr "3"
                :has-things {:test-attr "3"}}]

          tx4 [{:db/id #db/id[:db.part/user]
                :test-attr "4"
                :has-things {:test-attr "5"}}
               {:db/id #db/id[:db.part/user]
                :test-attr "5"
                :has-things {:test-attr "4"}}]]

      (= 6 (count (:tx-data @(transact conn tx1))))
      (= 3 (count (:tx-data @(transact conn tx2))))
      (= 3 (count (:tx-data @(transact conn tx3))))
      (= 5 (count (:tx-data @(transact conn tx4)))))))

(deftest unit:duplicate-reverse-attributes
  (with-local-mem-connection conn
    (let [;; reverse attributes should be able to be safely merged
          ;; in the map-expansion process.
          tres @(transact conn [{:db/id #db/id[:db.part/db]
                                 :db.install/_partition :db.part/db
                                 :db/ident :service.part/account}
                                {:db/id #db/id[:db.part/db]
                                 :db.install/_partition :db.part/db
                                 :db/ident :service.part/account}])]
      (is (= {true 3} (frequencies (map :added (:tx-data tres))))))))

(deftest unit:failing-cases)
  ;; TODO: check the following exceptions:
  ;;
  ;; map->adds missing :db/id
  ;; map->adds only :db/id
  ;; expand-cmany not :db.cardinality/many
  ;; valid-unnamed? nested either component or unique
  ;; expand-op encounters non-ref map
