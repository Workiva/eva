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

;; -----------------------------------------------------------------------------
;; This file uses implementation from Datascript which is distributed under the
;; Eclipse Public License (EPLv1.0) at https://github.com/tonsky/datascript with
;; the following notice:
;;
;; Copyright © 2014–2018 Nikita Prokopov
;;
;; Licensed under Eclipse Public License
;; -----------------------------------------------------------------------------

(ns eva.query.pull-test
  (:require [clojure.test :refer :all]
            [eva.api :refer [connect transact db tempid release]]
            [eva.entity-id :refer [->Long]]
            [eva.query.core :refer :all]
            [eva.query.dialect.util :refer [expression]]
            [eva.query.dialect.pull.core :refer [pull pull-many]]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [plumbing.core :as pc]
            [com.rpl.specter.macros :as sm]
            [com.rpl.specter :as specter]))

(def schema
  [{:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :aka
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :child
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/ref}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :friend
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/ref}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :enemy
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/ref}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :father
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :part
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many}

   {:db/id (tempid :db.part/user)
    :db.install/_attribute :db.part/db

    :db/ident :spec
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one}])

(def local-id->tempid
  (zipmap (range 1 28) (repeatedly #(tempid 2))))

(def datoms-tx
  (clojure.walk/postwalk-replace
   local-id->tempid
   (concat
    [[:db/add 1 :name  "Petr"]
     [:db/add 1 :aka   "Devil"]
     [:db/add 1 :aka   "Tupen"]
     [:db/add 2 :name  "David"]
     [:db/add 3 :name  "Thomas"]
     [:db/add 4 :name  "Lucy"]
     [:db/add 5 :name  "Elizabeth"]
     [:db/add 6 :name  "Matthew"]
     [:db/add 7 :name  "Eunan"]
     [:db/add 8 :name  "Kerri"]
     [:db/add 9 :name  "Rebecca"]
     [:db/add 1 :child 2]
     [:db/add 1 :child 3]
     [:db/add 2 :father 1]
     [:db/add 3 :father 1]
     [:db/add 6 :father 3]
     [:db/add 10 :name  "Part A"]
     [:db/add 11 :name  "Part A.A"]
     [:db/add 10 :part 11]
     [:db/add 12 :name  "Part A.A.A"]
     [:db/add 11 :part 12]
     [:db/add 13 :name  "Part A.A.A.A"]
     [:db/add 12 :part 13]
     [:db/add 14 :name  "Part A.A.A.B"]
     [:db/add 12 :part 14]
     [:db/add 15 :name  "Part A.B"]
     [:db/add 10 :part 15]
     [:db/add 16 :name  "Part A.B.A"]
     [:db/add 15 :part 16]
     [:db/add 17 :name  "Part A.B.A.A"]
     [:db/add 16 :part 17]
     [:db/add 18 :name  "Part A.B.A.B"]
     [:db/add 16 :part 18]]
    [[:db/add 1 :friend 1]
     [:db/add 1 :friend 2]
     [:db/add 1 :friend 3]]
    [[:db/add 4 :friend 5]
     [:db/add 5 :friend 6]
     [:db/add 6 :friend 7]
     [:db/add 7 :friend 8]]
    [[:db/add 4 :enemy 6]
     [:db/add 5 :enemy 7]
     [:db/add 6 :enemy 8]
     [:db/add 7 :enemy 4]]
    [[:db/add 21 :part 22]
     [:db/add 22 :part 23]
     [:db/add 23 :part 21]
     [:db/add 21 :spec 22]
     [:db/add 22 :spec 21]]
    [[:db/add 24 :part 25]
     [:db/add 26 :spec 27]])))

(deftest unit:pull-api
  (with-local-mem-connection conn
    (let [schema-res @(transact conn schema)
          datoms-res @(transact conn datoms-tx)
          perm (pc/for-map [[k v] local-id->tempid]
                           k ((:tempids datoms-res) (->Long v)))
          aka-txs (->> (for [i (range 2000)]
                         [:db/add (perm 8) :aka (str "aka-" i)])
                       (partition 100))
          aka-txs-res (doall (for [tx aka-txs] @(transact conn tx)))
          test-db (:db-after (last aka-txs-res))]
      (testing "basic"
        (let [{:keys [name aka]} (pull test-db '[:name :aka] (perm 1))]
          (is (= name "Petr"))
          (is (= (set aka) #{"Devil" "Tupen"})))
        (let [{:keys [name aka]} (pull test-db (pr-str '[:name :aka]) (perm 1))]
          (is (= name "Petr"))
          (is (= (set aka) #{"Devil" "Tupen"})))
        (is (= {:name "Matthew" :father {:db/id (perm 3)} :db/id (perm 6)}
               (pull test-db '[:name :father :db/id] (perm 6))
               (pull test-db (pr-str '[:name :father :db/id]) (perm 6))))
        (is (= [{:name "Petr"} {:name "Elizabeth"}
                {:name "Eunan"} {:name "Rebecca"}]
               (pull-many test-db '[:name] (map perm [1 5 7 9])))))
      (testing "pull-reverse-attr-spec"
        (is (= {:name "David" :_child [{:db/id (perm 1)}]}
               (pull test-db '[:name :_child] (perm 2))))
        (is (= {:name "David" :_child [{:name "Petr"}]}
               (pull test-db '[:name {:_child [:name]}] (perm 2)))))
      (testing "Reverse non-component references yield collections"
        (is (= {:name "Thomas" :_father [{:db/id (perm 6)}]}
               (pull test-db '[:name :_father] (perm 3))))
        (is (= #{{:db/id (perm 2)} {:db/id (perm 3)}}
               (into #{} (:_father (pull test-db '[:name :_father] (perm 1))))))
        (is (= {:name "Thomas" :_father [{:name "Matthew"}]}
               (pull test-db '[:name {:_father [:name]}] (perm 3))))
        (is (= #{{:name "David"} {:name "Thomas"}}
               (into #{} (:_father (pull test-db '[:name {:_father [:name]}] (perm 1)))))))
      (testing "Pull wildcard"
        (let [res (pull test-db '[*] (perm 1))]
          (is (= #{:db/id :name :aka :child :friend} (into #{} (keys res))))
          (is (= #{"Devil" "Tupen"} (into #{} (:aka res))))
          (is (= #{{:db/id (perm 2)}
                   {:db/id (perm 3)}}
                 (into #{} (:child res))))
          (is (= #{{:db/id (perm 1)}
                   {:db/id (perm 2)}
                   {:db/id (perm 3)}}
                 (into #{} (:friend res)))))
        (is (= {:db/id (perm 2) :name "David" :_child [{:db/id (perm 1)}] :father {:db/id (perm 1)}}
               (pull test-db '[* :_child] (perm 2))))
        (testing "explicit attr specs override wildcard"
          (is (= #{{:name "David"} {:name "Thomas"}}
                 (into #{} (:child (pull test-db '[* {:child [:name]}] (perm 1)))))))
        (testing "don't cause a silly NPE when there's a wildcard but *all* non-db-id attributes are explicit."
          (is (pull test-db '[* :aka :name {:child [*] :friend [*]}] (perm 1)))))
      (testing "Empty results return nil"
        (is (every? nil? [(pull test-db '[:foo] (perm 1))
                          (pull test-db '[*] 12345)])))
      (testing "A default can be used to replace nil results"
        (is (= {:foo "bar"}
               (pull test-db '[(default :foo "bar")] (perm 1)))))

      ;; TODO: refine the following test to check equivalence more generally.
      ;;       Ordering of results and walk's implicit conversion of mapentries
      ;;       to vectors makes this a little bit nontrivial
      (testing "pull component attrs"
        (let [parts {:name "Part A",
                     :part
                     [{:db/id (perm 11)
                       :name  "Part A.A",
                       :part
                       [{:db/id (perm 12)
                         :name  "Part A.A.A",
                         :part
                         [{:db/id (perm 13) :name "Part A.A.A.A"}
                          {:db/id (perm 14) :name "Part A.A.A.B"}]}]}
                      {:db/id (perm 15)
                       :name  "Part A.B",
                       :part
                       [{:db/id (perm 16)
                         :name  "Part A.B.A",
                         :part
                         [{:db/id (perm 17) :name "Part A.B.A.A"}
                          {:db/id (perm 18) :name "Part A.B.A.B"}]}]}]}
              setter (pc/fn->>
                      (sm/transform [:part specter/ALL :part specter/ALL :part] (partial into #{}))
                      (sm/transform [:part] (partial into #{})))]
          (testing "Component entities are expanded recursively"
            (is (= (setter parts)  (setter (pull test-db '[:name :part] (perm 10))))))
          (testing "Reverse component references yield a single result"
            (is (= {:name "Part A.A" :_part {:db/id (perm 10)}}
                   (pull test-db [:name :_part] (perm 11))))
            (is (= {:name "Part A.A" :_part {:name "Part A"}}
                   (pull test-db [:name {:_part [:name]}] (perm 11)))))
          (testing "Like explicit recursion, expansion will not allow loops"
            (let [rpart (update-in parts [:part 0 :part 0 :part]
                                   (partial into [{:db/id (perm 10)}]))
                  recdb (:db-after @(transact conn [[:db/add (perm 12) :part (perm 10)]]))]
              (is (= (setter rpart) (setter (pull recdb '[:name :part] (perm 10)))))))))
      (testing "Stubbed component references are preserved in wildcard and non-wildcard pulls."
        (is (= {:part [{:db/id (perm 25)}]}
               (pull test-db [{:part ['*]}] (perm 24))
               (pull test-db [:part] (perm 24))))
        (is (= {:spec {:db/id (perm 27)}}
               (pull test-db [{:spec ['*]}] (perm 26))
               (pull test-db [:spec] (perm 26)))))
      (testing "Without an explicit limit, the default is 1000"
        (is (= 1000 (->> (pull test-db '[:aka] (perm 8)) :aka count))))
      (testing "Explicit limit can reduce the default"
        (is (= 500 (->> (pull test-db '[(limit :aka 500)] (perm 8)) :aka count))))
      (testing "Explicit limit can increase the default"
        (is (= 1500 (->> (pull test-db '[(limit :aka 1500)] (perm 8)) :aka count))))
      (testing "A nil limit produces unlimited results"
        (is (= 2000 (->> (pull test-db '[(limit :aka nil)] (perm 8)) :aka count))))
      (testing "Limits can be used as map specification keys"
        (is (clojure.set/superset? #{{:name "Petr"} {:name "David"} {:name "Thomas"}}
                                   (into #{} (:friend (pull test-db '[:name {(limit :friend 2) [:name]}] (perm 1))))))
        (is (= 2 (count (:friend (pull test-db '[:name {(limit :friend 2) [:name]}] (perm 1)))))))
      (testing "Single attrs yield a map"
        (is (= {:name "Matthew" :father {:name "Thomas"}}
               (pull test-db '[:name {:father [:name]}] (perm 6)))))
      (testing "Multi attrs yield a collection of maps"
        (is (= #{{:name "David"}
                 {:name "Thomas"}}
               (into #{} (:child (pull test-db '[:name {:child [:name]}] (perm 1)))))))
      (testing "Missing attrs are dropped"
        (is (= {:name "Petr"}
               (pull test-db '[:name {:father [:name]}] (perm 1)))))
      (testing "Non matching results are removed from collections"
        (is (= {:name "Petr" :child []}
               (pull test-db '[:name {:child [:foo]}] (perm 1)))))
      (testing "Map specs can override component expansion"
        (let [parts #{{:name "Part A.A"} {:name "Part A.B"}}]
          (is (= parts (into #{} (:part (pull test-db '[:name {:part [:name]}] (perm 10))))))
          (is (= parts (into #{} (:part (pull test-db '[:name {:part 1}] (perm 10))))))))

      (testing "pull recursion"
        (let [friends
              (clojure.walk/prewalk-replace
               perm
               {:db/id 4
                :name  "Lucy"
                :friend [{:db/id 5
                          :name  "Elizabeth"
                          :friend [{:db/id 6
                                    :name  "Matthew"
                                    :friend [{:db/id 7
                                              :name  "Eunan"
                                              :friend [{:db/id 8
                                                        :name  "Kerri"}]}]}]}]})
              enemies
              (clojure.walk/prewalk-replace
               perm
               {:db/id 4 :name "Lucy"
                :friend [{:db/id 5 :name "Elizabeth"
                          :friend [{:db/id 6 :name "Matthew"
                                    :enemy [{:db/id 8 :name "Kerri"}]}]
                          :enemy [{:db/id 7 :name "Eunan"
                                   :friend [{:db/id 8 :name "Kerri"}]
                                   :enemy [{:db/id  4 :name "Lucy"
                                            :friend [{:db/id 5}]}]}]}]
                :enemy [{:db/id 6 :name "Matthew"
                         :friend [{:db/id 7 :name "Eunan"
                                   :friend [{:db/id 8 :name "Kerri"}]
                                   :enemy [{:db/id  4 :name "Lucy"
                                            :friend [{:db/id 5 :name "Elizabeth"}]}]}]
                         :enemy [{:db/id 8 :name "Kerri"}]}]})]
          (testing "Infinite recursion"
            (is (= friends (pull test-db '[:db/id :name {:friend ...}] (perm 4)))))
          (testing "Multiple recursion specs in one pattern"
            (is (= enemies (pull test-db '[:db/id :name {:friend 2 :enemy 2}] (perm 4)))))
          (let [test-db (:db-after @(transact conn [[:db/add (perm 8) :friend (perm 4)]]))]
            (is (= (update-in friends (take 8 (cycle [:friend 0]))
                              assoc :friend [{:db/id (perm 4) :name "Lucy" :friend [{:db/id (perm 5)}]}])
                   (pull test-db '[:db/id :name {:friend ...}] (perm 4)))))))
      (testing "dual recursion"
        (is (= (pull test-db '[:db/id {:part ...} {:spec ...}] (perm 21))
               (clojure.walk/prewalk-replace
                perm
                {:db/id 21,
                 :spec  {:db/id 22
                         :spec  {:db/id 21,
                                 :spec  {:db/id 22}, :part [{:db/id 22}]}
                         :part  [{:db/id 23,
                                  :part  [{:db/id 21,
                                           :spec  {:db/id 22},
                                           :part  [{:db/id 22}]}]}]}
                 :part  [{:db/id 22
                          :spec  {:db/id 21, :spec {:db/id 22}, :part [{:db/id 22}]}
                          :part  [{:db/id 23,
                                   :part  [{:db/id 21,
                                            :spec  {:db/id 22},
                                            :part  [{:db/id 22}]}]}]}]}))))
      (testing "deep recursion"
        (let [start -100
              depth -500
              txd (concat (mapcat
                           (fn [idx]
                             (let [id0 (tempid :db.part/user (inc idx))
                                   id1 (tempid :db.part/user idx)]
                               [[:db/add
                                 id1
                                 :name
                                 (str "Person-" idx)]
                                [:db/add
                                 id0
                                 :friend
                                 id1]]))

                           (range (dec start) depth -1))
                          [[:db/add (tempid :db.part/user start) :name (str "Person-" start)]])
              tx-res @(transact conn txd)
              start-perm (get (:tempids tx-res) (->Long (tempid 2 start)))
              pulled (pull (:db-after tx-res) '[:name {:friend ...}] start-perm)
              path (->> [:friend 0]
                        (repeat (dec (- (- depth start))))
                        (into [] cat))]
          (is (= (str "Person-" (inc depth))
                 (:name (get-in pulled path)))))))))

(deftest unit:basic-with-pull-wildcard
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [(pull ?e [*]) ...]
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= #{{:db/id 4,
                :db/ident :db/valueType,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc "Establishes the type of an attribute."}
               {:db/id 5,
                :db/ident :db/cardinality,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc
                "Establishes the cardinality of an attribute, :db.cardinality/one or :db.cardinality/many."}
               {:db/id 19,
                :db/ident :db.install/partition,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "Asserts that the given entity should be installed as a new partition."}
               {:db/id 6,
                :db/ident :db/unique,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc
                "Asserts either :db.unique/identity or :db.unique/value semantics for an attribute."}
               {:db/id 17,
                :db/ident :db.install/valueType,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "An attribute for installing new value types. Currently does nothing."}
               {:db/id 20,
                :db/ident :db.install/attribute,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "Asserts that the given entity should be installed as a new attribute."}}
             (into #{} (q pull-vts db-tx0 :db.type/ref)))))))

(deftest unit:patternvar-with-pull-wildcard
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [(pull ?e pattern) ...]
                      :in $ pattern ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= #{{:db/id 4,
                :db/ident :db/valueType,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc "Establishes the type of an attribute."}
               {:db/id 5,
                :db/ident :db/cardinality,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc
                "Establishes the cardinality of an attribute, :db.cardinality/one or :db.cardinality/many."}
               {:db/id 19,
                :db/ident :db.install/partition,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "Asserts that the given entity should be installed as a new partition."}
               {:db/id 6,
                :db/ident :db/unique,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 35},
                :db/doc
                "Asserts either :db.unique/identity or :db.unique/value semantics for an attribute."}
               {:db/id 17,
                :db/ident :db.install/valueType,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "An attribute for installing new value types. Currently does nothing."}
               {:db/id 20,
                :db/ident :db.install/attribute,
                :db/valueType {:db/id 32},
                :db/cardinality {:db/id 36},
                :db/doc
                "Asserts that the given entity should be installed as a new attribute."}}
             (into #{} (q pull-vts db-tx0 '[*] :db.type/ref)))))))

(deftest unit:basic-with-pull-select
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [(pull ?e [:db/ident :db/valueType]) ...]
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= #{{:db/ident :db/cardinality, :db/valueType {:db/id 32}}
               {:db/ident :db.install/attribute, :db/valueType {:db/id 32}}
               {:db/ident :db/unique, :db/valueType {:db/id 32}}
               {:db/ident :db/valueType, :db/valueType {:db/id 32}}
               {:db/ident :db.install/valueType, :db/valueType {:db/id 32}}
               {:db/ident :db.install/partition, :db/valueType {:db/id 32}}}
             (into #{} (q pull-vts db-tx0 :db.type/ref)))))))

(deftest unit:patternvar-with-pull-select
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [(pull ?e pattern!) ...]
                      :in $ ?value-type pattern!
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= #{{:db/ident :db/cardinality, :db/valueType {:db/id 32}}
               {:db/ident :db.install/attribute, :db/valueType {:db/id 32}}
               {:db/ident :db/unique, :db/valueType {:db/id 32}}
               {:db/ident :db/valueType, :db/valueType {:db/id 32}}
               {:db/ident :db.install/valueType, :db/valueType {:db/id 32}}
               {:db/ident :db.install/partition, :db/valueType {:db/id 32}}}
             (into #{} (q pull-vts db-tx0 :db.type/ref '[:db/ident :db/valueType])))))))

(deftest unit:find-scalar-with-pull
  (with-local-mem-connection conn
    (let [pull-vts  '[:find (pull ?e [:db/ident :db/valueType]) .
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (contains?
           #{{:db/ident :db/cardinality, :db/valueType {:db/id 32}}
             {:db/ident :db.install/attribute, :db/valueType {:db/id 32}}
             {:db/ident :db/unique, :db/valueType {:db/id 32}}
             {:db/ident :db/valueType, :db/valueType {:db/id 32}}
             {:db/ident :db.install/valueType, :db/valueType {:db/id 32}}
             {:db/ident :db.install/partition, :db/valueType {:db/id 32}}}
           (q pull-vts db-tx0 :db.type/ref))))))

(deftest unit:find-tuple-with-pull
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [?e ?ident (pull ?e [:db/valueType])]
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]
                      [?e :db/ident ?ident]]
          db-tx0 (db conn)
          res (q pull-vts db-tx0 :db.type/ref)]
      (is (contains?
           #{[5 :db/cardinality {:db/valueType {:db/id 32}}]
             [20 :db.install/attribute {:db/valueType {:db/id 32}}]
             [17 :db.install/valueType {:db/valueType {:db/id 32}}]
             [4 :db/valueType {:db/valueType {:db/id 32}}]
             [6 :db/unique {:db/valueType {:db/id 32}}]
             [19 :db.install/partition {:db/valueType {:db/id 32}}]}
           res)))))

(deftest unit:find-rel-with-pull
  (with-local-mem-connection conn
    (let [pull-vts  '[:find ?e (pull ?e [:db/ident]) (pull ?e [:db/valueType])
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (=
           #{[5 {:db/ident :db/cardinality} {:db/valueType {:db/id 32}}]
             [20 {:db/ident :db.install/attribute} {:db/valueType {:db/id 32}}]
             [17 {:db/ident :db.install/valueType} {:db/valueType {:db/id 32}}]
             [4 {:db/ident :db/valueType} {:db/valueType {:db/id 32}}]
             [6 {:db/ident :db/unique} {:db/valueType {:db/id 32}}]
             [19 {:db/ident :db.install/partition} {:db/valueType {:db/id 32}}]}
           (into #{} (q pull-vts db-tx0 :db.type/ref)))))))

(deftest unit:find-tuple-2
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [?e (pull ?e [:db/ident]) (pull ?e [:db/valueType])]
                      :in $ ?value-type
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (contains?
           #{[5 {:db/ident :db/cardinality} {:db/valueType {:db/id 32}}]
             [20 {:db/ident :db.install/attribute} {:db/valueType {:db/id 32}}]
             [17 {:db/ident :db.install/valueType} {:db/valueType {:db/id 32}}]
             [4 {:db/ident :db/valueType} {:db/valueType {:db/id 32}}]
             [6 {:db/ident :db/unique} {:db/valueType {:db/id 32}}]
             [19 {:db/ident :db.install/partition} {:db/valueType {:db/id 32}}]}
           (q pull-vts db-tx0 :db.type/ref))))))

(deftest unit:find-tuple-2-with-patternvars
  (with-local-mem-connection conn
    (let [pull-vts  '[:find [?e (pull ?e pattern1) (pull ?e pattern2)]
                      :in $ ?value-type pattern1 pattern2
                      :where
                      [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (contains?
           #{[5 {:db/ident :db/cardinality} {:db/valueType {:db/id 32}}]
             [20 {:db/ident :db.install/attribute} {:db/valueType {:db/id 32}}]
             [17 {:db/ident :db.install/valueType} {:db/valueType {:db/id 32}}]
             [4 {:db/ident :db/valueType} {:db/valueType {:db/id 32}}]
             [6 {:db/ident :db/unique} {:db/valueType {:db/id 32}}]
             [19 {:db/ident :db.install/partition} {:db/valueType {:db/id 32}}]}
           (q pull-vts db-tx0 :db.type/ref [:db/ident] [:db/valueType]))))))

(deftest unit:pull-rel-with-empty-results
  (with-local-mem-connection conn
    (let [q1 '[:find (pull ?e [:db/ident])
               :where [?e :db/ident :nonextant]]]
      (is (= '() (q q1 (db conn)))))))

(deftest unit:pull-passed-questionable-data
  (with-local-mem-connection conn
    ;; lookup ref, eid exists
    (is (= #:db{:ident :db.part/db} (pull (db conn) [:db/ident] [:db/ident :db.part/db])))

    ;; lookup ref, eid does not exist
    (is (= nil (pull (db conn) [:db/ident] [:db/ident :foo])))

    ;; lookup ref on db/id, eid does exist
    (is (= #:db{:id 0} (pull (db conn) [:db/id] [:db/ident :db.part/db])))

    ;; lookup ref on db/id, eid does not exist
    (is (= nil (pull (db conn) [:db/id] [:db/ident :foo])))

    ;; eid does not exist
    (is (= nil (pull (db conn) [:db/ident] 123)))

    ;; entid does exist
    (is (= {:db/ident :db.part/db} (pull (db conn) [:db/ident] 0)))

    (is (= {:db/id 123} (pull (db conn) [:db/id] 123)))
    (is (= {:db/id 123} (pull (db conn) [:db/id :db/ident] 123)))

    (is (= nil (pull (db conn) [:db/id] nil)))
    (is (= nil (pull (db conn) [:db/ident] nil)))))


(deftest unit:multi-src-pull-in-queries
  (with-local-mem-connection conn
    (let [db (db conn)]
      (testing "collection multi-src pulls"
        (is (= [#:db{:doc "The default database partition."}]
               (q '[:find [(pull $db1 ?db-ident-eid [:db/doc]) ...]
                    :in $db1
                    :where
                    [$db1 ?db-ident-eid :db/ident :db.part/db]] db))))
      (testing "scalar multi-src pulls"
        (is (= #:db{:doc "The default database partition."}
               (q '[:find (pull $db1 ?db-ident-eid [:db/doc]) .
                    :in $db1
                    :where
                    [$db1 ?db-ident-eid :db/ident :db.part/db]] db))))
      (testing "tupled multi-src pulls"
        (is (= [#:db{:doc "The default database partition."} #:db{:doc "The user transaction partition."}]
               (q '[:find [(pull $db1 ?db-ident-eid [:db/doc]) (pull $db2 ?user-ident-eid [:db/doc])]
                     :in $db1 $db2
                     :where
                     [$db1 ?db-ident-eid :db/ident :db.part/db]
                    [$db2 ?user-ident-eid :db/ident :db.part/user]] db db))))
      (testing "multi-src pulls with implicit src"
        (is (= [[#:db{:doc "The default database partition."} #:db{:doc "The user transaction partition."}]]
               (q '[:find (pull ?db-ident-eid [:db/doc]) (pull $db1 ?user-ident-eid [:db/doc])
                    :in $ $db1
                    :where
                    [?db-ident-eid :db/ident :db.part/db]
                    [$db1 ?user-ident-eid :db/ident :db.part/user]] db db))))
      (testing "multi-src pulls"
        (is (= [[#:db{:doc "The default database partition."} #:db{:doc "The user transaction partition."}]]
               (q '[:find (pull $db1 ?db-ident-eid [:db/doc]) (pull $db2 ?user-ident-eid [:db/doc])
                     :in $db1 $db2
                     :where
                     [$db1 ?db-ident-eid :db/ident :db.part/db]
                     [$db2 ?user-ident-eid :db/ident :db.part/user]] db db)))))))
