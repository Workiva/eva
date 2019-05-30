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

(ns eva.query.query-test
  (:require [eva.query.core :refer [q inspect]]
            [eva.api :refer [connect db release transact tempid datoms]]
            [clojure.set :as set]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]])
  (:use [clojure.test]))

(def id-counter (atom 10000))
(defn next-id [] (swap! id-counter inc))

(defn ent->datoms [ent]
  (let [id (:db/id ent)
        ent (dissoc ent :db/id)]
    (for [[a v] ent]
      [id a v])))

;; Movie Theater Schema
(def movie-schema-entities
  [{:db/id                 (next-id)
    :db/ident              :movie/title
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id                 (next-id)
    :db/ident              :movie/director
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id                 (next-id)
    :db/ident              :movie/actor
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id                 (next-id)
    :db/ident              :person.name/preferred
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id                 (next-id)
    :db/ident              :person.name/born
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id                 (next-id)
    :db/ident              :person.name.preferred/structure
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id                 (next-id)
    :db/ident              :person.name.born/structure
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id    (next-id)
    :db/ident :person.name.structure/given-middle-family}
   {:db/id    (next-id)
    :db/ident :person.name.structure/given-family}

   {:db/id          (next-id)
    :db/ident       :theater.location/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id          (next-id)
    :db/ident       :theater.location/address
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id          (next-id)
    :db/ident       :theater.location/phone-number
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/id          (next-id)
    :db/ident       :theater.showing/location
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/id          (next-id)
    :db/ident       :theater.showing/movie
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/id          (next-id)
    :db/ident       :theater.showing/starts
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])

(def movie-db0 (into #{} (mapcat ent->datoms movie-schema-entities)))

(def movie-db1
  (set/union movie-db0
             (mapcat ent->datoms
                     [{:db/id                           (next-id)
                       :person.name/preferred           "J.J. Abrams"
                       :person.name.preferred/structure :person.name.structure/given-family
                       :person.name/born                "Jeffrey Jacob Abrams"
                       :person.name.born/structure      :person.name.structure/given-middle-family}
                      {:db/id                           (next-id)
                       :person.name/preferred           "Harrison Ford"
                       :person.name.preferred/structure :person.name.structure/given-family}
                      {:db/id                           (next-id)
                       :person.name/preferred           "Mark Hamill"
                       :person.name.perferred/structure :person.name.structure/given-family}
                      {:db/id                           (next-id)
                       :person.name/preferred           "Domhnall Gleeson"
                       :person.name.preferred/structure :person.name.structure/given-family}
                      {:db/id                           (next-id)
                       :person.name/preferred           "Alicia Vikander"
                       :person.name.preferred/structure :person.name.structure/given-family}
                      {:db/id                           (next-id)
                       :person.name/preferred           "Alex Garland"
                       :person.name.preferred/structure :person.name.structure/given-family}])))

(defn find-attr-val [db attr val]
  (filter (fn [[e a v & _]] (and (= attr a) (= val v))) db))

(def movie-db2
  (let [swtfa (next-id)
        exmac (next-id)]
    (set/union movie-db1
               (mapcat ent->datoms
                       [{:db/id          swtfa
                         :movie/title    "Star Wars: The Force Awakens"
                         :movie/director (->> (find-attr-val movie-db1 :person.name/preferred "J.J. Abrams")
                                              ffirst)}
                        {:db/id          exmac
                         :movie/title    "Ex Machina"
                         :movie/director (->> (find-attr-val movie-db1 :person.name/preferred "Alex Garland")
                                              ffirst)}])
               [[swtfa :movie/actor (ffirst (find-attr-val movie-db1 :person.name/preferred "Mark Hamill"))]
                [swtfa :movie/actor (ffirst (find-attr-val movie-db1 :person.name/preferred "Harrison Ford"))]
                [exmac :movie/actor (ffirst (find-attr-val movie-db1 :person.name/preferred "Domhnall Gleeson"))]
                [exmac :movie/actor (ffirst (find-attr-val movie-db1 :person.name/preferred "Alicia Vikander"))]])))

(deftest test:query-evaluation
  (testing "query returning relation"
    (is (= #{["Star Wars: The Force Awakens" "Harrison Ford"]
             ["Star Wars: The Force Awakens" "Mark Hamill"]
             ["Ex Machina" "Domhnall Gleeson"]
             ["Ex Machina" "Alicia Vikander"]}
           (set (q '[:find ?title ?actor-name
                     :where
                     [?movie :movie/title ?title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]]
                   movie-db2)))))
  (testing "query returning tuple"
    (is (contains? #{["Star Wars: The Force Awakens" "Harrison Ford"]
                     ["Star Wars: The Force Awakens" "Mark Hamill"]
                     ["Ex Machina" "Domhnall Gleeson"]
                     ["Ex Machina" "Alicia Vikander"]}
                   (q '[:find [?title ?actor-name]
                        :where
                        [?movie :movie/title ?title]
                        [?movie :movie/actor ?actor]
                        [?actor :person.name/preferred ?actor-name]]
                      movie-db2))))
  (testing "query returning collection"
    (is (= #{"Harrison Ford" "Mark Hamill"}
           (set (q '[:find [?actor-name ...]
                     :where
                     [?movie :movie/title "Star Wars: The Force Awakens"]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]]
                   movie-db2)))))
  (testing "query returning scalar"
    (is (contains? #{"Harrison Ford" "Mark Hamill"}
                   (q '[:find ?actor-name .
                        :where
                        [?movie :movie/title "Star Wars: The Force Awakens"]
                        [?movie :movie/actor ?actor]
                        [?actor :person.name/preferred ?actor-name]]
                      movie-db2)))))

(deftest test:rules
  (testing "with simple rule"
    (is (= #{"Harrison Ford" "Mark Hamill"}
           (set (q '[:find [?actor-name ...]
                     :in % $
                     :where
                     (movie-actor "Star Wars: The Force Awakens" ?actor-name)]
                   '[[(movie-actor ?movie ?actor)
                      [?m :movie/title ?movie]
                      [?m :movie/actor ?a]
                      [?a :person.name/preferred ?actor]]]
                   movie-db2)))))
  (testing "same-generation"
    (is (= #{:john :george}
           (set (q '[:find [?x ...]
                     :in $par %
                     :where ($par sgc :john ?x)]
                   '[[:john :henry] [:henry :mary] [:george :jim] [:jim :mary]]
                   '[[(sgc [?x] ?y)
                      [(identity ?x) ?y]]
                     [(sgc [?x] ?y)
                      [?x ?a]
                      (sgc ?a ?b)
                      [?y ?b]]])))))
  (testing "Non-propagation of _"
    (is (= '[[bar]]
           (q '[:find ?x
                :in $ % ?z
                :where
                [?y :some-attr ?z]
                (foo-rule ?y ?x _)]                         ;; <--
              [[0 :some-attr 'foo]
               [0 :another-attr 1]
               [1 :some-attr 'bar]]
              '[[(foo-rule ?a ?b ?leaky!)
                 [?a :another-attr ?leaky!]
                 [?leaky! :some-attr ?d]
                 [(identity ?d) ?b]]]
              'foo))))
  (testing "addition"
    (= 11
       (q '[:find ?x .
            :in %
            :where
            (add 4 7 ?x)]
          '[[(add [?x ?y] ?z)
             (not [(zero? ?y)])
             (add ?a ?b ?z)
             [(inc ?x) ?a]
             [(dec ?y) ?b]]
            [(add [?x ?y] ?z)
             [(zero? ?y)]
             [(identity ?x) ?z]]]))))

(deftest test:multi-src-rules
  (testing "name your multi-srcs with abandon (appendix)."
    (is (= #{:a :o}
           (set (q '[:find [?x ...]
                     :in $p $q $r %
                     :where
                     ($p $q $r s ?x)]
                   [[:c :d] [:b :c] [:c :b]]
                   [[:e :a] [:a :i] [:i :o]]
                   [[:d :e]]
                   '[[($p $q $r
                          s ?x)
                      ($p $q $r
                          n :c ?x)]
                     [($z $f $t
                          n ?x ?y)
                      [$z ?x ?z]
                      ($z $f $t
                          n ?z ?w)
                      [$f ?w ?y]]
                     [($v $m $x
                          n ?x ?y)
                      [$x ?x ?y]]])))))
  (testing "rules can be written independently assuming a single default src."
    (is (= #{1 2}
           (set (q '[:find [?y ...]
                     :in $a $b ?x %
                     :where
                     (or ($a my-rule ?x ?y)
                         ($b my-rule ?x ?y))]
                   [[0 1]]
                   [[0 2]]
                   0
                   '[[(my-rule ?a ?b)
                      [?a ?b]]])))))
  (testing "multi-src rules, with a $ default"
    (is (= #{1 2}
           (set (q '[:find [?y ...]
                     :in $a $ ?x %
                     :where
                     (or (my-rule ?x ?y)
                         ($a my-rule ?x ?y))]
                   [[0 1]]
                   [[0 2]]
                   0
                   '[[(my-rule ?a ?b)
                      [?a ?b]]])))))
  (testing "multi-src propagation works correctly with scoped or rules."
    (is (= #{1}
           (set (q '[:find [?y ...]
                     :in $a $b ?x %
                     :where
                     ($a or (my-rule ?x ?y)
                         (my-rule ?x ?y))]
                   [[0 1]]
                   [[0 2]]
                   0
                   '[[(my-rule ?a ?b)
                      [?a ?b]]])))))
  (testing "scoping an or clause still allows reference to other edbs in scope."
    (is (= #{1 2}
           (set (q '[:find [?y ...]
                     :in $a $b ?x %
                     :where
                     ($a or (my-rule ?x ?y)
                         ($b my-rule ?x ?y))]
                   [[0 1]]
                   [[0 2]]
                   0
                   '[[(my-rule ?a ?b)
                      [?a ?b]]])))))
  (testing "multi-src-rules test with more complex nesting"
    (is (= #{1}
           (set (q '[:find [?y ...]
                     :in $ $a $b ?x %
                     :where
                     [?x ?y]
                     ($a $b has-a-foo ?y)]
                   [[0 1] [0 2]]
                   [[1 :worm]]
                   [[2 :rotten]
                    [1 :rotten]]
                   0
                   '[[($x $y has-a-foo ?thing)
                      ($x one-check ?thing)
                      ($y another-check ?thing)]
                     [(one-check ?apple)
                      [?apple :worm]]
                     [($smell-db another-check ?banana)
                      [$smell-db ?banana :rotten]]]))))))

(deftest test:negation
  (testing "simplest negation"
    (is (= #{:b :d}
           (set (q '[:find [?x ...]
                     :in $q $f
                     :where
                     [$q :a ?x]
                     ($f not [:a ?x])]
                   [[:a :b] [:a :c] [:a :d]]
                   [[:a :c]]))))))

(deftest test:multiple-sources
  (is (= #{[:a :foo] [:b :bar]}
         (set (q '[:find ?x ?y
                   :in $db1 $
                   :where
                   [$db1 ?id ?x]
                   [$ ?id ?y]]
                 #{[1 :a :extra] [2 :b :extra]}
                 #{[1 :foo] [2 :bar]})))))

(defn permutations [s]
  (lazy-seq
    (if (seq (rest s))
      (apply concat (for [x s]
                      (map #(cons x %) (permutations (remove #{x} s)))))
      [s])))

(deftest test:function-expressions
  (testing "with function scalar binding"
    (is (= #{"Harrison Ford" "Mark Hamill"}
           (set (q '[:find [?id ...]
                     :where
                     [?movie :movie/title "Star Wars: The Force Awakens"]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]
                     [(identity ?actor-name) ?id]]
                   movie-db2)))))
  (testing "with function tuple binding"
    (require 'clojure.string)
    (is (= #{["Harrison" "Ford"] ["Mark" "Hamill"]}
           (set (q '[:find ?first-name ?last-name
                     :where
                     [?movie :movie/title "Star Wars: The Force Awakens"]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]
                     [(clojure.string/split ?actor-name #" ") [?first-name ?last-name]]]
                   movie-db2)))))
  (testing "with function relation binding"
    (require 'clojure.string)
    (is (= #{["Harrison" "Ford"] ["Ford" "Harrison"]
             ["Mark" "Hamill"] ["Hamill" "Mark"]}
           (set (q '[:find ?s1 ?s2
                     :where
                     [?movie :movie/title "Star Wars: The Force Awakens"]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]
                     [(clojure.string/split ?actor-name #" ") ?split-name]
                     [(eva.query.query-test/permutations ?split-name) [[?s1 ?s2]]]]
                   movie-db2))))))

(deftest test:input-bindings
  (testing "query with scalar input binding"
    (is (= #{"Harrison Ford" "Mark Hamill"}
           (set (q '[:find [?actor-name ...]
                     :in ?movie-title $
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]]
                   "Star Wars: The Force Awakens"
                   movie-db2)))))
  (testing "query with collection input binding"
    (is (= #{"Star Wars: The Force Awakens"}
           (set (q '[:find [?movie-title ...]
                     :in $ [?actor-name ...]
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]]
                   movie-db2
                   ["Harrison Ford" "Mark Hamill"])))))
  (testing "query with collection input binding, passing in set"
    (is (= #{"Star Wars: The Force Awakens"}
           (set (q '[:find [?movie-title ...]
                     :in $ [?actor-name ...]
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]]
                   movie-db2
                   #{"Harrison Ford" "Mark Hamill"})))))
  (testing "query with tuple input binding"
    (is (= #{"Star Wars: The Force Awakens"}
           (set (q '[:find [?movie-title ...]
                     :in $ [?first-name ?last-name]
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]
                     [(clojure.string/split ?actor-name #" ") [?first-name ?last-name]]]
                   movie-db2
                   ["Harrison" "Ford"])))))
  (testing "query with relation input binding"
    (is (= #{"Star Wars: The Force Awakens"}
           (set (q '[:find [?movie-title ...]
                     :in $ [[?first-name ?last-name]]
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]
                     [?actor :person.name/preferred ?actor-name]
                     [(clojure.string/split ?actor-name #" ") [?first-name ?last-name]]]
                   movie-db2
                   [["Harrison" "Ford"] ["Mark" "Hamill"]]))))))

(deftest test:query-aggregates
  (testing "simple count aggregate"
    (is (= #{["Star Wars: The Force Awakens" 2]
             ["Ex Machina" 2]}
           (set (q '[:find ?movie-title (count ?actor)
                     :where
                     [?movie :movie/title ?movie-title]
                     [?movie :movie/actor ?actor]]
                   movie-db2)))))
  (testing "min aggregate - single return"
    (is (== 5
            (q '[:find (min ?x) .
                 :in $1 $2
                 :where [$1 ?x] [$2 ?x]]
               (map list (range 10))
               (map list (range 5 15))))))
  (testing "min aggregate - multiple return"
    (is (= #{5 6 7}
           (set (q '[:find (min 3 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))))
  (testing "min aggregate - multiple return v.2"
    (is (= #{5 6 7 8 9}
           (set (q '[:find (min 100 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))))
  (testing "max aggregate"
    (is (== 9
            (q '[:find (max ?x) .
                 :in $1 $2
                 :where [$1 ?x] [$2 ?x]]
               (map list (range 10))
               (map list (range 5 15))))))
  (testing "max aggregate - multiple return"
    (is (= #{7 8 9}
           (set (q '[:find (max 3 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))))
  (testing "max aggregate - multiple return v.2"
    (is (= #{5 6 7 8 9}
           (set (q '[:find (max 100 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))))
  (testing "rand aggregate"
    (is (every? #{5 6 7 8 9}
                (q '[:find (rand 1 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))
    (is (every? #{5 6 7 8 9}
                (q '[:find (rand 3 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))
    (is (every? #{5 6 7 8 9}
                (q '[:find (rand 5 ?x) .
                     :in $1 $2
                     :where [$1 ?x] [$2 ?x]]
                   (map list (range 10))
                   (map list (range 5 15)))))
    (is ((every-pred #(= 100 (count %))
                     (partial every? #{5 6 7 8 9}))
          (q '[:find (rand 100 ?x) .
               :in $1 $2
               :where [$1 ?x] [$2 ?x]]
             (map list (range 10))
             (map list (range 5 15))))))
  (testing "sample aggregate"
    (is ((every-pred (partial apply distinct?)
                     (partial every? #{5 6 7 8 9}))
          (q '[:find (sample 1 ?x) .
               :in $1 $2
               :where [$1 ?x] [$2 ?x]]
             (map list (range 10))
             (map list (range 5 15)))))
    (is ((every-pred (partial apply distinct?)
                     (partial every? #{5 6 7 8 9}))
          (q '[:find (sample 3 ?x) .
               :in $1 $2
               :where [$1 ?x] [$2 ?x]]
             (map list (range 10))
             (map list (range 5 15)))))
    (is ((every-pred (partial apply distinct?)
                     (partial every? #{5 6 7 8 9}))
          (q '[:find (sample 5 ?x) .
               :in $1 $2
               :where [$1 ?x] [$2 ?x]]
             (map list (range 10))
             (map list (range 5 15)))))
    (is ((every-pred (partial apply distinct?)
                     (partial every? #{5 6 7 8 9}))
          (q '[:find (sample 100 ?x) .
               :in $1 $2
               :where [$1 ?x] [$2 ?x]]
             (map list (range 10))
             (map list (range 5 15))))))
  (testing "count-distinct aggregate"
    (is (== 4
            (q '[:find (count-distinct ?x) .
                 :with ?y
                 :where [?x ?y]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]]))))
  (testing "sum aggregate"
    (is (== 9
            (q '[:find (sum ?x) .
                 :with ?y
                 :where [?x ?y]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]])))
    (is (== 6
            (q '[:find (sum ?x) .
                 :where [?x _]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]]))))
  (testing "avg aggregate"
    (is (== 0.9
            (q '[:find (avg ?x) .
                 :with ?y
                 :where [?x ?y]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]])))
    (is (== 1.5
            (q '[:find (avg ?x) .
                 :where [?x _]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]]))))
  (testing "median aggregate"
    (is (== 1
            (q '[:find (median ?x) .
                 :with ?y
                 :where [?x ?y]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]])))
    (is (== 1.5
            (q '[:find (median ?x) .
                 :where [?x _]]
               [[0 0] [0 1] [0 2] [0 3]
                [1 0] [1 1] [1 2] [1 3]
                [2 0]
                [3 0]])))))

(deftest test:failing-keyword-values
  (is (= #{[:q]}
         (set (q '[:find ?x
                   :where
                   [:a ?x]
                   [?x :b]]
                 #{[:a :q]
                   [:q :b]}))))
  (is (= [:q]
         (q '[:find [?x ...]
              :where
              [:a ?x]
              [?x :b]]
            #{[:a :q]
              [:q :b]}))))

(deftest test:false-values
  (testing "Simple extensional db match with value 'false'"
    (is (= #{[1 true] [2 false]}
           (set (q '[:find ?x ?y
                     :where
                     [1 ?x ?y]]
                   [[1 1 true]
                    [1 2 false]])))))
  (testing "false return value from scalar binding function"
    (is (= #{[1 true] [2 false]}
           (set (q '[:find ?x ?y
                     :where
                     [?x ?z]
                     [(not ?z) ?y]]
                   [[1 false]
                    [2 true]]))))))

(deftest unit:basic-query
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $ ?value-type
                         :where [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= '#{(4) (20) (6) (17) (5) (19)}
             (set (q get-vt-attrs db-tx0 :db.type/ref))))
      (is (= '#{(3)}
             (set (q get-vt-attrs db-tx0 :db.type/keyword))))
      (is (= '#{(7) (13) (41) (8)}
             (set (q get-vt-attrs db-tx0 :db.type/boolean)))))))

(deftest unit:unused-inputs
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $ ?value-type ?unused-lvar
                         :where [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (= '#{(4) (20) (6) (17) (5) (19)}
             (set (q get-vt-attrs db-tx0 :db.type/ref 'blah-blah))))
      (is (= '#{(3)}
             (set (q get-vt-attrs db-tx0 :db.type/keyword 'doof-doof))))
      (is (= '#{(7) (13) (41) (8)}
             (set (q get-vt-attrs db-tx0 :db.type/boolean 'uh-huh--uh-huh)))))))

(deftest unit:throw-on-full-scans
  (with-local-mem-connection conn
    (let [db (db conn)
          q1 '[:find ?e ?a ?v ?t
               :in $
               :where [?e ?a ?v ?t]]
          q2 '[:find ?e ?a ?v ?t
               :in $ ?v
               :where [?e ?a ?v ?t]]
          q3 '[:find ?e ?a ?v ?t
               :in $ ?t
               :where [?e ?a ?v ?t]]
          q4 '[:find ?e ?a ?v ?t
               :in $ ?v ?t
               :where [?e ?a ?v ?t]]]
      (are [query args]
           (thrown-with-msg? Exception #"Insufficient binding"
                             (apply q query db args))
        q1 []
        q2 [:v]
        q3 [:t]
        q4 [:v :t]))))

(deftest unit:not-clause
  (with-local-mem-connection conn
    (let [db (db conn)
          all-attrs (q '[:find [?attr-ident ...]
                         :where
                         [:db.part/db :db.install/attribute ?attr-id]
                         [?attr-id :db/ident ?attr-ident]]
                       db)

          kw-attrs (q '[:find [?attr-ident ...]
                        :where
                        [:db.part/db :db.install/attribute ?attr-id]
                        [?attr-id :db/ident ?attr-ident]
                        [?attr-id :db/valueType :db.type/keyword]]
                      db)

          all-but-kw (q '[:find [?attr-ident ...]
                          :where
                          [:db.part/db :db.install/attribute ?attr-id]
                          [?attr-id :db/ident ?attr-ident]
                          (not
                            [?attr-id :db/valueType :db.type/keyword])]
                        db)]
      (is (= (count all-attrs) (+ (count kw-attrs)
                                  (count all-but-kw))))
      (is (= (set all-attrs)
             (clojure.set/union (set kw-attrs) (set all-but-kw)))))))

(deftest unit:not-join-clause
  (with-local-mem-connection conn
    (let [db (db conn)
          all-attrs (q '[:find [?attr-ident ...]
                         :where
                         [:db.part/db :db.install/attribute ?attr-id]
                         [?attr-id :db/ident ?attr-ident]]
                       db)

          ref-attrs (q '[:find [?attr-ident ...]
                         :where
                         [:db.part/db :db.install/attribute ?attr-id]
                         [?attr-id :db/ident ?attr-ident]
                         [?attr-id :db/valueType :db.type/ref]]
                       db)

          all-but-ref (q '[:find [?attr-ident ...]
                           :where
                           [:db.part/db :db.install/attribute ?attr-id]
                           [?attr-id :db/ident ?attr-ident]
                           (not-join [?attr-id]
                                     [?attr-id :db/valueType :db.type/ref])]
                         db)]
      (is (= (count all-attrs) (+ (count ref-attrs)
                                  (count all-but-ref))))
      (is (= (set all-attrs)
             (clojure.set/union (set ref-attrs) (set all-but-ref)))))))

(deftest unit:or-clause
  (is (= #{"Mark Hamill" "Harrison Ford" "Domhnall Gleeson" "Alicia Vikander"}
         (set (q '[:find [?actor-name ...]
                   :where
                   [?movie :movie/director ?director]
                   [?movie :movie/actor ?actor]
                   [?actor :person.name/preferred ?actor-name]
                   [?director :person.name/preferred ?director-name]
                   (or [(= ?director-name "J.J. Abrams")]
                       [(= ?director-name "Alex Garland")])]
                 movie-db2))))
  (is (thrown? Exception
               (set (q '[:find [?actor-name ...]
                         :where
                         [?movie :movie/director ?director]
                         [?movie :movie/actor ?actor]
                         [?actor :person.name/preferred ?actor-name]
                         [?director :person.name/preferred ?director-name]
                         (or [(= ?director-name "J.J. Abrams")]
                             [(identity ?director-name) ?actor-name]
                             [(= ?director-name "Alex Garland")])]
                       movie-db2)))
      "The sets of lvars occurring in an or-clause's bodies must be identical")
  (is (thrown? Exception
               (set (q '[:find [?actor-name ...]
                         :where
                         [?movie :movie/director ?director]
                         [?movie :movie/actor ?actor]
                         [?actor :person.name/preferred ?actor-name]
                         (or (and [?director :person.name/preferred ?director-name]
                                  [(= ?director-name "J.J. Abrams")])
                             (and [?director :person.name/preferred ?director-name]
                                  [(= ?director-name "Alex Garland")]))]
                       movie-db2)))
      "or-clauses require all bindings required by their subclauses"))

(deftest unit:or-join-clause
  (is (= #{"Mark Hamill" "Harrison Ford" "Domhnall Gleeson" "Alicia Vikander"}
         (set (q '[:find [?actor-name ...]
                   :where
                   [?movie :movie/director ?director]
                   [?movie :movie/actor ?actor]
                   [?actor :person.name/preferred ?actor-name]
                   (or-join [?director]
                            (and [(= ?director-name "J.J. Abrams")]
                                 [?director :person.name/preferred ?director-name])
                            [?director :person.name/preferred "Alex Garland"])]
                 movie-db2))))
  (is (thrown? Exception
               (set (q '[:find [?actor-name ...]
                         :where
                         [?movie :movie/director ?director]
                         [?movie :movie/actor ?actor]
                         [?actor :person.name/preferred ?actor-name]
                         (or-join [?director ?director-name]
                                  (and [(= ?director-name "J.J. Abrams")]
                                       [?director :person.name/preferred ?director-name])
                                  [?director :person.name/preferred "Alex Garland"])]
                       movie-db2)))
      "all lvars in the head of an or-join rule must occur in the body")
  (is (= #{"Mark Hamill" "Harrison Ford" "Domhnall Gleeson" "Alicia Vikander"}
         (set (q '[:find [?actor-name ...]
                   :where
                   [?movie :movie/director ?director]
                   [?movie :movie/actor ?actor]
                   [?actor :person.name/preferred ?actor-name]
                   (or-join [?director-name]
                            (and [?director :person.name/preferred ?director-name]
                                 [(= ?director-name "J.J. Abrams")])
                            (and [?director :person.name/preferred ?director-name]
                                 [(= ?director-name "Alex Garland")]))]
                 movie-db2)))
      "unlike or-clauses, or-join clauses do *not* require all bindings required by their subclauses"))

(deftest unit:only-aggregate-in-find-spec
  (with-local-mem-connection conn
    (let [db (db conn)
          cnt '[:find (count ?a) :where [:db.part/db :db.install/attribute ?a]]]
      (is (= 14 (ffirst (q cnt db)))))))

(deftest unit:basic-join
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $ ?value-type
                         :where
                         [?e :db/valueType ?value-type]
                         [?e :db/ident :db.install/attribute]]
          db-tx0 (db conn)]
      (is (= '#{(20)}
             (set (q get-vt-attrs db-tx0 :db.type/ref)))))))

(deftest unit:basic-predicate
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $ ?value-type
                         :where
                         [?e :db/valueType ?value-type]
                         [(> ?e 10)]]
          db-tx0 (db conn)]
      (is (= '#{(17) (19) (20)}
             (set (q get-vt-attrs db-tx0 :db.type/ref)))))))

(deftest unit:basic-function
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $
                         :where
                         [(= ?equal true)]
                         [(= ?ident :db.part/db) ?equal]
                         [?e :db/ident ?ident]]
          db-tx0 (db conn)]
      (is (= '#{(0)}
             (set (q get-vt-attrs db-tx0)))))))

(deftest unit:basic-function-backwards
  (with-local-mem-connection conn
    (let [get-vt-attrs '[:find ?e
                         :in $
                         :where
                         [(+ 0 0) ?e]
                         [?e :db/ident ?ident]]
          db-tx0 (db conn)]
      (is (= '#{(0)}
             (set (q get-vt-attrs db-tx0)))))))

(deftest unit:find-scalar
  (with-local-mem-connection conn
    (let [pull-vts '[:find ?e .
                     :in $ ?value-type
                     :where
                     [?e :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (contains?
            #{4 20 6 17 5 19}
            (q pull-vts db-tx0 :db.type/ref))))))

(deftest unit:find-tuple
  (with-local-mem-connection conn
    (let [pull-vts '[:find [?e ?ident]
                     :in $ ?value-type
                     :where
                     [?e :db/valueType ?value-type]
                     [?e :db/ident ?ident]]
          db-tx0 (db conn)]
      (is (contains?
            '#{(20 :db.install/attribute) (4 :db/valueType)
               (17 :db.install/valueType) (19 :db.install/partition)
               (5 :db/cardinality) (6 :db/unique)}
            (q pull-vts db-tx0 :db.type/ref))))))

(deftest unit:unbound-first-data-pattern
  (with-local-mem-connection conn
    (let [vt1 '[:find ?value-type .
                :where
                [_ :db/valueType ?value-type]]
          vt2 '[:find ?value-type .
                :where
                [?_ :db/valueType ?value-type]]
          db-tx0 (db conn)]
      (is (contains? #{34 31 26 32 30 25}
                     (q vt1 db-tx0)))
      (is (contains? #{34 31 26 32 30 25}
                     (q vt2 db-tx0))))))

(deftest unit:rand-scalar-aggregate
  (with-local-mem-connection conn
    (let [q1 '[:find (rand 2 ?p) .
               :where
               ;; get a random partition
               [_ :db.install/partition ?p]]
          db-tx0 (db conn)]
      (is (every? #{0 1 2} (q q1 db-tx0))))))

(deftest unit:count-rel-aggregate
  (with-local-mem-connection conn
    (let [q1 '[:find ?vtype (count ?attr-eid)
               :where
               ;; get a random partition
               [:db.part/db :db.install/attribute ?attr-eid]
               [?attr-eid :db/valueType ?vtype]]
          db-tx0 (db conn)]
      (is (= '#{(34 1) (26 4) (25 1) (32 6) (31 1) (30 1)}
             (into #{} (q q1 db-tx0)))))))

(deftest unit:ignored-inputs
  (with-local-mem-connection conn
    ;; NOTE: scalar and collection input binding forms don't permit the var to be _
    (testing "tuple inputs can be ignored"
      (let [q1 '[:find ?e
                 :in $ [_]
                 :where
                 [?e :db/ident :db/ident]
                 ]
            db-tx0 (db conn)]
        (is (= '#{(3)}
               (into #{} (q q1 db-tx0 [42]))))))
    (testing "relation inputs can be ignored"
      (let [q1 '[:find ?e
                 :in $ [[_]]
                 :where
                 [?e :db/ident :db/ident]]
            db-tx0 (db conn)]
        (is (= '#{(3)}
               (into #{} (q q1 db-tx0 [[42]]))))))

    (testing "variant of original failing case"
      (let [query '[:find ?e ?v ?attr
                    :in $ [[?e ?a ?v _ _]]
                    :where
                    (or [?a :db/ident :db.part/db]
                        [?a :db/ident :db/ident])
                    [?a :db/ident ?attr]]
            snapshot (db conn)]
        (is (= [[0 :db.part/db :db/ident]]
               (q query snapshot [[0 3 :db.part/db "foo" "bar"]])))))))
