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

(ns eva.v2.datastructures.bbtree-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.clojure-test :as chuckt]
            [eva.v2.datastructures.bbtree :as bbt]
            [eva.v2.datastructures.bbtree.logic.v0.tree :as tree]
            [eva.datastructures.utils.comparators :as cmps]
            [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.utils.fressian :refer [def-autological-fn]]
            [eva.datastructures.protocols :as p]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.value-store.core :as value]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]))

(def ^:dynamic test-count 200)

(defn memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (random-uuid)
   ::value/partition-id (random-uuid)})

(def single-comparator tree/default-comparator)
(cmps/defcomparator pair-comparator
  (cmps/bounded-seq-comparator (comparator <) (comparator <)))

(def-autological-fn single-even? even?)
(def-autological-fn pair-even? (comp even? second))
(def-autological-fn single-odd? odd?)
(def-autological-fn pair-odd? (comp odd? second))

(defn gen-key
  "Returns a generator, whose behavior depends on n.
  Generally, the returned generator produces n-length vectors of ints.
  If n = 1, the generator just produces single ints."
  [n]
  (if (= 1 n)
    gen/int
    (gen/vector gen/int n)))

(def gen-query-key-component
  "Returns a value for use by gen-query-key.
  20% of the time this produces cmps/LOWER or cmps/UPPER.
  80% of the time this returns an int."
  (gen/frequency [[1 (gen/elements [cmps/LOWER cmps/UPPER])]
                  [4 gen/int]]))

(defn gen-query-key
  "Returns a generator, whose behavior depends on n.
  Generally, the returned generator produces n-length vectors of values
  produced by gen-query-key-component.
  If n=1, this function simply returns gen-query-key-component."
  [n]
  (if (= 1 n)
    gen-query-key-component
    (gen/vector gen-query-key-component n)))

(def gen-val "The values we store in our maps may be anything." ;; but practicality dictates we not use gen/any
  gen/int)

(defn gen-comparator
  "Returns a very simple generator that always returns the same value,
  depending on the argument c to this function. The comparator produced by
  the generator is designed to work with values produced by an analogous call
  to gen-key."
  [c]
  (condp = c
    1 (gen/no-shrink (gen/return single-comparator))
    2 (gen/no-shrink (gen/return pair-comparator))))

(defn gen-set-into
  "Returns a generator that produces 'into' operations for sets.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :into)
                :items (gen/not-empty (gen/vector (gen-key n)))))

(defn gen-map-into
  "Returns a generator that produces 'into' operations for maps.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :into)
                :items (gen/not-empty (gen/vector (gen/tuple (gen-key n)
                                                             gen-val)))))

(defn gen-assoc
  "Returns a generator that produces 'assoc' operations for maps.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :assoc)
                :key (gen-key n)
                :value gen-val))

(defn gen-dissoc
  "Returns a generator that produces 'dissoc' operations for maps.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :dissoc)
                :key (gen-key n)))

(defn gen-conj
  "Returns a generator that produces 'conj' operations for sets.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :conj)
                :key (gen-key n)))

(defn gen-disj
  "Returns a generator that produces 'disj' operations for sets.
  The argument n is passed along to gen-key."
  [n]
  (gen/hash-map :action (gen/return :disj)
                :key (gen-key n)))

(defn gen-filter-fn
  [n]
  (condp = n
    1 (gen/elements [single-even? single-odd?])
    2 (gen/elements [pair-even? pair-odd?])))

(defn gen-filter
  "Generates 'filter' operations for sets/maps."
  [n]
  (gen/hash-map :action (gen/return :filter)
                :fn (gen-filter-fn n)))

(defn gen-range
  "Returns a generator that produces sorted tuples appropriate for use in a
  range query over a map/set using comparator cmp with keys produced by
  (gen-key n). The tuples themselves are produced by (gen-query-key n)."
  [cmp n]
  (gen/fmap (comp vec (partial sort cmp))
            (gen/tuple (gen-query-key n) (gen-query-key n))))

(defn gen-interval-type
  [low high]
  (gen/bind (gen/elements [:open-open :open-closed :closed-open :closed-closed])
            (fn [type]
              (case type
                :open-open (gen/return (interval/open-interval low high))
                :open-closed (gen/return (interval/interval low high true false))
                :closed-open (gen/return (interval/interval low high false true))
                :closed-closed (gen/return (interval/closed-interval low high))))))

(defn gen-interval
  [cmp n]
  (gen/bind (gen-range cmp n)
            (fn [[low high]]
              (gen-interval-type low high))))

(defn gen-remove-interval
  "Generates intervals to remove."
  [cmp n]
  (gen/hash-map :action (gen/return :remove-interval)
                :interval (gen-interval cmp n)
                :cmp (gen/return cmp)))

(defn gen-keep-interval
  "Generates intervals to keep!"
  [cmp n]
  (gen/hash-map :action (gen/return :keep-interval)
                :interval (gen-interval cmp n)
                :cmp (gen/return cmp)))

(def gen-single-query-action
  "There are four different ways to perform a range query over our structures, of
  varying utility and efficiency. The methods should ultimately all produce
  exactly the same values."
  (gen/elements [:subseq :between :subrange :subranges]))

(defn gen-single-query
  "Returns something of the shape:
  {:action <gen-single-query-action>, :low low, :high high}
  The values low and high are derived from the generator returned by
  (gen-range cmp n)."
  [cmp n]
  (->> (fn [[action [low high]]] (gen/return {:action action :low low :high high}))
       (gen/bind (gen/tuple gen-single-query-action (gen-range cmp n)))))

(defn gen-multi-query
  "Returns something of the shape:
  {:ranges ranges}
  where the value ranges is a non-empty vector of ranges produced by
  the generator returned by (gen-range cmp n)."
  [cmp n]
  (gen/hash-map :ranges (gen/not-empty (gen/vector (gen-range cmp n)))))

(def gen-reincarnate
  "Used to generate a 'reincarnation' operation. This consists of grabbing our
  tree's head pointer (or only the uuid from the pointer), throwing away the
  entire in-memory structure, and then recreating it from the storage backend."
  (gen/one-of [(gen/return {:action :reincarnate})]))

(def gen-order (gen/elements [3 10 100]))
(def gen-buffer (gen/elements [0 3 20 100]))

(defn gen-expanded-set-ops
  "Generates a sequence of ops over a set:
    - 3 parts into
    - 1 part conj
    - 1 part disj
    - 1 part filter
    - 1 part remove-interval
    - 1 part keep-interval
    - 1 part reincarnate"
  [cmp n]
  (gen/not-empty (gen/vector (gen/frequency [[3 (gen-set-into n)]
                                             [1 (gen-conj n)]
                                             [1 (gen-disj n)]
                                             [1 (gen-filter n)]
                                             [1 (gen-remove-interval cmp n)]
                                             [1 (gen-keep-interval cmp n)]
                                             [1 gen-reincarnate]]))))

(defn gen-map-ops
  "Generates a sequence of ops over a map:
    - 3 parts into
    - 2 parts assoc
    - 3 parts dissoc
    - 1 part reincarnate"
  [n]
  (gen/not-empty (gen/vector (gen/frequency [[3 (gen-map-into n)]
                                             [2 (gen-assoc n)]
                                             [3 (gen-dissoc n)]
                                             [1 gen-reincarnate]]))))

(def gen-expanded-set-test
  "Tests:
    - into
    - conj
    - disj
    - destruction & recreation (two ways)
    - range queries
    - multiple simultaneous range queries
    - filter
    - remove-interval
    - keep-interval"
  (gen/bind (gen/one-of [(gen/return 1) (gen/return 2)]) ;; not gen/elements because of shrinking contract
            (fn [n]
              (gen/bind (gen-comparator n)
                        (fn [cmp]
                          (gen/hash-map :comparator (gen/return cmp)
                                        :single-queries (gen/not-empty (gen/vector (gen-single-query cmp n)))
                                        :multi-queries (gen/vector (gen-multi-query cmp n) 2)
                                        :op-sequence (gen-expanded-set-ops cmp n)
                                        :order gen-order
                                        :buffer-size gen-buffer))))))

(def gen-map-subseq-test
  "Tests:
    - range queries
    - multiple simultaneous range queries"
  (gen/bind (gen/one-of [(gen/return 1) (gen/return 2)]) ;; not gen/elements because of shrinking contract
            (fn [n]
              (gen/bind (gen-comparator n)
                        (fn [cmp]
                          (gen/hash-map :comparator (gen/return cmp)
                                        :single-queries (gen/not-empty (gen/vector (gen-single-query cmp n)))
                                        :multi-queries (gen/vector (gen-multi-query cmp n) 2)
                                        :op-sequence (gen-map-ops n)
                                        :order gen-order
                                        :buffer-size gen-buffer))))))

(defn perform-single-query-on-btree
  [tree {action :action, low :low, high :high}]
  (case action
    :subseq
    (subseq tree >= low <= high)
    :between
    (bbt/between tree low high)
    :subrange
    (bbt/subrange tree [low high])
    :subranges
    (get (bbt/subranges tree [[low high]]) [low high])))

(defn perform-single-query-on-other
  [coll {low :low, high :high}]
  (doall (subseq coll >= low <= high)))

(defn perform-multiple-queries-on-btree
  [tree {ranges :ranges}]
  (bbt/subranges tree ranges))

(defn perform-multiple-queries-on-other
  [coll {ranges :ranges}]
  (zipmap ranges
          (doall (map #(perform-single-query-on-other coll {:low (first %),
                                                            :high (second %)})
                      ranges))))

(defn apply-op-to-set
  [st {:keys [action key items fn interval cmp]}]
  (case action
    :conj
    (conj st key)
    :disj
    (disj st key)
    :into
    (into st items)
    :reincarnate
    st
    :filter
    (into (empty st) (filter fn st))
    :remove-interval
    (clojure.set/difference st (filter #(p/interval-contains? interval cmp %) st))
    :keep-interval
    (into (empty st) (filter #(p/interval-contains? interval cmp %) st))))

(defn apply-op-to-transient-set
  [st {:keys [action key items fn interval cmp]}]
  (case action
    :conj
    (conj! st key)
    :disj
    (disj! st key)
    :filter
    (if (satisfies? p/BTreeTransientOps st)
      (bbt/filter! st fn)
      (let [st (persistent! st)]
        (->> st (filter fn) (into (empty (persistent! st))) (transient))))
    :remove-interval
    (if (satisfies? p/BTreeTransientOps st)
      (bbt/remove-interval! st interval)
      (let [st (persistent! st)]
        (transient (clojure.set/difference st (filter #(p/interval-contains? interval cmp %) st)))))
    :keep-interval
    (if (satisfies? p/BTreeTransientOps st)
      (bbt/keep-interval! st interval)
      (let [st (persistent! st)]
        (transient (into (empty st) (filter #(p/interval-contains? interval cmp %) st)))))))

(defn apply-op-to-bset
  [st {:keys [action key items fn interval]}]
  (case action
    :conj
    (conj st key)
    :disj
    (disj st key)
    :into
    (into st items)
    :reincarnate
    st
    :filter
    (bbt/filter st fn)
    :remove-interval
    (bbt/remove-interval st interval)
    :keep-interval
    (bbt/keep-interval st interval)))

(defn apply-op-to-backed-bset
  [store st {:keys [action key items fn interval]}]
  (case action
    :conj
    (conj st key)
    :disj
    (disj st key)
    :into
    (into st items)
    :reincarnate
    (let [st (bbt/persist! st)]
      (bbt/open-writable-set store (p/storage-id st)))
    :filter
    (bbt/filter st fn)
    :remove-interval
    (bbt/remove-interval st interval)
    :keep-interval
    (bbt/keep-interval st interval)))

(defn apply-op-to-map
  [mp {:keys [action key value items fn interval cmp]}]
  (case action
    :assoc
    (assoc mp key value)
    :dissoc
    (dissoc mp key)
    :into
    (into mp items)
    :reincarnate
    mp
    :filter
    (into (empty mp) (filter (comp fn key) mp))
    :remove-interval
    (into (empty mp) (remove (comp #(p/interval-contains? interval cmp %) key) mp))
    :keep-interval
    (into (empty mp) (filter (comp #(p/interval-contains? interval cmp %) key) mp))))

(defn apply-op-to-transient-map
  [mp {action :action, key :key, value :value, items :items}]
  (case action
    :assoc
    (assoc! mp key value)
    :dissoc
    (dissoc! mp key)))

(defn apply-op-to-bmap
  [store mp {:keys [action key value items fn interval]}]
  (case action
    :assoc
    (assoc mp key value)
    :dissoc
    (dissoc mp key)
    :into
    (into mp items)
    :reincarnate
    (let [mp (bbt/persist! mp)]
      (bbt/open-writable-map store (p/storage-id mp)))
    :filter
    (bbt/filter mp fn)
    :remove-interval
    (bbt/remove-interval mp interval)
    :keep-interval
    (bbt/keep-interval mp interval)))

(defn reincarnate-set
  [store tree]
  (apply-op-to-backed-bset store tree (first (gen/sample gen-reincarnate 1))))

(defn reincarnate-map
  [store tree]
  (apply-op-to-bmap store tree (first (gen/sample gen-reincarnate 1))))

(defn nil=empty=
  ([a] true)
  ([a b] (or (and (or (= a ()) (nil? a))
                  (or (= b ()) (nil? b)))
             (= a b)))
  ([a b & more] (and (nil=empty= a b)
                     (apply nil=empty= b more))))

(defn map-nil=empty=
  ([cmp a] true)
  ([cmp a b]
   (let [ks-a (sort-by first cmp (keys a))
         ks-b (sort-by first cmp (keys b))]
     (and (= ks-a ks-b)
          (every? true? (map nil=empty= (map #(get a %) ks-a) (map #(get b %) ks-b))))))
  ([cmp a b & more] (and (map-nil=empty= a b)
                         (apply map-nil=empty= cmp b more))))

(qu/defmanager atom-store-manager
  :discriminator (fn [user-id _] [user-id])
  :constructor (fn [_ _] (atom {})))

(defspec set-equivalence-with-concurrent-serialization
  test-count
  (chuckt/for-all [{cmp :comparator,
                    single-qs :single-queries,
                    multi-qs :multi-queries,
                    op-seq :op-sequence
                    order :order
                    buffer-size :buffer-size} (gen/no-shrink gen-expanded-set-test)]
                  (qp/testing-for-resource-leaks
                   (let [config (memory-config)]
                     (try
                       (let [store (qu/acquire vsc/concurrent-value-store-manager :testing config)
                             empty-bbtree (bbt/backed-sorted-set-by cmp store order buffer-size)
                             empty-btree (bbt/sorted-set-by cmp order buffer-size)
                             empty-set (sorted-set-by cmp)
                             final-bbtree (reduce (partial apply-op-to-backed-bset store) empty-bbtree op-seq)
                             final-btree (reduce apply-op-to-bset empty-btree op-seq)
                             final-set (reduce apply-op-to-set empty-set op-seq)]
                         (is (= (seq (reincarnate-set store final-bbtree)) ;; do they serialize the same?
                                (seq final-btree)
                                (seq final-set)))
                         (doseq [query single-qs] ;; do queries return the same?
                           (is (nil=empty= (perform-single-query-on-btree final-bbtree query)
                                           (perform-single-query-on-btree final-btree query)
                                           (perform-single-query-on-other final-set query))))
                         (doseq [query multi-qs] ;; do multi-range queries return the same?
                           (is (map-nil=empty= cmp
                                               (perform-multiple-queries-on-btree final-bbtree query)
                                               (perform-multiple-queries-on-btree final-btree query)
                                               (perform-multiple-queries-on-other final-set query)))))
                       (finally (qu/release* vsc/concurrent-value-store-manager :testing config true)))))))

(defspec set-equivalence-no-serialization
  test-count
  (chuckt/for-all [{cmp :comparator,
                    single-qs :single-queries,
                    multi-qs :multi-queries,
                    op-seq :op-sequence
                    order :order
                    buffer-size :buffer-size} (gen/no-shrink gen-expanded-set-test)]
                  (qp/testing-for-resource-leaks
                   (try
                     (let [store (qu/acquire atom-store-manager :testing nil)
                           empty-bbtree (bbt/backed-sorted-set-by cmp store order buffer-size)
                           empty-btree (bbt/sorted-set-by cmp order buffer-size)
                           empty-set (sorted-set-by cmp)
                           final-bbtree (reduce (partial apply-op-to-backed-bset store) empty-bbtree op-seq)
                           final-btree (reduce apply-op-to-bset empty-btree op-seq)
                           final-set (reduce apply-op-to-set empty-set op-seq)]
                       (is (= (seq (reincarnate-set store final-bbtree)) ;; do they serialize the same?
                              (seq final-btree)
                              (seq final-set)))
                       (doseq [query single-qs] ;; do queries return the same?
                         (is (nil=empty= (perform-single-query-on-btree final-bbtree query)
                                         (perform-single-query-on-btree final-btree query)
                                         (perform-single-query-on-other final-set query))))
                       (doseq [query multi-qs] ;; do multi-range queries return the same?
                         (is (map-nil=empty= cmp
                                             (perform-multiple-queries-on-btree final-bbtree query)
                                             (perform-multiple-queries-on-btree final-btree query)
                                             (perform-multiple-queries-on-other final-set query)))))
                     (finally (qu/release* atom-store-manager :testing nil true))))))

(defspec map-equivalence-with-concurrent-serialization
  test-count
  (chuckt/for-all [{cmp :comparator,
                    single-qs :single-queries,
                    multi-qs :multi-queries,
                    op-seq :op-sequence
                    order :order
                    buffer-size :buffer-size} (gen/no-shrink gen-map-subseq-test)]
                  (qp/testing-for-resource-leaks
                   (let [config (memory-config)]
                     (try
                       (let [store (qu/acquire vsc/concurrent-value-store-manager :testing config)
                             empty-bbtree (bbt/backed-sorted-map-by cmp store order buffer-size)
                             empty-btree (bbt/sorted-map-by cmp order buffer-size)
                             empty-map (sorted-map-by cmp)
                             final-bbtree (reduce (partial apply-op-to-bmap store) empty-bbtree op-seq)
                             final-btree (reduce apply-op-to-map empty-btree op-seq)
                             final-map (reduce apply-op-to-map empty-map op-seq)]
                         (is (= (seq (reincarnate-map store final-bbtree)) ;; serialize the same?
                                (seq final-btree)
                                (seq final-map)))
                         (doseq [query single-qs] ;; do queries return the same?
                           (is (nil=empty= (perform-single-query-on-btree final-bbtree query)
                                           (perform-single-query-on-btree final-btree query)
                                           (perform-single-query-on-other final-map query))))
                         (doseq [query multi-qs] ;; do multi-range queries return the same?
                           (is (map-nil=empty= cmp
                                               (perform-multiple-queries-on-btree final-bbtree query)
                                               (perform-multiple-queries-on-btree final-btree query)
                                               (perform-multiple-queries-on-other final-map query)))))
                       (finally (qu/release* vsc/concurrent-value-store-manager :testing config true)))))))

(defspec map-equivalence-no-serialization
  test-count
  (chuckt/for-all [{cmp :comparator,
                    single-qs :single-queries,
                    multi-qs :multi-queries,
                    op-seq :op-sequence
                    order :order
                    buffer-size :buffer-size} (gen/no-shrink gen-map-subseq-test)]
                  (qp/testing-for-resource-leaks
                   (try
                     (let [store (qu/acquire atom-store-manager :testing nil)
                           empty-bbtree (bbt/backed-sorted-map-by cmp store order buffer-size)
                           empty-btree (bbt/sorted-map-by cmp order buffer-size)
                           empty-map (sorted-map-by cmp)
                           final-bbtree (reduce (partial apply-op-to-bmap store) empty-bbtree op-seq)
                           final-btree (reduce apply-op-to-map empty-btree op-seq)
                           final-map (reduce apply-op-to-map empty-map op-seq)]
                       (is (= (seq (reincarnate-map store final-bbtree)) ;; serialize the same?
                              (seq final-btree)
                              (seq final-map)))
                       (doseq [query single-qs] ;; do queries return the same?
                         (is (nil=empty= (perform-single-query-on-btree final-bbtree query)
                                         (perform-single-query-on-btree final-btree query)
                                         (perform-single-query-on-other final-map query))))
                       (doseq [query multi-qs] ;; do multi-range queries return the same?
                         (is (map-nil=empty= cmp
                                             (perform-multiple-queries-on-btree final-bbtree query)
                                             (perform-multiple-queries-on-btree final-btree query)
                                             (perform-multiple-queries-on-other final-map query)))))
                     (finally (qu/release* atom-store-manager :testing nil true))))))
