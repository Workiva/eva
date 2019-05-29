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

(ns eva.datastructures.test-version.logic.balance
  "A case study in uglifying code."
  (:require [eva.datastructures.test-version.logic.protocols :refer :all]
            [eva.datastructures.test-version.logic.nodes :refer :all]
            [eva.datastructures.test-version.logic.state :as state]
            [eva.datastructures.test-version.logic.buffer :as buffer]
            [eva.v2.datastructures.bbtree.storage :refer [node-pointer?]]
            [eva.datastructures.utils.core :refer [arg-cmp]]
            [barometer.core :as metrics]
            [clojure.math.numeric-tower :refer [floor ceil]]
            [utiliva.core :refer [group-like partition-map]]
            [utiliva.comparator :refer [compare-comp]]))

;; ******************************************************
;; ##############   BEGIN MOCK OPERATIONS  ##############
;; ******************************************************

(def ^:private ^:dynamic *max-size* 0)
(def ^:private ^:dynamic *min-size* 0)
(defn- min-size [max-size] (ceil (/ max-size 2)))

(defrecord MockResult [sizes ops depth touched])

;; we first minimize the number of untouched nodes that we touch; then we
;; minimize the maximum size of any new nodes that are created (this reduces
;; chances that we trigger a buffer overflow); then we minimize the number of
;; actual balancing operations required.
(defn <touched [res1 res2] (- (count (:touched res1)) (count (:touched res2))))
(defn <max [res1 res2] (- (apply max (:sizes res1)) (apply max (:sizes res2))))
(defn <ops [res1 res2] (- (:depth res1) (:depth res2)))
(defn select [results] (arg-cmp (compare-comp <touched <max <ops) identity results))

(defn shift*
  "Given two numbers representing node sizes, this tries to redistribute such that
  the smallest is not smaller than *min-size*."
  [s1 s2]
  (cond (and (< s1 *min-size*) (> s2 *min-size*))
        (let [diff (min (- *min-size* s1) (- s2 *min-size*))] (list (+ s1 diff) (- s2 diff)))
        (and (> s1 *min-size*) (< s2 *min-size*))
        (let [diff (min (- *min-size* s2) (- s1 *min-size*))] (list (- s1 diff) (+ s2 diff)))
        :else nil))

(defn combine*
  "Given two numbers representing node sizes, this tries to combine them such that
  the new size is not greater than *max-size*."
  [s1 s2]
  (let [c (+ s1 s2)]
    (if (<= c *max-size*) c nil)))

(def keywordize {shift* :shift combine* :combine})

(defn sandvich
  "Creates a vector from smashing two collections together with a value in the middle:
  (sandvich [1 2 3] 4 [5 6 7]) ⇒ [1 2 3 4 5 6 7]"
  [c1 v c2]
  `[~@c1 ~v ~@c2])

(defn recombine
  [op sizes idx res]
  (condp = op
    shift* (apply assoc sizes (interleave [idx (inc idx)] res))
    combine* (let [[sizes-left rst] (split-at idx sizes)
                   sizes-right (second (split-at 2 rst))]
               (sandvich sizes-left res sizes-right))))

(declare op-on-idx)
(defn ops-from-idx
  [sizes ref-idx idx depth]
  (let [idxs (range idx (dec (count sizes)))]
    (if (empty? idxs)
      nil
      (let [res (for [index idxs
                      op [shift* combine*]
                      :let [ref-index (+ ref-idx (- index idx))
                            res (op-on-idx sizes ref-index index depth op)]
                      :when res]
                  res)]
        (if (empty? res) nil res)))))

(defn op-on-idx
  [sizes ref-idx idx depth op]
  (if (some #(< % *min-size*) (first (split-at idx sizes)))
    nil
    (if-let [res (op (nth sizes idx) (nth sizes (inc idx)))]
      (let [new-touched (disj (sorted-set ref-idx (inc ref-idx)))
            sizes (recombine op sizes idx res)]
        (if (every? #(<= *min-size* %) sizes)
          (->MockResult sizes, (list (list (keywordize op) idx)), depth, new-touched)
          (if-let [result (select (map #(update % :touched into new-touched)
                                       (ops-from-idx sizes (inc ref-idx) (if (= op shift*) (inc idx) idx) (inc depth))))]
            (update result :ops conj (list (keywordize op) idx))
            nil)))
      nil)))

(defn plan
  [max sizes]
  (binding [*max-size* max
            *min-size* (min-size max)]
    (-> (ops-from-idx (vec sizes) 0 0 0)
        select
        (select-keys [:ops :touched]))))

;; ******************************************************
;; ##############   BEGIN REAL OPERATIONS  ##############
;; ******************************************************
;; Warning: probably most complicated code in the btree.

;; Note to future maintainers: feel free to combine shift-left and shift-right.
;; I did via function generation. It was ugly as sin. I separated them again for clarity's sake.
(defn shift-left
  "Shifts n children from the left side of rnode to the right side of lnode."
  [lnode rnode n]
  (let [leaf? (leaf-node? lnode)
        kvs-to-shift (take n (children rnode))
        msgs (map #(get (messages rnode) %) (keys kvs-to-shift))]
    (if leaf?
      (list (reduce node-conj lnode kvs-to-shift)
            (remove-values rnode (keys kvs-to-shift))) ;; not node-dissoc because min-rec and max-rec need to be updated.
      (list (add-children lnode (zipmap (vals kvs-to-shift) msgs))
            (remove-children rnode (keys kvs-to-shift))))))

(defn shift-right
  "Shifts n children from the right side of lnode to the left side of rnode."
  [lnode rnode n]
  (let [leaf? (leaf-node? lnode)
        kvs-to-shift (take-last n (children lnode))
        msgs (map #(get (messages lnode) %) (keys kvs-to-shift))]
    (if leaf?
      (list (remove-values lnode (keys kvs-to-shift)) ;; not node-dissoc because min-rec and max-rec need to be updated.
            (reduce node-conj rnode kvs-to-shift))
      (list (remove-children lnode (keys kvs-to-shift))
            (add-children rnode (zipmap (vals kvs-to-shift) msgs))))))

(defn shift
  "Moves n values (and associated messages) left or right, from lnode to rnode
  or rnode to lnode. Moves the minimal number possible to bring the underflowing
  node as close as possible to its minimum size."
  [lnode rnode]
  (cond (and (underflowed? lnode) ((complement underflowed?) rnode))
        (shift-left lnode rnode (min (- (min-node-size lnode) (node-size lnode))
                                     (- (node-size rnode) (min-node-size rnode))))
        (and (underflowed? rnode) ((complement underflowed?) lnode))
        (shift-right lnode rnode (min (- (node-size lnode) (min-node-size lnode))
                                      (- (min-node-size rnode) (node-size rnode))))
        :else
        [lnode rnode]))

(defn combine
  "If they're leaf nodes, it just combines the children collections.
  If they're internal nodes, it combines the children collections, tweaks the
  pivot keys, and distributes the messages."
  [lnode rnode]
  (if (leaf-node? lnode)
    (list (reduce node-conj
                  (node-empty lnode)
                  (concat (children lnode) (children rnode))))
    (list (add-children lnode (zipmap (vals (children rnode))
                                      (map #(get (messages rnode) %) (keys (children rnode))))))))

(defn plan-step
  "Given a collection of nodes, and an action from a plan sequence,
  this carries out that action on the collection, returning the result."
  [c [action k :as step]]
  (let [[left-side stuff] (split-at k c)
        [[lnode rnode] right-side] (split-at 2 stuff)]
    (concat left-side
            (condp = action
              :shift (shift lnode rnode)
              :combine (combine lnode rnode))
            right-side)))

(defn balance-single-seq
  "Takes a sequence of nodes and a plan for rebalancing, returns the rebalanced sequence."
  [s plan]
  (reduce plan-step s (:ops plan)))

(defn neighborhoods-matching
  "Given a sequence of things, finds all 'immediate neighborhoods' centered around
  elements matching pred. Example:
  (neighborhoods-matching even? [1 2 4 5 7 9 11 12]) => ((1 2 4 5) (11 12))"
  [pred c]
  (let [[x y & more :as c] (seq c)]
    (loop [ r (if (or (pred x) (and y (pred y))) [x] [])
           [x y z & _ :as c] c
           f []]
      (if (not c)
        (if (empty? r) f (conj f r))
        (if (and y (or (pred y) (pred x)))
          (recur (conj r y) (next c) f)
          (recur (if (and y z (or (pred y) (pred z))) [y] [])
                 (next c)
                 (if (empty? r) f (conj f r))))))))

(def balance-resolution-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:resolve-touched-batch.counter
                           (metrics/counter "Counts calls to resolve-touched-batch")))

(def balance-resolved-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:resolved-at-balance.counter
                           (metrics/counter "Counts the number of pointers that are resolved in order to rebalance.")))

(def balance-resolved-histogram
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:resolved-at-balance.histogram
                           (metrics/histogram (metrics/reservoir) "The number of pointers resolved at a time in order to rebalance.")))

(def balance-resolution-required-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:resolve-touched-necessary.counter
                           (metrics/counter "Counts calls to resolve-touched-batch that actually do something.")))

(defn resolve-touched-batch
  "Takes an ordered collection of unbalanced sequences, and a corresponding ordered collection
  of plans. Returns an almost identical ordered collection of unbalanced sequences, where any
  node-pointers 'touched' by the plans have been resolved in one large batch."
  [unbalanced plans]
  (metrics/increment balance-resolution-counter)
  (group-like
   (->> (mapcat (fn [ubseq plan] (map-indexed #(vector (contains? (:touched plan) %) %2) ubseq))
                unbalanced
                plans) ;; [touched? node/pointer]
        (partition-map first ;; touched?
                       {true #(partition-map node-pointer?
                                             {true (comp pointers->nodes ;; <- functionally operative
                                                         (fn [args] ;; <- only for the metric.
                                                           (metrics/increment balance-resolved-counter
                                                                              (count args))
                                                           (metrics/update balance-resolved-histogram
                                                                           (count args))
                                                           (metrics/increment balance-resolution-required-counter)
                                                           args))}
                                             (map second %)) ;; map second => node/pointer
                        false #(map second %)})) ;; map second => node/pointer
   unbalanced))

(def balance-node-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:balanced-nodes.counter
                           (metrics/counter "The number of nodes which needed to balance their children.")))

(def balance-batched-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:balance-batched.counter
                           (metrics/counter "Counts the calls to balance-batched.")))

(def balance-batched-histogram
  (metrics/get-or-register metrics/DEFAULT 'eva.datastructures.test-version.logic.balance:balance-group-sizes.histogram
                           (metrics/histogram (metrics/reservoir)
                                              "Tracks the cardinality of the node groups being balanced.")))

(defn balance-batched
  "Given a sequence of nodes who need to perform some balancing among their children,
  this returns a sequence of those nodes after the rebalancing has occurred.
  THIS FUNCTION ASSUMES A UNIFORM NODE ORDER (MIN-WIDTH)."
  [nodes]
  (metrics/increment balance-batched-counter)
  (metrics/increment balance-node-counter (count nodes))
  (let [unbalanced-groups (map #(neighborhoods-matching (comp underflowed? val)
                                                        (children %))
                               nodes)
        ;; ^^ For each node, get a list of all "neighborhoods" that need to be balanced. (cap/child pairs)
        original-keys (map #(mapcat keys %) unbalanced-groups)
        ;; ^^ For each node, a list of keys that will need to be dissoc'ed.
        unbalanced-groups (map #(map vals %) unbalanced-groups)
        ;; ^^ The actual nodes/pointers.
        order (node-order (first nodes)) ;; <== assumption.
        plans-of-action (for [node-group unbalanced-groups]
                          (for [neighborhood node-group
                                :let [sizes (map node-size neighborhood)]]
                            (plan order (map node-size neighborhood))))
        ;; ^^ corresponding plans for coalescing the neighborhoods
        resolved-groups (group-like (resolve-touched-batch (apply concat unbalanced-groups) (apply concat plans-of-action))
                                    ;; ^^ concat the list of lists, resolve any pointers we need to resolve
                                    unbalanced-groups)
        ;; ^^ For each node, a list of all "neighborhoods" that need to be balanced, with all necessary pointers resolved.
        balanced-groups (map #(reverse (map (comp reverse balance-single-seq) % %2))
                                    resolved-groups
                                    plans-of-action)
        ;; ^^ For each node, a list of all freshly-balanced neighborhoods, with everything (for that node) ordered high->low.
        ]
    (doseq [group unbalanced-groups]
      (metrics/update balance-batched-histogram (count group)))
    (map (fn [node ks group]
           (as-> (transaction-id node state/*transaction-id*) new-node
             (reduce node-dissoc new-node ks) ;; remove the original keys
             (reduce add-child-buffer-unaware new-node (apply concat group))
             (reset-messages new-node)))
         nodes
         original-keys
         balanced-groups)))
