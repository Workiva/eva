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

(ns eva.v2.datastructures.bbtree.logic.v0.operations
  (:require [eva.v2.datastructures.bbtree.logic.v0.nodes :as nodes :refer :all]
            [eva.v2.datastructures.bbtree.logic.v0.protocols :refer :all]
            [eva.v2.datastructures.bbtree.storage :refer [uuid node-pointer? node?]]
            [eva.v2.datastructures.bbtree.logic.v0.state :as state]
            [eva.datastructures.utils.comparators :as comparators]
            [eva.v2.datastructures.bbtree.logic.v0.buffer :as buffer]
            [eva.v2.datastructures.bbtree.logic.v0.message :as message]
            [eva.datastructures.utils.core :refer [fast-last]]
            [eva.v2.datastructures.bbtree.logic.v0.balance :as balance]
            [eva.v2.datastructures.bbtree.logic.v0.storage :as storage]
            [utiliva.alpha :refer [mreduce]]
            [utiliva.core :refer [zip-from group-like partition-map piecewise-map]]
            [barometer.core :as metrics]
            [eva.error :refer [insist]]
            [plumbing.core :as pc]
            [clojure.data.avl :as avl]
            [clojure.math.numeric-tower :refer [ceil floor]]
            [com.rpl.specter :as sp]
            [com.rpl.specter.macros :as sm]
            [recide.sanex.logging :refer [debug]]))

(set! *warn-on-reflection* true)
(declare deliver-messages-batch)

;; ****** SPLITTING ******

(defn num-buckets [item-count order] (ceil (/ item-count order)))

(defn bucket
  "Like partition, but =n= specifies the number of buckets."
  [c n]
  (let [cnt (count c)
        size (floor (/ cnt n))
        [c1 c2] (split-at (* (rem cnt n) (inc size)) c)]
    (concat (partition (inc size) c1)
            (partition size c2))))

(defn split*
  "Splits a node. Uses `split-weight' to determine whether it favors maximally full
  or minimally full nodes in the result."
  [node]
  (let [buckets (bucket (children node) (num-buckets (node-size node) (node-order node)))
        new-nodes-constructor (fn [buckets]
                                (map #(reduce (fn [n [k v]]
                                                (node-assoc n k v))
                                              (% node)
                                              %2)
                                     (conj (repeat new-node-from) node-empty)
                                     (reverse buckets)))]
    (if (leaf-node? node)
      (new-nodes-constructor buckets)
      (let [msgs (for [b buckets] (pc/map-from-keys #(get (messages node) %) (keys b)))
            ;; If we're not in a leaf node, we want to make the last internal pointer to be comparators/UPPER
            [properly-keyed-buckets replaced] (sm/replace-in [sp/ALL sp/LAST sp/FIRST] (fn [x] [comparators/UPPER [x]]) buckets)
            ;; And we need to transfer messages from the old queues to the new queues, with these replacements in mind:
            keys-to-replace (set replaced)
            properly-keyed-msgs (sm/setval [sp/ALL sp/ALL sp/FIRST keys-to-replace] comparators/UPPER msgs)]
        ;; this is accomplished with transfer-messages:
        (map transfer-messages
             (new-nodes-constructor properly-keyed-buckets)
             (reverse properly-keyed-msgs))))))

(def split-nodes-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.operations:split-nodes.counter
                           (metrics/counter "Counts how many nodes get split.")))

(defn split
  "This method does *not* check whether the node is overflowing. It just splits it."
  [node]
  (metrics/increment split-nodes-counter)
  (if (root-node? node)
    (let [babies (split* node)
          ks (conj (butlast (map min-rec babies)) comparators/UPPER)
          new-root (reduce node-conj
                           (-> node
                               new-node-from
                               (mark-root true)
                               (mark-leaf false)
                               (assoc-in [:properties :node-counter] @state/*node-id-counter*)
                               (assoc-in [:properties :semantics] (:semantics (properties node)))) ;; TODO:
                           (zipmap ks babies))]
      (if (overflowed? new-root)
        (recur new-root)
        (list new-root)))
    ;; if it's not a root node:
    (split* node)))

;; ****** MESSAGES AND BUFFERS ******

(defn deliver-and-add-batch
  "Given a sequence of nodes with certain children, a sequence of corresponding sequences of
  said children, and finally a sequence of sequences of messages corresponding to those children,
  this delivers the messages to the children (recursively moving down the tree if necessary)
  and then adds these resulting sequences of children into the parent nodes."
  [nodes chldrn msgs]
  (let [new-children (deliver-messages-batch (sequence cat chldrn) chldrn (sequence cat msgs))]
    (map (fn [node child-list]
           (transduce cat (completing add-child) node child-list))
         nodes new-children)))

(defn keys-to-flush
  "Which keys need to be flushed?"
  [node]
  (sort (comp - (node-comparator node)) (buffer/keys-to-flush (messages node))))

(def overflowed-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.operations:overflowed.counter
                           (metrics/counter "The number of buffers that have overflowed.")))

(def flush-buffer-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.operations:flush-buffer-batched.counter
                           (metrics/counter "The number of times flush-buffer-batched has been called.")))

(defn flush-buffer-batched
  "Given a sequence of nodes, this flushes their buffers."
  [nodes]
  (metrics/increment overflowed-counter (count nodes))
  (metrics/increment flush-buffer-counter)
  (let [ks (map keys-to-flush nodes)
        msgs (map #(map (partial get (messages %)) %2) nodes ks)
        chldrn (map #(map (partial get (children %)) %2) nodes ks)]
    (as-> nodes nodes
      (map #(reduce node-dissoc % %2) nodes ks) ;; remove the old children
      (map #(reduce (fn [n k] (messages n dissoc k)) % %2) nodes ks) ;; remove the old messages
      (deliver-and-add-batch nodes chldrn msgs)))) ;; deliver the new messages and add the resulting children back in

(def too-small-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.operations:small-nodes.counter
                           (metrics/counter "The number of nodes that have been flagged as too small and needing balancing.")))

(defn needs-balancing?
  "Are any of the node's children too small?"
  [node]
  (when (and (inner-node? node) ;; a leaf node has no children to balance.
             (some #(> (min-node-size %) (node-size %))
                   (filter (complement node-pointer?) (vals (children node)))))
    (metrics/increment too-small-counter)
    true))

(defn deliver-messages-batch
  "This is the process that drives it all. The recursion point back into
  this function (at a lower level in the tree) occurs in the function
  apply-messages-batched, so by the time this gets to rebalancing, the
  lower levels of the tree have already been balanced."
  [nodes shaped-nodes msgs]
  (as-> nodes nodes
    (partition-map node-pointer? {true pointers->nodes} nodes)
    (piecewise-map leaf-node? {true apply-messages, false add-messages} nodes msgs)
    (partition-map buffer-overflowing? {true flush-buffer-batched} nodes)
    (partition-map needs-balancing? {true balance/balance-batched} nodes)
    (piecewise-map overflowed? {true split false list} nodes)
    (group-like nodes shaped-nodes)))

;; ****** WRAPPING THINGS UP NICELY ******

(defn- pointerize*
  "The operations were done in memory. Now we need to turn all nodes back into pointers
  from the bottom up."
  [node]
  (if (node-pointer? node)
    {:element node}
    (let [pointer (node->pointer node)
          current-uuid (uuid pointer)]
      (if (leaf-node? node)
        (let [node (uuid node current-uuid)]
          {:element pointer :store-pair [pointer node] :all-store-pairs {pointer node}})
        (let [results (map pointerize* (vals (children node)))
              store-pairs (into {} (map :store-pair) results)
              pointerized-node (-> node
                                   (children (fn [m results]
                                               (into m (comp (map :element) (zip-from (keys (children node)))) results))
                                             results)
                                   (uuid current-uuid))] ;; <== edits to the node clear the UUID.
          {:element pointer
           :store-pair [pointer pointerized-node]
           :all-store-pairs (assoc (into (hash-map) (mapcat :all-store-pairs) results)
                                   pointer
                                   pointerized-node)})))))

(defn persist-tree
  "Converts the entire tree back into pointer form, then persists the tree to the store.
  Returns the root node."
  [store old-root root]
  (insist (root-node? root))
  (insist (not (node-pointer? root)))
  (binding [state/*transaction-id* (transaction-id root)]
    (let [result (pointerize* root)
          [pointer root] (:store-pair result)]
      (storage/persist-tree store (:all-store-pairs result))
      root)))

(def insert-counter
  (metrics/get-or-register metrics/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.operations:insert.counter
                           (metrics/counter "The number of times that 'insert' has been called.")))

(defn insert ;; TODO: Cleanup
  "Entry point to the process of manipulating the btree. After some high-level bookkeeping,
  delegates to deliver-messages-batch."
  ([store node msgs]
   (binding [state/*store* store]
     (let [node (if (node-pointer? node) (first (pointers->nodes [node])) node)]
       (insert node msgs))))
  ([node msgs]
   (metrics/increment insert-counter)
   (binding [state/*transaction-id* (inc (transaction-id node))
             state/*node-id-counter* (atom (:node-counter (properties node)))]
     (let [msgs (map #(assoc % :tx-added state/*transaction-id*) msgs)
           [new-root] (ffirst (deliver-messages-batch [node] [[node]] [msgs]))]
       (-> (if (underflowed? new-root) ;; only a single child
             (let [[k c] (first (children new-root))
                   c (if (node-pointer? c) (first (pointers->nodes [c])) c)] ;; ^^^
               (insert (assoc-in (mark-root c true) [:properties :node-counter] @state/*node-id-counter*)
                       (get (messages new-root) k)))
             (assoc-in new-root [:properties :node-counter] @state/*node-id-counter*))
           (assoc-in [:properties :semantics] (:semantics (properties node))))))))
