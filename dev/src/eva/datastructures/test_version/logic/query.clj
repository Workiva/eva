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

(ns eva.datastructures.test-version.logic.query
  (:require [eva.datastructures.test-version.logic.protocols :as protocols :refer :all]
            [eva.datastructures.test-version.logic.state :as state]
            [eva.datastructures.test-version.logic.message :as message]
            [eva.datastructures.test-version.logic.operations :as operations]
            [eva.datastructures.test-version.logic.nodes :as nodes]
            [eva.v2.datastructures.bbtree.storage :refer [node-pointer?]]
            [eva.datastructures.utils.comparators :as comparison]
            [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.protocols :refer [low high]]
            [utiliva.control :refer [?->]]
            [utiliva.core :refer [partition-map distinct-by]]
            [eva.error :refer [insist]]
            [clojure.data.avl :as avl]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:import [eva.datastructures.test_version.logic.nodes BufferedBTreeNode BufferedBTreePointer]))

(defrecord CustomSelector [label internal leaf]
  protocols/ICustomSelector
  (label [this] label)
  (apply-internal [this avl-children] (internal avl-children))
  (apply-leaf [this avl-children] (leaf avl-children)))

(defn- ensure-node
  "Pointer->node resolution. Takes into account the fact that resolving a pointer may
  yield another pointer (but this does not chain)."
  [node]
  (cond (instance? BufferedBTreePointer node) (nodes/pointer->node)
        (instance? BufferedBTreeNode node) node
        :else (throw (IllegalArgumentException. "ensure-node called on neither node nor pointer."))))

(defn- get-next-level
  [node->selectors-and-messages]
  (let [cmp (node-comparator (ffirst node->selectors-and-messages))]
    (->> (for [[node {:keys [selectors messages]}] node->selectors-and-messages
               :let [child->msgs (nodes/make-child->msgs node messages)
                     kids (children node)]]
           (let [child-map (persistent!
                            (reduce
                             (fn [child-map selector]
                               (let [selection (apply-internal selector kids)]
                                 (if (empty? selection)
                                   child-map
                                   (reduce
                                    (fn [child-map [k child]]
                                      (-> child-map
                                          (assoc! child
                                                  (if-some [entry (get child-map child)]
                                                    (update entry :selectors conj selector)
                                                    {:selectors [selector],
                                                     :messages (concat (get (protocols/messages node) k)
                                                                       (get child->msgs child))}))))
                                    child-map
                                    selection))))
                             (transient {})
                             selectors))]
             (keep (partial find child-map) (vals kids))))
         (into [] cat))))

(defn collect-results
  [node->selectors-and-messages]
  (persistent!
   (reduce (fn [selector->vals [node {:keys [selectors messages]}]]
             (let [node (nodes/add-messages node messages)]
               (reduce (fn [selector->vals selector]
                         (assoc! selector->vals
                                 (label selector)
                                 ((fnil into [])
                                  (get selector->vals (label selector))
                                  (->> node children (apply-leaf selector)))))
                       selector->vals
                       selectors)))
           (transient {})
           node->selectors-and-messages)))

(defn- custom-queries*
  [node->selectors-and-messages]
  (if (leaf-node? (ffirst node->selectors-and-messages))
    (collect-results node->selectors-and-messages)
    (let [children->selectors-and-messages (get-next-level node->selectors-and-messages)]
      (when (not-empty children->selectors-and-messages)
        (recur (map vector
                    (partition-map node-pointer?
                                   {true nodes/pointers->nodes}
                                   (keys children->selectors-and-messages))
                    (vals children->selectors-and-messages)))))))

(defn custom-queries
  "The selectors argument is expected to be a sequence of CustomSelectors."
  ([root selectors]
   (insist (instance? BufferedBTreeNode root) "custom-queries first argument must be a BufferedBTreeNode.")
   (let [selectors (distinct-by label selectors)]
     (merge-with (comp (if (= (:semantics (protocols/properties root)) :set) vals seq) #(do %2))
                 (zipmap (map label selectors) (repeat nil))
                 (custom-queries* [[root {:selectors selectors :messages ()}]]))))
  ([store root selectors]
   (insist (instance? BufferedBTreeNode root) "custom-queries first argument must be a BufferedBTreeNode.")
   (binding [state/*store* store]
     (custom-queries root selectors))))

(defn custom-query
  "The selector argument is expected to be a CustomSelector."
  ([root selector] (get (custom-queries root [selector]) (label selector)))
  ([store root selector] (get (custom-queries store root [selector]) (label selector))))

;; ========== BUILDING THE RANGE QUERIES ==========

(defn avl-between-kids
  "Accepts an avl map of cap keys to child nodes. Returns
  the submap containing all nodes which could possibly contain
  values within the provided range. Don't think too hard."
  [m [low high]]
  (avl/subrange m > low <= (key (avl/nearest m
                                             (if (= high comparison/UPPER) >= >)
                                             high))))

(defn create-selector-from-range
  [[low high :as range]]
  (->CustomSelector range
                    #(seq (avl-between-kids % range))
                    #(avl/subrange % >= low <= high)))

(d/defn ^{::d/aspects [traced]} subranges
  "Takes (optionally) a store, followed by a btree root,
  a list of ranges, and a list of messages to be applied.
  Returns a map of range->kv-pairs for matches."
  ([root ranges]
   (custom-queries root (map create-selector-from-range ranges)))
  ([store root ranges]
   (custom-queries store root (map create-selector-from-range ranges))))

(defn subrange
  "Takes (optionally) a store, followed by a btree root, a range, and insert/delete messages.
  Returns a sequence of the values stored in the tree."
  ([root range] (custom-query root (create-selector-from-range range)))
  ([store root range] (custom-query store root (create-selector-from-range range))))

(defn tree-get*
  ([node id msgs]
   (if (node-pointer? node)
     (recur (nodes/pointer->node node) id msgs)
     (if (leaf-node? node)
       (get (children (nodes/apply-messages node msgs)) id)
       (let [next-node-k (node-key-for node id)]
         (recur (get (children node) next-node-k)
                id
                (into (filterv #(= id (recip %)) (get (messages node) next-node-k)) msgs)))))))

(d/defn ^{::d/aspects [traced]} tree-get
  ([node id]
   (tree-get* node id []))
  ([store node id]
   (insist (instance? BufferedBTreeNode node) "argument 'node' passed to tree-get must be a BufferedBTreeNode.")
   (binding [state/*store* store]
     (tree-get* node id []))))

(defn between
  ([node start end]
   (subrange node [start end]))
  ([store node start end]
   (subrange store node [start end])))

(defn tree-keys
  ([node]
   (keys (subranges node [[comparison/LOWER comparison/UPPER]])))
  ([store node]
   (keys (subranges store node [[comparison/LOWER comparison/UPPER]]))))

(defn tree-filter*
  ([node f]
   (operations/insert node [(message/filter-message f)])))
