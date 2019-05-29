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

(ns eva.datastructures.test-version.logic.nodes
  (:require [eva.datastructures.test-version.logic.state :as state]
            [eva.datastructures.test-version.logic.protocols :as protocols :refer :all]
            [eva.datastructures.protocols :as dsp :refer [interval-overlaps? restrict interval-contains? intersect]]
            [eva.datastructures.test-version.logic.buffer :as buffer]
            [eva.datastructures.test-version.logic.message :as message]
            [eva.v2.datastructures.bbtree.storage :refer [uuid get-nodes get-node put-nodes node-pointer? node?]]
            [eva.datastructures.versioning :refer [ensure-version]]
            [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.utils.comparators :as comparison :refer [UPPER LOWER]]
            [eva.datastructures.utils.core :refer [fast-last overlaps?]]
            [utiliva.control :refer [?-> ?->>]]
            [utiliva.core :refer [piecewise-map partition-map zip-from]]
            [utiliva.comparator :refer [min max]]
            [utiliva.alpha :refer [mreduce]]
            [eva.error :refer [insist]]
            [plumbing.core :refer [?>]]
            [clojure.data.avl :as avl]
            [clojure.core.memoize :as memo]
            [clojure.math.numeric-tower :refer [ceil floor]])
  (:import [clojure.data.avl AVLMap]
           [clojure.lang RT MapEntry]
           [eva.v2.datastructures.bbtree.storage NodeStorageInfo]
           [java.util UUID])
  (:refer-clojure :exclude [min max]))

;; Important Legal Note.
;;
;; In our implementation of the btree structure, we don't exactly use pivot keys.
;; In the standard implementation, if a node has n children, it has n-1 pivots,
;; interposed. But in our implementation, we use "cap" keys. With n children, we
;; have n caps. These are arranged such that all caps to the "left" of a given
;; cap C correspond to children whose subdictionaries contain only keys strictly
;; less than C.
;;
;; The important legal bit: Before changing the organization scheme for the internal
;; keys (e.g., to more closely match the canonical B-tree as described in literature,
;; such as using standard 'pivots'), please contact Dean Ritz.

(set! *warn-on-reflection* true)

(defn new-node-id
  "Nodes within a tree are identified uniquely by a combination of id and version.
  Whenever a brand-new node is added to the tree, a new node id is generated sequentially.
  That's what this method does."
  []
  (insist (some? state/*node-id-counter*) "Cannot create new node id with unbound *node-id-counter*")
  (swap! state/*node-id-counter* inc))

;; The following record is never used as anything other than a map. My understanding
;; is that the defined fields will have faster access times. That's all.
(defrecord NodeProperties
    [leaf? root? min-rec max-rec order buffer-size comparator]) ;; universal properties for nodes
  ;; other properties sometimes used:
  ;;  * ROOT ONLY: :semantics, :node-counter
  ;;  * POINTER ONLY: :node-size

(defn empty-properties
  "Selects from properties the keys #{:comparator :order :leaf? :buffer-size}
  then merges {:root? false} and returns the result."
  [props]
  (map->NodeProperties (assoc (select-keys props [:comparator :order :leaf? :buffer-size])
                              :root? false)))

;; each new node that is created is assigned a node-id, which persists through node modifications.
;; tx indicates during which sequential operation on the tree the node was last modified.
;; uuid (Maybe String) combines the above with a unique string for safe storage.
;; vvv  key-vals should be an AVLMap (currently).
(defrecord BufferedBTreeNode
    [uuid node-id tx buffer key-vals properties] ; properties: {:order o, :root? #t, :leaf? #f, :min-rec m, :etc etc}
  dsp/Versioned
  (get-version [_] VERSION)
  NodeStorageInfo
  (uuid [this] uuid)
  (uuid [this s] (assoc this :uuid s)) ;; TODO: safety checks
  (node? [_] true)
  (node-pointer? [_] false)
  protocols/IBufferedBTreeNode
  (node-id [this] node-id)
  (node-id [this x] (assoc this :node-id x)) ;; TODO: safety checks
  (new-node-from [this] (assoc (node-empty this) :node-id (new-node-id) :tx state/*transaction-id* :uuid nil))
  (node-empty [this] ;; TODO: documentation does NOT match implementation.
    (BufferedBTreeNode. nil node-id tx (empty buffer) (empty key-vals) (empty-properties properties)))
  (node-conj [this kv]
    (insist (not (nil? (val kv))))
    (node-assoc this (key kv) (val kv)))
  (node-assoc [this k v]
    (insist (not (nil? v)))
    (let [tmp (BufferedBTreeNode. nil node-id tx buffer (assoc key-vals k v) properties)]
      (-> tmp
          (min-rec (partial min (node-comparator this)) (if (leaf-node? tmp) k (min-rec v)))
          (max-rec (partial max (node-comparator this)) (if (leaf-node? tmp) k (max-rec v))))))
  (node-dissoc [this k] (BufferedBTreeNode. nil node-id tx buffer (dissoc key-vals k) properties))
  (node-get [this k] (get key-vals k))
  (buffer-dissoc [this k] (BufferedBTreeNode. nil node-id tx (dissoc buffer k) key-vals properties))
  (children [this] key-vals)
  (children [this m]  (BufferedBTreeNode. nil node-id tx buffer m properties))
  (children [this f v] (BufferedBTreeNode. nil node-id tx buffer (f key-vals v) properties))
  (messages [this] buffer)
  (messages [this messages] (BufferedBTreeNode. nil node-id tx messages key-vals properties))
  (messages [this f v] (BufferedBTreeNode. nil node-id tx (f buffer v) key-vals properties))
  (node-key-for [this k]
    (if (leaf-node? this)
      k
      (if-let [above-thing (avl/nearest key-vals > k)]
        (key above-thing)
        (key (fast-last key-vals)))))
  (leaf-node? [this] (get properties :leaf?))
  (mark-leaf [this b] (BufferedBTreeNode. nil node-id tx buffer key-vals (assoc properties :leaf? (boolean b))))
  (inner-node? [this] (not (leaf-node? this)))
  (root-node? [this] (get properties :root?))
  (mark-root [this b] (BufferedBTreeNode. nil node-id tx buffer key-vals (assoc properties :root? (boolean b))))
  (properties [this] properties)
  (node-comparator [this] (get properties :comparator))
  (node-order [this] (get properties :order))
  (buffer-size [this] (get properties :buffer-size))
  (node-size [this] (count key-vals));; constant-time: (= true (counted? key-vals))
  (max-rec [this] (get properties :max-rec))
  (max-rec [this v] (BufferedBTreeNode. nil node-id tx buffer key-vals (assoc properties :max-rec v)))
  (max-rec [this f v] (BufferedBTreeNode. nil node-id tx buffer key-vals (update properties :max-rec (fnil f LOWER) v)))
  (min-rec [this] (get properties :min-rec))
  (min-rec [this v] (BufferedBTreeNode. nil node-id tx buffer key-vals (assoc properties :min-rec v)))
  (min-rec [this f v] (BufferedBTreeNode. nil node-id tx buffer key-vals (update properties :min-rec (fnil f UPPER) v)))
  (transaction-id [this] tx)
  (transaction-id [this v] (BufferedBTreeNode. nil node-id v buffer key-vals properties)))

(defrecord BufferedBTreePointer
    [uuid node-id tx properties]
  dsp/Versioned
  (get-version [_] VERSION)
  NodeStorageInfo
  (uuid [this] uuid)
  (uuid [this k] (throw (IllegalArgumentException. "Cannot modify the uuid of a node pointer.")))
  (node? [_] false)
  (node-pointer? [_] true)
  protocols/IBufferedBTreeNode
  ;; unsupported:
  (node-empty [this] (throw (IllegalArgumentException. "Cannot empty a node pointer.")))
  (node-assoc [this k v] (throw (IllegalArgumentException. "Cannot assoc to a node pointer.")))
  (node-dissoc [this k] (throw (IllegalArgumentException. "Cannot dissoc from a node pointer.")))
  (node-get [this k] (throw (IllegalArgumentException. "Cannot query a node pointer.")))
  (buffer-dissoc [this k] (throw (IllegalArgumentException. "Cannot dissoc from the buffer of a node pointer.")))
  (children [this] (throw (IllegalArgumentException. "Cannot get the children of a node pointer.")))
  (children [this m] (throw (IllegalArgumentException. "Cannot set the children of a node pointer.")))
  (children [this f v] (throw (IllegalArgumentException. "Cannot update the children of a node pointer.")))
  (messages [this] (throw (IllegalArgumentException. "Cannot get the buffer of a node pointer.")))
  (messages [this messages] (throw (IllegalArgumentException. "Cannot set the buffer of a node pointer.")))
  (messages [this f v] (throw (IllegalArgumentException. "Cannot update the buffer of a node pointer.")))
  (node-key-for [this k] (throw (IllegalArgumentException. "Cannot examine keys for a node pointer.")))
  (mark-leaf [this b] (throw (IllegalArgumentException. "Cannot change properties of a node pointer.")))
  (mark-root [this b] (throw (IllegalArgumentException. "Cannot change properties of a node pointer.")))
  (node-order [this] (throw (IllegalArgumentException. "Cannot read node order from the pointer.")))
  (max-rec [this v] (throw (IllegalArgumentException. "Cannot set the max-recursive key of a node pointer.")))
  (max-rec [this f v] (throw (IllegalArgumentException. "Cannot update the max-recursive key of a node pointer.")))
  (node-id [this k] (throw (IllegalArgumentException. "Cannot modify the node-id of a node pointer.")))
  (transaction-id [this k] (throw (IllegalArgumentException. "Cannot modify the transaction-id of a node pointer.")))
  ;; supported:
  (node-id [this] node-id)
  (transaction-id [this] tx)
  (min-rec [this v] (BufferedBTreePointer. uuid node-id tx (assoc properties :min-rec v)))
  (min-rec [this f v] (BufferedBTreePointer. uuid node-id tx (update properties :min-rec (fnil f UPPER) v)))
  (properties [this] (get this :properties))
  (node-comparator [this] (get-in this [:properties :comparator]))
  (leaf-node? [this] (get-in this [:properties :leaf?]))
  (inner-node? [this] (not (leaf-node? this)))
  (root-node? [this] (get-in this [:properties :root?]))
  (node-size [this] (get-in this [:properties :node-size]))
  (max-rec [this] (get-in this [:properties :max-rec]))
  (min-rec [this] (get-in this [:properties :min-rec])))

(defn ensure-uuid
  "Expects, but does not enforce, that maybe-uuid is either
  nil or a string of (format \"%s-%s-%s\" id tx (UUID/randomUUID)).
  If maybe-uuid is nil, this returns a new string of that format."
  [maybe-uuid id tx]
  (or maybe-uuid
      (format "%s-%s-%s" id tx (UUID/randomUUID))))

(defn node->pointer
  [node]
  (let [property-keys (if (root-node? node)
                        [:leaf? :root? :max-rec :order :min-rec :node-counter :semantics :comparator]
                        [:leaf? :root? :max-rec :order :min-rec :node-counter])]
    (->BufferedBTreePointer (ensure-uuid (uuid node) (:node-id node) (transaction-id node))
                            (:node-id node)
                            (transaction-id node)
                            (assoc (select-keys (properties node)
                                                property-keys)
                                   :node-size
                                   (node-size node)))))

(defn nodes->pointers [nodes] (map node->pointer nodes))

(defn pointers->nodes
  [pointers]
  (insist (some? state/*store*) "pointers->nodes called when *store* is nil.")
  (->> (get-nodes state/*store* pointers)
       (map (partial ensure-version VERSION)))) ;; <== ensuring the version of the requested nodes.

(defn pointer->node
  [pointer]
  (insist (some? state/*store*) "pointer->node called when *store* is nil.")
  (first (pointers->nodes [pointer])))

(defn ensure-pointer ;; TODO: This is a hack and Timothy should be ashamed.
  ;; Edit: This has become even hackier, and Timothy should be utterly mortified.
  "Takes either a pointer or a node id. Returns either that pointer or a minimal implementation
  of Pointer and Node that will give its uuid."
  [pointer-thing]
  (cond (node-pointer? pointer-thing)
        pointer-thing
        (satisfies? protocols/IBufferedBTreeNode pointer-thing)
        (node->pointer pointer-thing)
        :else
        (do (insist (string? pointer-thing)) ;; TODO: uuid? predicate
            (reify NodeStorageInfo
              (uuid [_] pointer-thing)
              (node? [_] false)
              (node-pointer? [_] true)
              dsp/Versioned
              (get-version [_] VERSION)))))

(defn min-node-size "Minimum size the node can be before considered 'too small'." [node] (ceil (/ (node-order node) 2)))

(defn overflowed? [node]
  (if (leaf-node? node)
    (> (node-size node) (+ (node-order node) (buffer-size node))) ;; <-- fat leaves optimization happens here
    (> (node-size node) (node-order node))))
(defn buffer-overflowing? [node] (buffer/overflowing? (messages node)))
(defn node-valid-size? [node] (<= (min-node-size node) (node-size node) (node-order node)))

(defn underflowed?
  [node]
  (and (not (node-pointer? node)) ;; why are node-pointers not underflowed? I suppose because then their underflowediness would have been caught.
       (or (and (root-node? node) ;; if it's a root but not a leaf and it has a single child, that child should be made root.
                (not (leaf-node? node))
                (= (node-size node) 1))
           (and (not (root-node? node)) ;; If it's not a root, check the min-node-size!
                (< (node-size node) (min-node-size node))))))

(defn node-overlaps?
  "Does this node overlap with this range? Optionally accepts comparator."
  ([node range]
   (node-overlaps? (node-comparator node) node range))
  ([cmp node [y1 y2]]
   (overlaps? cmp [(min-rec node) (max-rec node)] [y1 y2])))

(defn node-minrec-above
  "Takes an internal node and a dictionary key. Returns the min-rec of the next node
  'above' that key. Useful for establishing cap keys or sorting messages."
  ([this k] (node-minrec-above this k true))
  ([this k buffer-aware?]
   (insist (inner-node? this))
   (if-let [above-thing (avl/nearest (children this) > k)]
     (if-not buffer-aware?
       (min-rec (val above-thing))
       (if-let [min-msg (seq (filter (complement ranged?) (get (messages this) (key above-thing))))]
         (min (node-comparator this)
              (apply min (node-comparator this)
                     (map recip min-msg)) (min-rec (val above-thing)))
         (min-rec (val above-thing))))
     UPPER)))

(defn node-minrec-above-buffer-unaware [this k] (node-minrec-above this k false))

(defn left-most-key? [node k] (= k (key (first (children node)))))
(defn right-most-key? [node k] (= k (key (last (children node)))))

(defn update-min-max
  [node]
  (if (leaf-node? node)
    (-> node
        (min-rec (key (first (children node))))
        (max-rec (key (fast-last (children node)))))
    ;; if internal node, must also check messages queues:
    (if (empty? (children node))
      node
      (let [min-rec-child (-> node children first val min-rec)
            max-rec-child (-> node children fast-last val max-rec)
            min-rec-msg (buffer/min-recip (partial min (node-comparator node)) (messages node)) ;; TODO: clunky!!! min/max
            max-rec-msg (buffer/max-recip (partial max (node-comparator node)) (messages node))]
        (-> node
            (min-rec (if min-rec-msg (min (node-comparator node) min-rec-child min-rec-msg) min-rec-child))
            (max-rec (if max-rec-msg (max (node-comparator node) max-rec-child max-rec-msg) max-rec-child)))))))

(defn apply-messages
  "Applies message operations. Valid ops and their implementations are defined in
  the message namespace."
  [node msgs]
  (insist (leaf-node? node))
  (as-> (transaction-id node state/*transaction-id*) node
    (children node
              (reduce #(apply-message %2 (node-comparator node) %)
                      (children node) msgs))
    (if (pos? (node-size node))
      (-> node
          (min-rec (key (first (children node))))
          (max-rec (key (last (children node)))))
      node)))

(defn min-rec-buffer-aware
  "Finds the min-rec of the child, taking into account messages currently sitting
  in the buffer to be delivered to that child."
  [node k child]
  (if-let [msgs (seq (remove ranged? (get (messages node) k)))]
    (let [min (partial min (node-comparator node))]
      (min (apply min (map recip msgs)) (min-rec child)))
    (min-rec child)))

(defn make-child->msgs
  [node messages]
  (let [kids (children node)]
    (persistent!
     (reduce (fn [child-map message]
               (if (ranged? message)
                 (let [selection (avl/subrange kids
                                               >= (node-key-for node (dsp/low (recip message)))
                                               <= (node-key-for node (dsp/high (recip message))))]
                   (reduce #(assoc! % %2 (conj (get % %2 []) message))
                           child-map
                           (vals selection)))
                 (let [k (node-key-for node (recip message))
                       child (get kids k)]
                   (assoc! child-map child (conj (get child-map child []) message)))))
             (transient {})
             messages))))

(defn- simply-add-ranged-message*
  [node msg]
  (let [slice (avl/subrange (children node)
                            >= (node-key-for node (dsp/low (recip msg)))
                            <= (node-key-for node (dsp/high (recip msg))))]
    (reduce (fn [node [key msg]]
              (messages node
                        (buffer/insert (messages node)
                                       key
                                       msg)))
            node
            (for [[k child] slice
                  :let [min-rec* (min-rec child)]
                  :when (interval-overlaps? (recip msg)
                                            (node-comparator node)
                                            min-rec*
                                            k)]
              [k (assoc msg
                        :target
                        (intersect (recip msg)
                                   (node-comparator node)
                                   (interval/interval min-rec* k false true)))]))))

(defn add-ranged-message
  "Adds a ranged message to all appropriate message buffers in the node. No updates
  are made to min- or max-rec."
  [node msg]
  (simply-add-ranged-message* node msg))

(defn add-simple-message
  "Add a single message to a node's message buffer. Updates min- and max-rec accordingly."
  [node msg]
  (let [k (node-key-for node (recip msg))
        node (-> node
                 (messages (buffer/insert ^buffer/BTreeBuffer (messages node)
                                          k
                                          msg))
                 (min-rec (partial min (node-comparator node)) (recip msg))
                 (max-rec (partial max (node-comparator node)) (recip msg)))]
    (if (= :upsert (op msg))
      (children node
                (fn [kvs target] ;; TODO: move next child under?
                  (update kvs k min-rec (partial min (node-comparator node)) target))
                (recip msg))
      node)))

(defn add-messages
  "Adds many messages to a node's message buffer. Updates min- and max-rec accordingly."
  [node msgs]
  (if (leaf-node? node)
    (apply-messages node msgs)
    (let [node (transaction-id node state/*transaction-id*)
          add-fn (fn [node msg]
                   (if (ranged? msg)
                     (add-ranged-message node msg)
                     (add-simple-message node msg)))]
      (reduce add-fn node msgs))))

(defn transfer-messages
  "Adds messages to the buffer of a node when you already know which keys the messages
  should correspond to. Does nothing to the node's min-rec or max-rec values."
  [node ks->msgs]
  (messages node
            (fn [buffer ks->msgs]
              (reduce (fn [buffer [k msgs]]
                        (reduce #(buffer/insert % k %2) buffer msgs))
                      buffer ks->msgs))
            ks->msgs))

(defn add-child
  "Adds a child to an internal node. Meant to be used when creating a node or when 'swapping' children.
  You should always add children in reverse sorted order.
  The flag 'buffer-aware?' indicates whether to take the message buffers into account when
  assigning local cap keys for the child."
  ([node child] (add-child node child true))
  ([node child buffer-aware?]
   (insist (inner-node? node))
   (let [k (node-minrec-above node (max-rec child) buffer-aware?)
         node (node-assoc node k child)]
     (?-> node
          (left-most-key? k) (min-rec (min-rec child))
          (right-most-key? k) (max-rec (max-rec child))))))

(defn add-child-buffer-unaware
  [node child]
  (add-child node child false))

(defn add-children
  "Takes a node and a map nodes->msgs to add. Ensures min- and max-rec are up-to-date."
  [node adopted->msgs]
  (let [kids (vals (children node))
        kids->msgs (sequence (comp (map #(get (messages node) %)) (zip-from kids)) (keys (children node)))
        nodes->msgs (sort-by (comp min-rec key) (node-comparator node) (concat adopted->msgs kids->msgs))
        ks (conj (vec (rest (map min-rec (keys nodes->msgs)))) UPPER)]
    (as-> (node-empty node) node
      (reduce node-conj node (zipmap ks (keys nodes->msgs)))
      (transfer-messages node (zipmap ks (vals nodes->msgs)))
      (update-min-max node))))

(defn remove-children
  "Takes an inner node and a simple sequence of keys. Removes children bound to
  those keys, along with any messages intended for those children. Updates
  min- and max-rec accordingly."
  [node ks]
  (insist (inner-node? node))
  (let [kids (vals (for [kv (children node)
                         :when (every? #(not= (key kv) %) ks)]
                     kv))
        new-ks (conj (vec (rest (map min-rec kids))) UPPER)
        msgs (buffer/get-all (reduce #(dissoc % %2) (messages node) ks))]
    (as-> (node-empty node) node
      (reduce node-conj node (zipmap new-ks kids))
      (add-messages node msgs)
      (update-min-max node))))

(defn remove-values
  "Takes a leaf node and a simple sequence of keys. Removes the values stored by those keys.
  Once it is finished, it updates the min-rec and max-rec properties of the node."
  [node ks]
  (insist (leaf-node? node))
  (update-min-max (reduce node-dissoc node ks)))

(defn reset-messages ;; TODO: This could be done much more efficiently.
  "Removes all of the messages from the node's buffer and re-adds them.
  To be used when the node's internal cap keys have been altered to keep
  the message buffers in sync."
  [node]
  (let [buffer (messages node)
        msgs (mapcat val buffer)]
    (add-messages (messages node (empty buffer)) msgs)))
