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

(ns eva.v2.datastructures.bbtree.logic.v0.types
  (:require [eva.v2.datastructures.bbtree.logic.v0.operations :as operations]
            [eva.v2.datastructures.bbtree.logic.v0.message :as message]
            [eva.v2.datastructures.bbtree.logic.v0.protocols :as protocols]
            [eva.v2.datastructures.bbtree.logic.v0.query :as query]
            [eva.v2.datastructures.bbtree.logic.v0.error :refer [raise-safety raise-persist]]
            [eva.v2.datastructures.bbtree.storage :refer [node-pointer? get-nodes uuid]]
            [eva.datastructures.utils.fressian :as fresh]
            [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.utils.comparators :as comparison]
            [recide.sanex :as sanex]
            [eva.datastructures.protocols :as dsp]
            [eva.error :refer [raise insist]]
            [ichnaie.core :refer [tracing]]
            [potemkin :as p])
  (:import [java.util.concurrent.atomic AtomicReference]
           [eva.v2.datastructures.bbtree.logic.v0.nodes BufferedBTreeNode]
           [eva.datastructures.utils.interval Interval]
           [clojure.lang RT]))

(defn- persist-structure
  [store root persisted-uuid persistable?]
  (cond (not persistable?)
        (raise-persist :local-only
                       "it is a local-only copy."
                       {::sanex/sanitary? true})
        (or (node-pointer? root)
            (and (= (:node-id root) (:node-id persisted-uuid))
                 (= (protocols/transaction-id root) (protocols/transaction-id persisted-uuid))))
        root
        :else
        (operations/persist-tree store persisted-uuid root)))

(defn- insist-same-thread
  [^AtomicReference thread]
  (let [owner (.get thread)]
    (cond (identical? owner (Thread/currentThread)) true
          (nil? owner) (throw (IllegalAccessError. "Transient cannot be used after persistent! call"))
          :else (throw (IllegalAccessError. "Transient cannot be used by non-owner thread")))))

(declare ->BBTreeTransientSortedMap)
(p/def-map-type BBTreeSortedMap
  [root order buffer-size _meta]
  (meta [this] _meta)
  (with-meta [_ mta] (BBTreeSortedMap. root order buffer-size mta))
  (get [_ k default-value] (if-let [res (query/tree-get root k)] res default-value))
  (assoc [this k v]
         (BBTreeSortedMap. (operations/insert root [(message/upsert-message k v)]) order buffer-size _meta))
  (dissoc [_ k] (BBTreeSortedMap. (operations/insert root [(message/delete-message k)]) order buffer-size _meta))
  (keys [_] (query/tree-keys root))

  clojure.lang.IPersistentCollection
  (empty [this] (BBTreeSortedMap. (protocols/node-empty root) order buffer-size _meta))

  clojure.lang.Seqable
  (seq [this] (seq (query/subrange root [comparison/LOWER comparison/UPPER])))
  clojure.lang.Sorted
  (seq [this ascending?]
    (if (pos? (protocols/node-size root))
      (cond
        ascending? (seq this)
        :else (rseq this))))
  (seqFrom [this k ascending?]
           (seq
               (if ascending?
                 (query/subrange root [k comparison/UPPER])
                 (query/subrange root [comparison/LOWER k]))))
  (entryKey [this entry] (key entry))
  (comparator [this] (protocols/node-comparator root))
  clojure.lang.Reversible
  (rseq [this] (reverse (seq this)))

  clojure.lang.IEditableCollection
  (asTransient [this]
    (->BBTreeTransientSortedMap (AtomicReference. (Thread/currentThread))
                                (transient (vector))
                                order
                                buffer-size
                                root))

  dsp/BTreeBacked
  (root-node [_] root)

  dsp/BTreeOps
  (filter [this f] (BBTreeSortedMap. (operations/insert root [(message/filter-message f)]) order buffer-size _meta))
  (remove-interval [this rng]
                   (insist (instance? Interval rng))
                   (BBTreeSortedMap. (operations/insert root [(message/remove-interval-message rng)]) order buffer-size _meta))
  (keep-interval [this rng]
                 (insist (instance? Interval rng))
                 (BBTreeSortedMap.
                  (operations/insert root
                                     [(message/remove-interval-message (interval/open-interval comparison/LOWER (:low_ rng)))
                                      (message/remove-interval-message (interval/open-interval (:high_ rng) comparison/UPPER))])
                  order buffer-size _meta)))

(deftype BBTreeTransientSortedMap
    [^AtomicReference thread
     ^:unsynchronized-mutable ^clojure.lang.ITransientVector messages
     ^int order
     ^int buffer-size
     ^BufferedBTreeNode root]

  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k not-found] (if-let [res (query/tree-get root k)] res not-found))

  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k not-found] (.valAt this k not-found))
  (applyTo [this args]
    (let [n (RT/boundedLength args 2)]
      (case n
        0 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (.invoke this (first args) (second args))
        3 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName)))))))

  clojure.lang.ITransientCollection
  (conj [this entry]
    (insist-same-thread thread)
    (if (vector? entry)
      (assoc! this (nth entry 0) (nth entry 1))
      (reduce conj! this entry)))

  (persistent [this]
    (insist-same-thread thread)
    (.set thread nil)
    (tracing "eva.v2.datastructures.bbtree.logic.v0/persistent!"
      (let [messages (persistent! messages)
            unchanged? (nil? (seq messages))
            new-root (if unchanged?
                       root
                       (operations/insert root messages))]
        (BBTreeSortedMap. new-root order buffer-size {}))))

  clojure.lang.ITransientAssociative
  (assoc [this k v]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/upsert-message k v)))
    this)

  clojure.lang.ITransientMap
  (without [this k]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/delete-message k)))
    this)

  dsp/BTreeBacked
  (root-node [_] root)

  dsp/BTreeTransientOps
  (filter! [this f]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/filter-message f)))
    this)
  (remove-interval! [this rng]
    (insist (instance? Interval rng))
    (insist-same-thread thread)
    (set! messages (conj! messages (message/remove-interval-message rng)))
    this)
  (keep-interval! [this rng]
    (insist (instance? Interval rng))
    (insist-same-thread thread)
    (set! messages
          (reduce #(conj! % %2) messages
                  [(message/remove-interval-message (interval/open-interval comparison/LOWER (:low_ rng)))
                   (message/remove-interval-message (interval/open-interval (:high_ rng) comparison/UPPER))]))
    this))

(declare ->BackedBBTreeTransientSortedMap)
(p/def-map-type BackedBBTreeSortedMap
  [root persisted-uuid order buffer-size store persistable? persisted? _meta]
  (meta [this] _meta)
  (with-meta [_ mta] (BackedBBTreeSortedMap. root persisted-uuid order buffer-size store persistable? persisted? mta))
  (get [_ k default-value] (if-let [res (query/tree-get store root k)] res default-value))
  (assoc [this k v]
         (let [new-root (operations/insert store root [(message/upsert-message k v)])]
           (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (dissoc [_ k]
          (let [new-root (operations/insert store root [(message/delete-message k)])]
            (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (keys [_] (query/tree-keys store root))
  dsp/BackedStructure
  (storage-id [_] (if persisted?
                    persisted-uuid
                    (raise-safety :unpersisted-changes
                                  "No storage-id exists for this instance. Try persisting accumulated changes."
                                  {::sanex/sanitary? true})))
  (store [_] store)
  (persist! [this]
            (let [new-root (persist-structure store root persisted-uuid persistable?)]
              (BackedBBTreeSortedMap. new-root (uuid new-root) order buffer-size store true true _meta)))
  dsp/BTreeBacked
  (root-node [_] root)
  dsp/BTreeOps
  (filter [this f]
          (insist (satisfies? fresh/Autological f))
          (let [new-root (operations/insert store root [(message/filter-message f)])]
            (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (remove-interval [this rng]
                   (insist (instance? Interval rng))
                   (let [new-root (operations/insert store root [(message/remove-interval-message rng)])]
                     (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (keep-interval [this rng]
                 (insist (instance? Interval rng))
                 (let [new-root (operations/insert store
                                                    root
                                                    [(message/remove-interval-message (interval/open-interval comparison/LOWER (:low_ rng)))
                                                     (message/remove-interval-message (interval/open-interval (:high_ rng) comparison/UPPER))])]
                   (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  clojure.lang.IPersistentCollection
  (empty [this]
         (let [root (if (node-pointer? root)
                        (first (get-nodes @store [root]))
                        root)
               new-root (assoc-in (protocols/node-empty root)
                                  [:properties :semantics]
                                  (:semantics (protocols/properties root)))]
           (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  clojure.lang.Seqable
  (seq [this] (seq (query/subrange store root [comparison/LOWER comparison/UPPER])))
  clojure.lang.Sorted
  (seq [this ascending?]
    (if (pos? (protocols/node-size root))
      (cond
        ascending? (seq this)
        :else (rseq this))))
  (seqFrom [this k ascending?]
           (seq
               (if ascending?
                 (query/subrange store root [k comparison/UPPER])
                 (query/subrange store root [comparison/LOWER k]))))
  (entryKey [this entry] (key entry))
  (comparator [this] (protocols/node-comparator root))
  clojure.lang.Reversible
  (rseq [this] (reverse (seq this)))
  clojure.lang.IEditableCollection
  (asTransient [this]
               (->BackedBBTreeTransientSortedMap (AtomicReference. (Thread/currentThread))
                                                 order
                                                 buffer-size
                                                 store
                                                 (transient (vector))
                                                 root
                                                 persisted-uuid
                                                 persistable?
                                                 persisted?)))

(deftype BackedBBTreeTransientSortedMap
    [^AtomicReference thread
     ^int order
     ^int buffer-size
     store
     ^:unsynchronized-mutable ^clojure.lang.ITransientVector messages
     ^BufferedBTreeNode root
     persisted-uuid
     persistable?
     persisted?]
  dsp/EditableBackedStructure
  (make-editable! [this] this)
  dsp/BTreeBacked
  (root-node [_] root)
  dsp/BackedStructure
  (storage-id [_] (if persisted?
                    persisted-uuid
                    (raise-safety :unpersisted-changes
                                  "No storage-id exists for this map. Make it persistent! then call persist! first."
                                  {::sanex/sanitary? true})))
  (store [_] store)
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k not-found] (if-let [res (query/tree-get store root k)] res not-found))
  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k not-found] (.valAt this k not-found))
  (applyTo [this args]
    (let [n (RT/boundedLength args 2)]
      (case n
        0 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (.invoke this (first args) (second args))
        3 (throw (clojure.lang.ArityException.
                  n (.. this (getClass) (getSimpleName)))))))
  clojure.lang.ITransientCollection
  (conj [this entry]
    (insist-same-thread thread)
    (if (vector? entry)
      (assoc! this (nth entry 0) (nth entry 1))
      (reduce conj! this entry)))
  (persistent [this]
    (insist-same-thread thread)
    (.set thread nil)
    (tracing "eva.v2.datastructures.bbtree.logic.v0/persistent!"
      (let [messages (persistent! messages)
            unchanged? (nil? (seq messages))
            new-root (if unchanged?
                       root
                       (operations/insert store root messages))]
        (BackedBBTreeSortedMap. new-root persisted-uuid order buffer-size store persistable? (and persisted? unchanged?) {}))))
  clojure.lang.ITransientAssociative
  (assoc [this k v]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/upsert-message k v)))
    this)
  clojure.lang.ITransientMap
  (without [this k]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/delete-message k)))
    this)
  dsp/BTreeTransientOps
  (filter! [this f]
    (insist-same-thread thread)
    (insist (satisfies? fresh/Autological f))
    (set! messages (conj! messages (message/filter-message f)))
    this)
  (remove-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages (conj! messages (message/remove-interval-message rng)))
    this)
  (keep-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages
          (reduce conj! messages
                  [(message/remove-interval-message (interval/open-interval comparison/LOWER (:low_ rng)))
                   (message/remove-interval-message (interval/open-interval (:high_ rng) comparison/UPPER))]))
    this))

(declare ->BBTreeTransientSortedSet)
(deftype BBTreeSortedSet
    [root order buffer-size _meta]
  clojure.lang.IMeta
  (meta [this] _meta)
  clojure.lang.IObj
  (withMeta [this meta] (BBTreeSortedSet. root order buffer-size meta))
  clojure.lang.IPersistentCollection
  (cons [this x] (BBTreeSortedSet. (operations/insert root [(message/upsert-message x x)]) order buffer-size _meta))
  (empty [this] (BBTreeSortedSet. (protocols/node-empty root) order buffer-size _meta))
  (equiv [this that]
    (if (instance? BBTreeSortedSet that)
      (and (= root (.-root ^BBTreeSortedSet that))
           (= order (.-order ^BBTreeSortedSet that))
           (= buffer-size (.-buffer_size ^BBTreeSortedSet that)))
      false))
  clojure.lang.Seqable
  (seq [this] (seq (query/subrange root [comparison/LOWER comparison/UPPER])))
  clojure.lang.IPersistentSet
  (disjoin [this v] (BBTreeSortedSet. (operations/insert root [(message/delete-message v)]) order buffer-size _meta))
  (contains [this k] (not (nil? (query/tree-get root k))))
  (get [this k] (query/tree-get root k))
  clojure.lang.IFn
  (invoke [this k] (query/tree-get root k))
  (applyTo [this args]
    (let [n (RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))
  clojure.lang.Sorted
  (seq [this ascending?]
    (if (pos? (protocols/node-size root))
      (cond
        ascending? (seq this)
        :else (rseq this))))
  (seqFrom [this k ascending?]
    (seq
        (if ascending?
          (query/subrange root [k comparison/UPPER])
          (query/subrange root [comparison/LOWER k]))))
  (entryKey [this entry] entry)
  (comparator [this] (protocols/node-comparator root))
  clojure.lang.Reversible
  (rseq [this] (reverse (seq this)))
  clojure.lang.IEditableCollection
  (asTransient [this]
    (->BBTreeTransientSortedSet (AtomicReference. (Thread/currentThread))
                                (transient (vector))
                                order
                                buffer-size
                                root))
  dsp/BTreeBacked
  (root-node [_] root)
  dsp/BTreeOps
  (filter [this f]
    (BBTreeSortedSet. (operations/insert root [(message/filter-message f)]) order buffer-size _meta))
  (remove-interval [this rng]
    (insist (instance? Interval rng))
    (BBTreeSortedSet. (operations/insert root [(message/remove-interval-message rng)]) order buffer-size _meta))
  (keep-interval [this rng]
    (insist (instance? Interval rng))
    (BBTreeSortedSet. (operations/insert root [(message/remove-interval-message (interval/interval comparison/LOWER (:low_ rng) false (not (:low-open_ rng))))
                                               (message/remove-interval-message (interval/interval (:high_ rng) comparison/UPPER (not (:high-open_ rng)) false))]) order buffer-size _meta)))

(deftype BBTreeTransientSortedSet
    [^AtomicReference thread
     ^:unsynchronized-mutable ^clojure.lang.ITransientVector messages
     ^int order
     ^int buffer-size
     ^BufferedBTreeNode root]
  clojure.lang.IFn
  (invoke [this k] (query/tree-get root k))
  (applyTo [this args]
    (let [n (RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))
  clojure.lang.ITransientCollection
  (conj [this entry]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/upsert-message entry entry)))
    this)
  (persistent [this]
    (insist-same-thread thread)
    (.set thread nil)
    (tracing "eva.v2.datastructures.bbtree.logic.v0/persistent!"
      (let [messages (persistent! messages)
            unchanged? (nil? (seq messages))
            new-root (if unchanged?
                       root
                       (operations/insert root messages))]
        (BBTreeSortedSet. new-root order buffer-size {}))))
  clojure.lang.ITransientSet
  (disjoin [this entry]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/delete-message entry)))
    this)
  (contains [this entry] (or (contains? messages entry)
                         (= entry (query/tree-get root entry))))
  (get [this k] (query/tree-get root k))
  dsp/BTreeBacked
  (root-node [_] root)
  dsp/BTreeTransientOps
  (filter! [this f]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/filter-message f)))
    this)
  (remove-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages (conj! messages (message/remove-interval-message rng)))
    this)
  (keep-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages
          (reduce conj! messages
                  [(message/remove-interval-message (interval/interval comparison/LOWER (:low_ rng) false (not (:low-open_ rng))))
                   (message/remove-interval-message (interval/interval (:high_ rng) comparison/UPPER (not (:high-open_ rng)) false))]))
    this))

(declare ->BackedBBTreeTransientSortedSet)
(deftype BackedBBTreeSortedSet
    [root persisted-uuid order buffer-size store persistable? persisted? _meta]
  dsp/BackedStructure
  (storage-id [_] (if persisted?
                    persisted-uuid
                    (raise-safety :unpersisted-changes
                                  "No storage-id exists for this instance. Try persisting accumulated changes."
                                  {::sanex/sanitary? true})))
  (store [_] store)
  (persist! [this]
    (let [new-root (persist-structure store root persisted-uuid persistable?)]
      (BackedBBTreeSortedSet. new-root (uuid new-root) order buffer-size store true true _meta)))
  clojure.lang.IMeta
  (meta [this] _meta)
  clojure.lang.IObj
  (withMeta [this meta] (BackedBBTreeSortedSet. root persisted-uuid order buffer-size store persistable? persisted? meta))
  clojure.lang.IPersistentCollection
  (cons [this x]
    (let [new-root (operations/insert store root [(message/upsert-message x x)])]
      (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (empty [this]
    (let [root (if (node-pointer? root)
                 (first (get-nodes @store [root]))
                 root)
          empty-root (assoc-in (protocols/node-empty root)
                               [:properties :semantics]
                               (:semantics (protocols/properties root)))]
      (BackedBBTreeSortedSet. empty-root persisted-uuid order buffer-size store persistable? false _meta)))
  (equiv [this that]
    (if (instance? BackedBBTreeSortedSet that)
      (and (= root (.-root ^BackedBBTreeSortedSet that))
           (= order (.-order ^BackedBBTreeSortedSet that))
           (= buffer-size (.-buffer_size ^BackedBBTreeSortedSet that))
           (= store (.-store ^BackedBBTreeSortedSet that)))
      false))
  clojure.lang.Seqable
  (seq [this] (seq (query/subrange store root [comparison/LOWER comparison/UPPER])))
  clojure.lang.IPersistentSet
  (disjoin [this v]
    (let [new-root (operations/insert store root [(message/delete-message v)])]
      (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (contains [this k] (not (nil? (query/tree-get store root k))))
  (get [this k] (query/tree-get store root k))
  clojure.lang.IFn
  (invoke [this k] (query/tree-get store root k))
  (applyTo [this args]
    (let [n (RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))
  clojure.lang.Sorted
  (seq [this ascending?]
    (if (pos? (protocols/node-size root))
      (cond
        ascending? (seq this)
        :else (rseq this))))
  (seqFrom [this k ascending?]
    (seq
        (if ascending?
          (query/subrange store root [k comparison/UPPER])
          (query/subrange store root [comparison/LOWER k]))))
  (entryKey [this entry] entry)
  (comparator [this] (protocols/node-comparator root))
  clojure.lang.Reversible
  (rseq [this] (reverse (seq this)))
  dsp/BTreeBacked
  (root-node [_] root)
  (in-mem-nodes [_]
      (tree-seq (fn [node]
                  (and (not (node-pointer? node))
                       (not (protocols/leaf-node? node))))
                (comp vals protocols/children) root))
  dsp/BTreeOps
  (filter [this f]
    (insist (satisfies? fresh/Autological f))
    (let [new-root (operations/insert store root [(message/filter-message f)])]
      (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (remove-interval [this rng]
    (insist (instance? Interval rng))
    (let [new-root (operations/insert store root [(message/remove-interval-message rng)])]
      (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  (keep-interval [this rng]
    (insist (instance? Interval rng))
    (let [new-root (operations/insert store root [(message/remove-interval-message (interval/interval comparison/LOWER (:low_ rng) false (not (:low-open_ rng))))
                                                  (message/remove-interval-message (interval/interval (:high_ rng) comparison/UPPER (not (:high-open_ rng)) false))])]
      (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? false _meta)))
  clojure.lang.IEditableCollection
  (asTransient [this]
    (->BackedBBTreeTransientSortedSet (AtomicReference. (Thread/currentThread))
                                      (transient (vector))
                                      order
                                      buffer-size
                                      store
                                      root
                                      persisted-uuid
                                      persistable?
                                      persisted?)))

(deftype BackedBBTreeTransientSortedSet
    [^AtomicReference thread
     ^:unsynchronized-mutable ^clojure.lang.ITransientVector messages
     ^int order
     ^int buffer-size
     store
     ^BufferedBTreeNode root
     persisted-uuid
     persistable?
     persisted?]
  dsp/EditableBackedStructure
  (make-editable! [this] this)
  dsp/BackedStructure
  (storage-id [_] (if persisted?
                    persisted-uuid
                    (raise-safety :unpersisted-changes
                                  "No storage-id exists for this set. Make persistent! then call persist! first."
                                  {::sanex/sanitary? true})))
  (store [_] store)
  clojure.lang.IFn
  (invoke [this k] (query/tree-get store root k))
  (applyTo [this args]
    (let [n (RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        2 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))
  clojure.lang.ITransientCollection
  (conj [this entry]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/upsert-message entry entry)))
    this)
  (persistent [this]
    (insist-same-thread thread)
    (.set thread nil)
    (tracing "eva.v2.datastructures.bbtree.logic.v0/persistent!"
      (let [messages (persistent! messages)
            unchanged? (nil? (seq messages))
            new-root (if unchanged?
                       root
                       (operations/insert store root messages))]
        (BackedBBTreeSortedSet. new-root persisted-uuid order buffer-size store persistable? (and persisted? unchanged?) {}))))
  clojure.lang.ITransientSet
  (disjoin [this entry]
    (insist-same-thread thread)
    (set! messages (conj! messages (message/delete-message entry)))
    this)
  (contains [this entry] (or (contains? messages entry)
                         (= entry (query/tree-get root entry))))
  (get [this k] (query/tree-get root k))
  dsp/BTreeBacked
  (root-node [_] root)
  dsp/BTreeTransientOps
  (filter! [this f]
    (insist-same-thread thread)
    (insist (satisfies? fresh/Autological f))
    (set! messages (conj! messages (message/filter-message f)))
    this)
  (remove-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages (conj! messages (message/remove-interval-message rng)))
    this)
  (keep-interval! [this rng]
    (insist-same-thread thread)
    (insist (instance? Interval rng))
    (set! messages
          (reduce conj! messages
                  [(message/remove-interval-message (interval/interval comparison/LOWER
                                                                       (:low_ rng)
                                                                       false
                                                                       (not (:low-open_ rng))))
                   (message/remove-interval-message (interval/interval (:high_ rng)
                                                                       comparison/UPPER
                                                                       (not (:high-open_ rng))
                                                                       false))]))
    this))
