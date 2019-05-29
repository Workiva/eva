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

(ns eva.v2.datastructures.bbtree.api
  "The function of this namespace is to collect the public-facing 'permanent' methods of the
  btree code. As new versions are written, the constructors here should be updated to point
  to the latest constructor code."
  (:require [eva.datastructures.protocols :as dsp]
            [eva.v2.datastructures.bbtree.storage :as node-storage]
            [eva.v2.datastructures.bbtree.logic.v0.core :as v0]
            [eva.v2.datastructures.bbtree.logic.v0.query :as v0-query]
            [eva.v2.datastructures.bbtree.logic.v0.types :as v0-types]
            [eva.v2.datastructures.bbtree.logic.v0.tree :as v0-tree]
            [eva.v2.datastructures.bbtree.logic.v0.protocols :as v0-protocols]
            [eva.v2.datastructures.bbtree.logic.v0.storage :as v0-storage]
            [morphe.core :as d]
            [barometer.aspects :refer [timed]])
  (:refer-clojure :exclude [sorted-map sorted-set sorted-map-by sorted-set-by]))

;; ==================
;; == CONSTRUCTORS ==
;; ==================

(defn sorted-map
  "Returns a buffered-btree implementation of a sorted map, as a drop-in replacement for the built-in sorted map.
  Has a transient version. Is optimized for a latency-heavy backing (see backed-sorted-map), so the built-in
  sorted-map is probably a better choice."
  ([] (sorted-map (v0/order-size) (v0/buffer-size)))
  ([n] (sorted-map (v0/order-size n) (v0/buffer-size n)))
  ([order buffer-size] (v0-types/->BBTreeSortedMap (v0-tree/root :map order buffer-size) order buffer-size {})))

(defn sorted-map-by
  "Returns a buffered-btree implementation of a sorted map with a custom comparator, as a drop-in replacement
  for the built-in sorted map. Has a transient version. Is optimized for a latency-heavy storage backend
  (see backed-sorted-map-by), so clojure.core/sorted-map-by is probably a better choice."
  ([cmp] (sorted-map-by cmp (v0/order-size) (v0/buffer-size)))
  ([cmp n] (sorted-map-by cmp (v0/order-size n) (v0/buffer-size n)))
  ([cmp order buffer-size] (v0-types/->BBTreeSortedMap (v0-tree/root-by :map order buffer-size cmp) order buffer-size {})))

(defn sorted-set
  "Returns a buffered-btree implementation of a sorted set, as a drop-in replacement for the built-in sorted set.
  Has a transient version. Is optimized for a latency-heavy backing (see backed-sorted-set), so clojure.core's
  sorted-set is probably a better choice for you."
  ([] (sorted-set (v0/order-size) (v0/buffer-size)))
  ([n] (sorted-set (v0/order-size n) (v0/buffer-size n)))
  ([order buffer-size] (v0-types/->BBTreeSortedSet (v0-tree/root :set order buffer-size) order buffer-size {})))

(defn sorted-set-by
  "Returns a buffered-btree implementation of a sorted set with a custom comparator, as a drop-in replacement
  for core's sorted set. Has a transient version. This is optimized for a latency-heavy storage solution
  (see backed-sorted-set-by), so clojure.core/sorted-set-by is probably a better choice."
  ([cmp] (sorted-set-by cmp (v0/order-size) (v0/buffer-size)))
  ([cmp n] (sorted-set-by cmp (v0/order-size n) (v0/buffer-size n)))
  ([cmp order buffer-size] (v0-types/->BBTreeSortedSet (v0-tree/root-by :set order buffer-size cmp) order buffer-size {})))

(defn backed-sorted-map
  "Returns a buffered-btree implementation of a sorted map, backed by a persistent key value store.
  This is optimized for a solution with high latency (> 10 ms) for retrieving nodes, and the transient
  version simply stores up a sequence of actions to take, performing them in batch at the time of the
  persist! call."
  ([store] (backed-sorted-map store (v0/order-size) (v0/buffer-size)))
  ([store n] (backed-sorted-map store (v0/order-size n) (v0/buffer-size n)))
  ([store order buffer-size]
   (let [root-node (v0-storage/init-persist store (v0-tree/root :map order buffer-size))]
     (v0-types/->BackedBBTreeSortedMap root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-map-by
  "Returns a buffered-btree implementation of a sorted map with a custom comparator, backed by a persistent
  key-value store. This is optimized for high fetch latency (> 10 ms) for retrieving nodes, and the transient
  version simply stores up a sequence of actions to take, performing them in batch at the time of the persist!
  call."
  ([cmp store] (backed-sorted-map-by cmp store (v0/order-size) (v0/buffer-size)))
  ([cmp store n] (backed-sorted-map-by cmp store (v0/order-size n) (v0/buffer-size n)))
  ([cmp store order buffer-size]
   (let [root-node (v0-storage/init-persist store (v0-tree/root-by :map order buffer-size cmp))]
     (v0-types/->BackedBBTreeSortedMap root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-set
  "Returns a buffered-btree implementation of a sorted set backed by a persistent key-value store, serving
  as a drop-in replacement for the built-in sorted set. This is optimized for high IO latency (> 10 ms)
  and the transient version simply stores up a sequence of conjoins and disjoins which are performed
  in a batch at the time of the persist! call."
  ([store] (backed-sorted-set store (v0/order-size) (v0/buffer-size)))
  ([store n] (backed-sorted-set store (v0/order-size n) (v0/buffer-size n)))
  ([store order buffer-size]
   (let [root-node (v0-storage/init-persist store (v0-tree/root :set order buffer-size))]
     (v0-types/->BackedBBTreeSortedSet root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-set-by
  "Returns a buffered-btree implementation of a sorted set with a custom comparator, backed by a persistent
  key-value store. Serves as a drop-in replacement for Clojure's sorted set. This is optimized for high
  IO latency (> 10 ms) for retrieving nodes, and the transient version simply stores up a sequence of
  conjoins and disjoins which are finally performed in batch at the time of the persist! call."
  ([cmp store] (backed-sorted-set-by cmp store (v0/order-size) (v0/buffer-size)))
  ([cmp store n] (backed-sorted-set-by cmp store (v0/order-size n) (v0/buffer-size n)))
  ([cmp store order buffer-size]
   (let [root-node (v0-storage/init-persist store (v0-tree/root-by :set order buffer-size cmp))]
     (v0-types/->BackedBBTreeSortedSet root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

;; =========================
;; == OPEN EXISTING TREES ==
;; =========================

(defn open
  "Constructs a new map/set from the specified root. The resulting map/set cannot be persisted."
  [store uuid]
  (v0/open* store uuid false))

(defn open-writable
  "Constructs a new map/set from the specified root. The resulting map/set can be persisted."
  [store uuid]
  (v0/open* store uuid true))

(defn open-set
  "Constructs a new set from the specified root. The resulting set cannot be persisted.
   Throw an exception if the tree was originally a map."
  [store uuid]
  (v0/open* :set store uuid false))

(defn open-writable-set
  "Constructs a new set from the specified root. The resulting set can be persisted.
   Throw an exception if the tree was originally a map."
  [store uuid]
  (v0/open* :set store uuid true))

(defn open-map
  "Constructs a new map from the specified root. The resulting map cannot be persisted.
   Throw an exception if the tree was originally a set."
  [store uuid]
  (v0/open* :map store uuid false))

(defn open-writable-map
  "Constructs a new map from the specified root. The resulting map can be persisted.
   Throw an exception if the tree was originally a set."
  [store uuid]
  (v0/open* :map store uuid true))

;; =====================
;; ===== QUERY FNS =====
;; =====================

(d/defn ^{::d/aspects [timed]} between
  "We are trying to construct a more inclusive search. We are going to make a
  query in which no one is left out."
  [set-or-map low high]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (v0-query/between (dsp/store set-or-map) (dsp/root-node set-or-map) low high)
    (v0-query/between (dsp/root-node set-or-map) low high)))

(d/defn ^{::d/aspects [timed]} subrange
  "Returns a sequence containing all elements from the map/set within the specified range."
  [set-or-map range]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (v0-query/subrange (dsp/store set-or-map) (dsp/root-node set-or-map) range)
    (v0-query/subrange (dsp/root-node set-or-map) range)))

(d/defn ^{::d/aspects [timed]} subranges
  "Returns a map: range->sequence, where each sequence contains (in order) all the elements
  from the map/set contained by the corresponding range."
  [set-or-map ranges]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (v0-query/subranges (dsp/store set-or-map) (dsp/root-node set-or-map) ranges)
    (v0-query/subranges (dsp/root-node set-or-map) ranges)))
