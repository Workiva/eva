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

(ns eva.datastructures.test-version.api
  "The function of this namespace is to collect the public-facing 'permanent' methods of the
  btree code. As new versions are written, the constructors here should be updated to point
  to the latest constructor code."
  (:require [eva.datastructures.protocols :as dsp]
            [eva.v2.datastructures.bbtree.storage :as node-storage]
            [eva.datastructures.test-version.logic.core :as test-version]
            [eva.datastructures.test-version.logic.query :as test-version-query]
            [eva.datastructures.test-version.logic.types :as test-version-types]
            [eva.datastructures.test-version.logic.tree :as test-version-tree]
            [eva.datastructures.test-version.logic.protocols :as test-version-protocols]
            [eva.datastructures.test-version.logic.storage :as test-version-storage])
  (:refer-clojure :exclude [sorted-map sorted-set sorted-map-by sorted-set-by]))

;; ==================n
;; == CONSTRUCTORS ==
;; ==================

(defn sorted-map
  "Returns a buffered-btree implementation of a sorted map, as a drop-in replacement for the built-in sorted map.
  Has a transient version. Is optimized for a latency-heavy backing (see backed-sorted-map), so the built-in
  sorted-map is probably a better choice."
  ([] (sorted-map (test-version/order-size) (test-version/buffer-size)))
  ([n] (sorted-map (test-version/order-size n) (test-version/buffer-size n)))
  ([order buffer-size] (test-version-types/->BBTreeSortedMap (test-version-tree/root :map order buffer-size) order buffer-size {})))

(defn sorted-map-by
  "Returns a buffered-btree implementation of a sorted map with a custom comparator, as a drop-in replacement
  for the built-in sorted map. Has a transient version. Is optimized for a latency-heavy storage backend
  (see backed-sorted-map-by), so clojure.core/sorted-map-by is probably a better choice."
  ([cmp] (sorted-map-by cmp (test-version/order-size) (test-version/buffer-size)))
  ([cmp n] (sorted-map-by cmp (test-version/order-size n) (test-version/buffer-size n)))
  ([cmp order buffer-size] (test-version-types/->BBTreeSortedMap (test-version-tree/root-by :map order buffer-size cmp) order buffer-size {})))

(defn sorted-set
  "Returns a buffered-btree implementation of a sorted set, as a drop-in replacement for the built-in sorted set.
  Has a transient version. Is optimized for a latency-heavy backing (see backed-sorted-set), so clojure.core's
  sorted-set is probably a better choice for you."
  ([] (sorted-set (test-version/order-size) (test-version/buffer-size)))
  ([n] (sorted-set (test-version/order-size n) (test-version/buffer-size n)))
  ([order buffer-size] (test-version-types/->BBTreeSortedSet (test-version-tree/root :set order buffer-size) order buffer-size {})))

(defn sorted-set-by
  "Returns a buffered-btree implementation of a sorted set with a custom comparator, as a drop-in replacement
  for core's sorted set. Has a transient version. This is optimized for a latency-heavy storage solution
  (see backed-sorted-set-by), so clojure.core/sorted-set-by is probably a better choice."
  ([cmp] (sorted-set-by cmp (test-version/order-size) (test-version/buffer-size)))
  ([cmp n] (sorted-set-by cmp (test-version/order-size n) (test-version/buffer-size n)))
  ([cmp order buffer-size] (test-version-types/->BBTreeSortedSet (test-version-tree/root-by :set order buffer-size cmp) order buffer-size {})))

(defn backed-sorted-map
  "Returns a buffered-btree implementation of a sorted map, backed by a persistent key value store.
  This is optimized for a solution with high latency (> 10 ms) for retrieving nodes, and the transient
  version simply stores up a sequence of actions to take, performing them in batch at the time of the
  persist! call."
  ([store] (backed-sorted-map store (test-version/order-size) (test-version/buffer-size)))
  ([store n] (backed-sorted-map store (test-version/order-size n) (test-version/buffer-size n)))
  ([store order buffer-size]
   (let [root-node (test-version-storage/init-persist store (test-version-tree/root :map order buffer-size))]
     (test-version-types/->BackedBBTreeSortedMap root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-map-by
  "Returns a buffered-btree implementation of a sorted map with a custom comparator, backed by a persistent
  key-value store. This is optimized for high fetch latency (> 10 ms) for retrieving nodes, and the transient
  version simply stores up a sequence of actions to take, performing them in batch at the time of the persist!
  call."
  ([cmp store] (backed-sorted-map-by cmp store (test-version/order-size) (test-version/buffer-size)))
  ([cmp store n] (backed-sorted-map-by cmp store (test-version/order-size n) (test-version/buffer-size n)))
  ([cmp store order buffer-size]
   (let [root-node (test-version-storage/init-persist store (test-version-tree/root-by :map order buffer-size cmp))]
     (test-version-types/->BackedBBTreeSortedMap root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-set
  "Returns a buffered-btree implementation of a sorted set backed by a persistent key-value store, serving
  as a drop-in replacement for the built-in sorted set. This is optimized for high IO latency (> 10 ms)
  and the transient version simply stores up a sequence of conjoins and disjoins which are performed
  in a batch at the time of the persist! call."
  ([store] (backed-sorted-set store (test-version/order-size) (test-version/buffer-size)))
  ([store n] (backed-sorted-set store (test-version/order-size n) (test-version/buffer-size n)))
  ([store order buffer-size]
   (let [root-node (test-version-storage/init-persist store (test-version-tree/root :set order buffer-size))]
     (test-version-types/->BackedBBTreeSortedSet root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

(defn backed-sorted-set-by
  "Returns a buffered-btree implementation of a sorted set with a custom comparator, backed by a persistent
  key-value store. Serves as a drop-in replacement for Clojure's sorted set. This is optimized for high
  IO latency (> 10 ms) for retrieving nodes, and the transient version simply stores up a sequence of
  conjoins and disjoins which are finally performed in batch at the time of the persist! call."
  ([cmp store] (backed-sorted-set-by cmp store (test-version/order-size) (test-version/buffer-size)))
  ([cmp store n] (backed-sorted-set-by cmp store (test-version/order-size n) (test-version/buffer-size n)))
  ([cmp store order buffer-size]
   (let [root-node (test-version-storage/init-persist store (test-version-tree/root-by :set order buffer-size cmp))]
     (test-version-types/->BackedBBTreeSortedSet root-node (node-storage/uuid root-node) order buffer-size store true true {}))))

;; =========================
;; == OPEN EXISTING TREES ==
;; =========================

(defn open
  "Constructs a new map/set from the specified root. The resulting map/set cannot be persisted."
  [store uuid]
  (test-version/open* store uuid false))

(defn open-writable
  "Constructs a new map/set from the specified root. The resulting map/set can be persisted."
  [store uuid]
  (test-version/open* store uuid true))

(defn open-set
  "Constructs a new set from the specified root. The resulting set cannot be persisted.
   Throw an exception if the tree was originally a map."
  [store uuid]
  (test-version/open* :set store uuid false))

(defn open-writable-set
  "Constructs a new set from the specified root. The resulting set can be persisted.
   Throw an exception if the tree was originally a map."
  [store uuid]
  (test-version/open* :set store uuid true))

(defn open-map
  "Constructs a new map from the specified root. The resulting map cannot be persisted.
   Throw an exception if the tree was originally a set."
  [store uuid]
  (test-version/open* :map store uuid false))

(defn open-writable-map
  "Constructs a new map from the specified root. The resulting map can be persisted.
   Throw an exception if the tree was originally a set."
  [store uuid]
  (test-version/open* :map store uuid true))

;; =====================
;; ===== QUERY FNS =====
;; =====================

(defn between
  "We are trying to construct a more inclusive search. We are going to make a
  query in which no one is left out."
  [set-or-map low high]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (test-version-query/between (dsp/store set-or-map) (dsp/root-node set-or-map) low high)
    (test-version-query/between (dsp/root-node set-or-map) low high)))

(defn subrange
  "Returns a sequence containing all elements from the map/set within the specified range."
  [set-or-map range]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (test-version-query/subrange (dsp/store set-or-map) (dsp/root-node set-or-map) range)
    (test-version-query/subrange (dsp/root-node set-or-map) range)))

(defn subranges
  "Returns a map: range->sequence, where each sequence contains (in order) all the elements
  from the map/set contained by the corresponding range."
  [set-or-map ranges]
  (if (satisfies? dsp/BackedStructure set-or-map)
    (test-version-query/subranges (dsp/store set-or-map) (dsp/root-node set-or-map) ranges)
    (test-version-query/subranges (dsp/root-node set-or-map) ranges)))
