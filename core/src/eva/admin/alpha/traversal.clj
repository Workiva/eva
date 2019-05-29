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

(ns eva.admin.alpha.traversal
  "Provides a generic protocol for traversing hierarchically linked data,
   and implementations of that protocol against stored Eva objects"
  (:require [eva.v2.system.database-catalogue.core :as dbc]
            [eva.v2.datastructures.vector :as vect]
            [eva.v2.database.log :as log]
            [eva.v2.datastructures.bbtree :as bbtree]
            [eva.v2.datastructures.bbtree.logic.v0.protocols :as bp]
            [eva.v2.datastructures.bbtree.storage :as bs]
            [eva.v2.datastructures.bbtree.logic.v0.nodes :as bn]
            [eva.v2.datastructures.bbtree.logic.v0.state :as bst]
            [eva.v2.storage.value-store :as vs-api :refer [create-key]])
  (:import (eva.v2.system.database_catalogue.core DatabaseInfo)
           (eva.v2.datastructures.vector PersistedVector)
           (eva.v2.datastructures.bbtree.logic.v0.nodes BufferedBTreeNode)
           (eva.v2.database.log TransactionLogEntry)
           (eva.v2.datastructures.bbtree.logic.v0.types BackedBBTreeSortedSet)))

(defprotocol Traversable
  (k [this] "Provides the value-store key this Traversable is stored under.")
  (v [this] "Provides the value-store value this Traversable is stored as.")
  (expandable? [this] "Is this Traversable expandable?")
  (expand [this xform] "Yield a lazy sequence of the zero or more Traversable objects reachable from this object, xform is a transducer on the traversable objects that is applied before the sequence of Traversables is returned."))

(defn shim-log-entry
  "Fetch a tx-log-entry from a vector and inject metadata on the entry,
   enabling it to satisfy Traversable."
  [^PersistedVector pv i]
  (-> (nth pv i)
      deref
      (vary-meta assoc
                 ::key (vect/nth-key (.head_key pv) i)
                 ::store (.store pv))))

(defn shim-database-info
  "Fetch database info from a value store and inject metadata on the info,
   enabling it to satisfy Traversable."
  [value-store database-id]
  (-> (dbc/database-info value-store database-id)
      (vary-meta assoc ::store value-store)))

(defn pointer->node* [store pointer]
  (assert (some? store))
  (binding [bst/*store* store]
    (bn/pointer->node pointer)))

(defn safe-meta-get [x k]
  (let [r (get (meta x) k ::not-found)]
    (if-not (identical? ::not-found r)
      r
      (throw (IllegalStateException. "Failed to find key %s in metadata.")))))

(extend-protocol Traversable
  DatabaseInfo
  (k [this] (str (:database-id this)))
  (v [this] this)
  (expandable? [this] true)
  (expand [this xform]
    (sequence xform [(log/open-transaction-log (safe-meta-get this ::store) this)]))

  PersistedVector
  (k [this] (.head_key this))
  (v [this] (.cur_head this))
  (expandable? [this] (< 0 (count this)))
  (expand [this xform]
    (sequence
     (comp (map (partial shim-log-entry this))
           xform)
     (range (count this))))

  TransactionLogEntry
  (k [this] (safe-meta-get this ::key))
  (v [this] this)
  (expandable? [this] true)
  (expand [this xform]
    (let [store (safe-meta-get this ::store)]
      (sequence (comp (map (comp #(vary-meta % assoc ::store store)
                                 #(.root ^BackedBBTreeSortedSet %)
                                 (partial bbtree/open-set store)
                                 :index))
                      xform)
                (-> this :index-roots vals))))

  BufferedBTreeNode
  (k [this] (bs/uuid this))
  (v [this] this)
  (expandable? [this] (bp/inner-node? this))
  (expand [this xform]
    (sequence
     (comp (map (comp #(vary-meta % assoc ::store (safe-meta-get this ::store))
                      (partial pointer->node* (safe-meta-get this ::store))))
           xform)
     (vals (bp/children this)))))

(defn unique-on
  "Provides a transducer similar to 'distinct' with two key differences:

   1. The set of 'seen' objects is built at transducer construction,
      not transducer invocation.
   2. A function 'f' may be provided to discriminate uniqueness on (f x) vs x.
   3. A seed set of (f x) values may be provided to initialize the seen set."
  ([f] (unique-on f #{}))
  ([f seed]
   (let [seen (atom seed)]
     (vary-meta (fn [rf]
                  (fn
                    ([] (rf))
                    ([result] (rf result))
                    ([result input]
                     (if (contains? @seen (f input))
                       result
                       (do (swap! seen conj (f input))
                           (rf result input))))))
                assoc ::seen-vol seen))))

(defn postorder-tree-seq
  "Same as tree-seq, but performs a lazy postorder DFS vs a lazy preorder DFS."
  [branch? children root]
  (let [walk (fn walk [node]
               (lazy-cat
                (when (branch? node)
                  (mapcat walk (children node)))
                [node]))]
    (walk root)))

(defn traversal* [expansion-xform init-traversable]
  (postorder-tree-seq #(and (satisfies? Traversable %)
                            (expandable? %))
                      #(expand % expansion-xform)
                      init-traversable))

(defn traversal
  "Yields a lazy postorder DFS traversal of all objects reachable given a
   database-id. The postorder property means that all child nodes will be
   yielded before their parents.

   expansion-xform is an optional transducer on the traversable objects that is
   applied before the objects are returned. By default, a stateful transducer
   that elides all objects with a previously-seen k is provided. Non-Traversable
   objects returned by this xform are considered terminal wrt the traversal."
  ([value-store database-id]
   (traversal (unique-on k) value-store database-id))
  ([expansion-xform value-store database-id]
   (let [database-info (shim-database-info value-store database-id)]
     (traversal* expansion-xform database-info))))
