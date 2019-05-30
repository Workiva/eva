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

(ns eva.datastructures.protocols
  (:refer-clojure :exclude [filter]))

(defprotocol BTreeBacked
  (root-node [this] "Returns the root node of the btree backing this data structure.")
  (in-mem-nodes [this] "Returns the collection of nodes that are not persisted."))

;; So long as we only load nodes into memory when we are about to modify them, in-mem-nodes can satisfy
;; its contract simply by returning all nodes that are loaded into memory. Future work that could threaten
;; this assumption includes local-only write-on-read behaviors. Until then, the main threat to the
;; assumption has come from the balancing of nodes in the tree, and currently we keep enough information
;; in the pointers to avoid loading them into memory unless we actually need to modify the nodes.

(defprotocol BTreeOps
  (filter [this f] "When using this with backed trees, f must implement datastructures.utils.fressian.Autological.")
  (remove-interval [this rng] "Removes everything that falls within the supplied interval.")
  (keep-interval [this rng] "Removes everything EXCEPT what falls within the supplied interval."))

(defprotocol BTreeTransientOps
  (filter! [this f] "When using this with backed trees, f must implement datastructures.utils.fressian.Autological.")
  (remove-interval! [this rng] "Removes everything that falls within the supplied interval.")
  (keep-interval! [this rng] "Removes everything EXCEPT what falls within the supplied interval."))

(defprotocol EditableBackedStructure
  (make-editable! [this] "Returns an editable version of the given structure. Provides no contract vis-a-vis the safety of editing"))

(defprotocol BackedStructure
  (storage-id [this] "Returns the storage id (should be uuid) of the root of this datastructure. Throws an error if it has not been persisted.")
  (persisted? [this] "Is this version persisted to storage?")
  (persist! [this] "Persists the current version.")
  (store [this] "Returns the persistent store backing this structure."))

(defprotocol Versioned
  (get-version [this] "Expected to return a namespaced keyword: :datastructure-type/version-id. Used by multimethod ensure-version."))

(defprotocol Interval
  (low [this] "Returns the min of the range.")
  (high [this] "Returns the max of the range.")
  (low-open? [this] "Returns true if lower end of interval is open")
  (high-open? [this] "Returns true if higher end of interval is open")
  (intersect [this cmp other] "Returns the intersection of this interval and the other.")
  (restrict [this cmp low high] "Returns a new range restricted by the given low and high elements. Open and closed properties remain unchanged.")
  (interval-overlaps? [this cmp other-range] [this cmp low high] "Tells you whether the range overlaps with the given range.")
  (interval-contains? [this cmp point]))
