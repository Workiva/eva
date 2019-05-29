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

(ns eva.datastructures.test-version.logic.core
  (:require [eva.datastructures.test-version.logic.versioning] ;; <== necessary for conversions from v0.
            [eva.datastructures.test-version.logic.protocols :as protocols]
            [eva.datastructures.test-version.logic.types :as types]
            [eva.datastructures.test-version.logic.nodes :as nodes]
            [eva.datastructures.test-version.logic.error :refer [raise-safety]]
            [eva.datastructures.test-version.logic.protocols :refer [VERSION]]
            [eva.v2.datastructures.bbtree.storage :as node-storage]
            [eva.datastructures.versioning :refer [ensure-version]]
            [eva.error :refer [insist]])
  (:import [eva.datastructures.test_version.logic.nodes BufferedBTreePointer BufferedBTreeNode]))

(defn order-size ([] 250) ([n] (int (Math/pow n 0.5))))
(defn buffer-size ([] 250) ([n] (- n (int (Math/pow n 0.5)))))

(defn- construct
  [constructor store node writable?]
  (insist (instance? BufferedBTreeNode node))
  (constructor node
               (node-storage/uuid node)
               (protocols/node-order node)
               (protocols/buffer-size node)
               store
               writable?
               true
               {}))

(defn open*
  "Provides implementation for bbtree API open functions. uuid should refer to the root
  of the structure, and writable? should be a boolean. semantics should be either
  :set or :map."
  ([store uuid writable?]
   (let [pointer (ensure-version VERSION (nodes/ensure-pointer uuid))]
     (if-let [desired-root (node-storage/get-node store pointer)]
       (let [desired-root (ensure-version VERSION desired-root)
             constructor (condp = (:semantics (protocols/properties desired-root))
                           :map types/->BackedBBTreeSortedMap
                           :set types/->BackedBBTreeSortedSet)]
         (construct constructor store desired-root writable?))
       (raise-safety :not-found (format "Unable to find node %s in provided store." (node-storage/uuid pointer)) {:method 'open*}))))
  ([semantics store uuid writable?]
   (let [pointer (ensure-version VERSION (nodes/ensure-pointer uuid))]
     (if-let [desired-root (node-storage/get-node store pointer)]
       (let [desired-root (ensure-version VERSION desired-root)
             constructor (condp = (:semantics (protocols/properties desired-root))
                           :map types/->BackedBBTreeSortedMap
                           :set types/->BackedBBTreeSortedSet)]
         (insist (= (:semantics (protocols/properties desired-root)) semantics))
         (construct constructor store desired-root writable?))
       (raise-safety :not-found (format "Unable to find node %s in provided store." (node-storage/uuid pointer)) {:method 'open*})))))


