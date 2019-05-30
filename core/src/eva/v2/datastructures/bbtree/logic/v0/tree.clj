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

(ns eva.v2.datastructures.bbtree.logic.v0.tree
  (:require [eva.v2.datastructures.bbtree.logic.v0.nodes :as nodes]
            [eva.v2.datastructures.bbtree.logic.v0.buffer :as buffer]
            [eva.datastructures.utils.comparators :refer [defcomparator]]
            [eva.error :refer [insist]]
            [clojure.data.avl :as avl]))

(defcomparator ^:bound-above ^:bound-below default-comparator
  compare)

(defn root
  "Returns a new root for a btree, sorted by the equivalent of =compare="
  [semantics order buffer-size]
  (insist (or (= semantics :map) (= semantics :set)))
  (let [cmp default-comparator]
    (nodes/->BufferedBTreeNode nil 0 0
                               (buffer/btree-buffer buffer-size)
                               (avl/sorted-map-by cmp)
                               (nodes/map->NodeProperties {:order order
                                                           :buffer-size buffer-size
                                                           :root? true
                                                           :leaf? true
                                                           :min-rec nil
                                                           :max-rec nil
                                                           :count-rec 0
                                                           :node-counter 0
                                                           :comparator cmp
                                                           :semantics semantics}))))

(defn root-by
  [semantics order buffer-size cmp]
  (insist (or (= semantics :map) (= semantics :set)))
  (insist (instance? eva.datastructures.utils.comparators.Comparator cmp))
  (insist (and (eva.datastructures.utils.comparators/bounded-above? cmp)
               (eva.datastructures.utils.comparators/bounded-below? cmp)))
  (nodes/->BufferedBTreeNode nil 0 0
                             (buffer/btree-buffer buffer-size)
                             (avl/sorted-map-by cmp)
                             (nodes/map->NodeProperties {:order order
                                                         :buffer-size buffer-size
                                                         :root? true
                                                         :leaf? true
                                                         :min-rec nil
                                                         :max-rec nil
                                                         :count-rec 0
                                                         :node-counter 0
                                                         :comparator cmp
                                                         :semantics semantics})))
