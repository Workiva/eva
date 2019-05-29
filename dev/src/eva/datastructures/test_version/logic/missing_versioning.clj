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

(ns eva.datastructures.test-version.logic.missing-versioning
  (:require [eva.datastructures.test-version.logic.versioning :refer [convert-from-v0]]
            [eva.datastructures.test-version.logic.nodes :as nodes]))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.nodes.BufferedBTreePointer
  convert-from-v0:pointer
  [o]
  (nodes/map->BufferedBTreePointer o))
