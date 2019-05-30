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

(ns eva.v2.datastructures.bbtree
  "Convenience namespace to refer to protocol methods and certain internal btree methods
  all in one place. See eva.v2.datastructures.bbtree.api for implementation details and docs."
  (:require [potemkin :as p]
            [eva.v2.datastructures.bbtree.api]
            [eva.datastructures.protocols])
  (:refer-clojure :exclude [sorted-map sorted-set sorted-map-by sorted-set-by filter]))

(p/import-vars
 [eva.v2.datastructures.bbtree.api
  sorted-map
  sorted-map-by
  sorted-set
  sorted-set-by
  backed-sorted-map
  backed-sorted-map-by
  backed-sorted-set
  backed-sorted-set-by
  open
  open-writable
  open-set
  open-writable-set
  open-map
  open-writable-map
  between
  subrange
  subranges]
 [eva.datastructures.protocols
  persist!
  make-editable!
  root-node
  storage-id
  store
  filter
  remove-interval
  keep-interval
  filter!
  remove-interval!
  keep-interval!
  in-mem-nodes])
