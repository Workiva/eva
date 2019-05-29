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

(ns eva.v2.datastructures.bbtree.logic.v0.error
  (:require [eva.error :refer [deferror-group]]))

(deferror-group persist
  [:bbtree.faulty-persist]
  (local-only "This copy cannot be persisted"))

(deferror-group safety
  [:bbtree.safety]
  (not-found "A pre-existing buffered btree was expected but not found")
  (no-overwrite "Attempting to overwrite newer version of buffered btree")
  (unpersisted-changes "This operation cannot be called on a tree with unpersisted changes"))

(deferror-group fressian-read-err
  [:fressian.unreadable [:handler-chain]]
  (bbtree-node "Unable to deserialize tree node")
  (bbtree-pointer "Unable to deserialize tree pointer")
  (bbtree-buffer "Unable to deserialize node buffer")
  (bbtree-message "Unable to deserialize bbtree message"))
