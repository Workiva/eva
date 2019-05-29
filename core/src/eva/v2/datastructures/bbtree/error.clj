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

(ns eva.v2.datastructures.bbtree.error
  (:require [eva.error :refer [deferror-group]])
  (:import (eva.error.v1 EvaErrorCode)))

(deferror-group storage
  [:bbtree.storage [:method]]
  (failure "A storage failure occurred in the btree")
  (timeout "A storage timeout occurred in the btree" [:timeout-ms]))

(deferror-group fressian-read-err
  [:fressian.unreadable [:handler-chain]]
  (vector "Unable to deserialize vector")
  (list "Unable to deserialize list")
  (set "Unable to deserialize set")
  (var "Unable to resolve var")
  (byte-string "Unable to deserialize ByteString"))

(def overrides
  {:flowgraph/timeout EvaErrorCode/STORAGE_TIMEOUT})
