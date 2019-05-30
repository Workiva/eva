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

(ns eva.query.dialect.translation.error
  (:require [eva.error :refer :all]))

(deferror-group translation-error
  :query.translation
  (invalid-form "Invalid query form")
  (unrecognized-clause "Unrecognized clause type")
  (unknown-predicate "Unknown predicate symbol" [:symbol])
  (unresolved-sym "Unable to resolve symbol" [:fn-sym]))

(deferror edb-error
  :query.inputs/invalid-data-source
  "Invalid data source"
  [:src-var])
