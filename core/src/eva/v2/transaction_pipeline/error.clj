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

(ns eva.v2.transaction-pipeline.error
  "Transaction pipeline related errors and error codes."
  (:require [eva.error :as ee])
  (:import [eva.error.v1 EvaErrorCode]))

(ee/deferror-group tx-fn-error
  (:transaction-pipeline [:fn])
  (tx-fn-threw "A tx-fn threw an exception")
  (tx-fn-illegal-return "A tx-fn return value indecipherable."))

(ee/deferror unrecognized-command
  :transaction-pipeline/unrecognized-command
  "Cannot interpret object as a transaction command"
  [:unrecognized-command])

(ee/deferror invalid-tx-inst
  :transact-exception/invalid-tx-inst
  "invalid :db/txInstant")

(ee/deferror clock-skew
  :transact-exception/clock-skew
  "excessive transactor clock skew detected")

;; The following map is 
(def error-codes 
     "For adding overrides to the types of exceptions thrown from the 
     transactor. 
     
     There aren't any as of yet, but it's probably pretty likely 
     we'll find *some* case of exception that we want to be a bit more 
     obvious from the context of a transaction."
     {:attribute-resolution/unresolvable-attribute EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
      :attribute-resolution/irresolvable-attribute EvaErrorCode/MODEL_CONSTRAINT_VIOLATION})
