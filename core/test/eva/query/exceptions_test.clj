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

(ns eva.query.exceptions-test
  (:require [eva.query.core :refer [q inspect]]
            [eva.error :refer [is-thrown?]])
  (:import [eva.error.v1 EvaErrorCode])
  (:use [clojure.test]))

(deftest better-exception:missing-predicate-expression
  (is-thrown?
   {:msg-re #".*predicate does not exist\."
    :error-codes #{EvaErrorCode/UNKNOWN_QUERY_PREDICATE
                   EvaErrorCode/DATALOG_INTERPRETER_FAILURE
                   EvaErrorCode/PROCESSING_FAILURE}
    :http-code 422
    :error-type :query.translation/unknown-predicate}
   (q '[:find ?name
        :in $ %
        :where
        (book-1author "Modeling Business Processes: A Petri-Net Oriented Approach" ?name)]
      []
      '[[(book-author ?book ?name)
         [?b :book/title ?book]
         [?b :book/author ?a]
         [?a :author/name ?name]]])))
