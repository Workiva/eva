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

(ns eva.test.clojure-test-ext
  "Provides extensions to clojure.test"
  (:require [clojure.test :refer :all]))

;; Allows you to test for an exception along with its cause.
;; This is particularly useful when dealing with ExecutionExceptions:
;;
;; ```
;; (is (thrown-with-cause? ExecutionException IllegalArgumentException
;;      @(future (throw (IllegalArgumentException.))
;; ```
(defmethod assert-expr 'thrown-with-cause? [msg form]
  (let [outer-klass (nth form 1)
        inner-klass (nth form 2)
        body (nthnext form 3)]
    `(try ~@body
          (do-report {:type :fail, :message ~msg
                      :expected '~form :actual nil})
          (catch ~outer-klass e#
            (if (instance? ~inner-klass (.getCause e#))
              (do-report {:type     :pass, :message ~msg
                          :expected '~form, :actual (.getCause e#)})
              (do-report {:type :fail, :message ~msg,
                          :expected '~form, :actual (.getCause e#)}))
            (.getCause e#)))))
