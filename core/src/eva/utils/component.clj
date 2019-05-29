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

(ns eva.utils.component
  (:require [eva.error :refer [insist]]
            [com.stuartsierra.component :refer [start stop]]))

(defmacro with-systems
  "bindings => [name init ...]

  Evaluates body in a try expression with names bound to the values
  of the inits, and a finally clause that calls (.close name) on each
  name in reverse order."
  {:added "1.0"}
  [bindings & body]
  (insist (and (vector? bindings) (even? (count bindings))))
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(conj (subvec bindings 0 2)
                                        (bindings 0) `(start ~(bindings 0)))
                              (try
                                (with-systems ~(subvec bindings 2) ~@body)
                                (finally
                                  (stop ~(bindings 0)))))
    :else (throw (IllegalArgumentException. "with-systems only allows Symbols in bindings"))))
