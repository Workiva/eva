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

(ns eva.utils.components
  (:require [com.stuartsierra.component :as component]))

(defmacro ^{:private true} assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                 (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

(defmacro with-components
  "Takes a binding vector of name-component pairs, starts all components (in the listed order),
  binds them to their associated name, executes the body statements, and then stops the
  components in reverse order."
  [bindings & body]
  (assert-args
   (vector? bindings) "a vector for bindings"
   (even? (count bindings)) "an even number of forms in binding vector")
  (cond (= (count bindings) 0) `(do ~@body)
        (symbol? (bindings 0)) `(let [~(bindings 0) (component/start ~(bindings 1))]
                                  (try (with-components ~(subvec bindings 2) ~@body)
                                       (finally (component/stop ~(bindings 0)))))))
