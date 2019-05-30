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

(ns eva.logging
  (:import [org.slf4j MDC]))

(defn with-context* [context f]
  (let [context (if (map? context) context (into {} context))
        curr (not-empty (MDC/getCopyOfContextMap))]
    (try (doseq [[k v] context
                 :let [k (if (keyword? k) (name k) (str k))]]
           (MDC/put k (str v)))
         (f)
         (finally
           (if curr
             (MDC/setContextMap curr)
             (MDC/clear))))))
(defmacro with-context [context & body] `(with-context* ~context (fn [] ~@body)))
