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

(ns eva.query.dialect.translation.core
  (:require [eva.query.dialect.translation.compile :as compile]
            [eva.config :refer [config]]
            [eva.query.trace :as special]
            [eva.query.dialect.spec :as qs]
            [clojure.spec.alpha :as spec]
            [clojure.core.memoize :as mem])
  (:refer-clojure :exclude [compile]))

(def compile-query-and-rules
  ;; TODO: Measure and switch around the caching of compilation if spec conformation is expensive
  (mem/lru compile/compile
           :lru/threshold (config :eva.query.memoization-cache)))

(defn compile
  [query & inputs]
  (let [query (if (map? query)
                (mapcat (partial apply (partial apply list)) query)
                query)
        _ (special/trace "QUAESTIO: query looks like: \n" query)

        conformed-datalog (qs/conform! ::qs/datalog query)
        rules-idx (qs/rules-idx conformed-datalog)
        conformed-query (qs/conform! ::qs/query
                                     (cond-> [query]
                                             rules-idx (conj (nth inputs rules-idx))))]
    (compile-query-and-rules conformed-query)))
