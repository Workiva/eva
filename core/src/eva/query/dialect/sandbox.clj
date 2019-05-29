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

(ns eva.query.dialect.sandbox
  (:require [eva.query.dialect.aggregates]
            [eva.query.dialect.functions]))

(defn create-eval-ns []
  (let [ns-sym (symbol (str *ns* "." (gensym "evalns")))
        ns (create-ns ns-sym)]
    (binding [*ns* ns]
      (refer-clojure :exclude '[eval min max rand])
      (refer 'eva.query.dialect.aggregates)
      (refer 'eva.query.dialect.functions))
    (.setDynamic ^clojure.lang.Var (intern ns 'sym->edb))
    (.setDynamic ^clojure.lang.Var (intern ns 'pattern-vars))
    ns))

;; ^^^^^ sym->edb ^^^^^^
;; Data sources are passed around as symbols in the datalog engine. sym->edb is used
;; to resolve the symbol to a specific Extensional database.
;;
;; When the query is compiled to Datalog, it wraps all the Extensional
;; sources with a single Extensional object with an extra ?src-var term; this object
;; then dispatches based on the value of that ?src-var.
;;
;; But if a data source is passed to a function, the function expects to see an actual
;; data source, not symbol. Because data sources are not actually available until
;; the inputs are passed into the query, and because we support arbitrary functions,
;; and because of *details*, it is easier to have the sym->edb map available at query
;; runtime than it is to wrap the functions correctly at compile time.

(def ^:dynamic *sandbox-ns* (create-eval-ns))
