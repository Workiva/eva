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

(ns eva.query.core
  (:require [eva.query.dialect.translation.core :refer [compile]]
            [eva.query.trace :as special]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:refer-clojure :exclude [compile]))

(d/defn ^{::d/aspects [traced]} q
  "This executes a query, given a matching set of inputs."
  [query & inputs]
  (special/trace "QUAESTIO: eva.query/q called.")
  (let [query (apply compile query inputs)
        _ (special/trace "QUAESTIO: Inspecting query: " ((:inspect query) inputs))
        r (query inputs)]
    (special/trace "QUAESTIO: eva/query/q returning.")
    r))

(defn inspect
  "This compiles a query with a matching set of inputs, and returns
  a map with :program (the compiled datalog program), :sym->edb
  (the map of source symbols to extensional objects), and :init-bindings
  (the set of initial bindings seeding the initial datalog query)."
  [query & inputs]
  ((:inspect (apply compile query inputs))
   inputs))
