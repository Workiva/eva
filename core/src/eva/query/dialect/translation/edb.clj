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

(ns eva.query.dialect.translation.edb
  (:require [eva.query.datalog.protocols :refer :all]
            [eva.query.datalog.edb :refer [coll-of-tuples->EDB]]
            [eva.query.dialect.translation.error :refer [raise-edb-error]]
            [eva.error :refer [insist]]
            [recide.sanex :as sanex]
            [ichnaie.core :refer [tracing]]))

(defrecord SymbolDispatchingEDB
    [sym->edb]
  EDB
  (extensions [_ terms]
    (tracing "eva.query.datalog.protocols/extensions"
      (let [src->terms (group-by first terms)]
        (->> (for [[src group] src->terms]
               (if-let [edb (get sym->edb src)]
                 (map (partial cons src)
                      (extensions edb (map rest group)))
                 (raise-edb-error (format "var '%s' does not correspond to any extensional source." src)
                                  {:src-var src
                                   ::sanex/sanitary? true})))
             (apply concat))))))


;;;

(defn ->edb-composer-fn [idx-sym]
  (fn [inputs]
    (let [sym->edb (into {}
                         (for [[idx sym] idx-sym]
                           (let [edb (nth inputs idx)
                                 edb (cond (satisfies? EDB edb) edb
                                           (coll? edb) (coll-of-tuples->EDB edb)
                                           (nil? edb) (raise-edb-error (format "input for '%s' is nil." sym)
                                                                       {:src-var sym})
                                           :else (raise-edb-error (format "input for '%s' does not satisfy the EDB protocol" sym)
                                                                  {:src-var sym}))]
                             [sym edb])))]
      {:sym->edb sym->edb
       :datalog-edb (->SymbolDispatchingEDB sym->edb)})))

;;;;;;spec version

(defn src-var? [[type]]
  (= type :src-var))
(defn src-var-value [[_ val]] val)

(defn edb-composer-gen-spec
  "Given the :in clause of a query, this returns a constructor that will take the
  sequence of corresponding runtime inputs and generates an object satisfying the
  EDB protocol that handles all datom resolution by delegating to the sources passed
  in.

  The EDB returned by this function expects an additional initial argument, which is simply
  the symbol corresponding to the source to which it should delegate."
  [inputs-list]
  (let [idx-sym (keep-indexed #(when (src-var? %2) [%1 (src-var-value %2)])
                              inputs-list)]
    (->edb-composer-fn idx-sym)))
