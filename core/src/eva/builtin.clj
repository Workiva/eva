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

(ns eva.builtin
  "Abstraction point for modifying built in database functions."
  (:require [eva.attribute]
            [eva.core :as core]
            [eva.error :refer [raise deferror-group]]
            [eva.entity-id :refer [entid-strict]])
  (:import [eva Database Attribute]))

(deferror-group cas-failure
  (:eva.cas [:e :a :expected])
  (failed-comparison "Comparison failed for CAS" [:found])
  (ambiguous-target "Cannot CAS on an attribute with :db.cardinality/many"))

(defn retract-entity [^Database db entity-to-retract]
  (let [->retracts (map (juxt (constantly :db/retract) :e :a :v))]
    (loop [[eid & eids] [(.entid db entity-to-retract)]
           seen #{}
           res #{}]
      (if-not eid
        res
        (if (seen eid)
          (recur eids seen res)
          (let [eavt (core/select-datoms db [:eavt eid]) ;; select all children
                vaet (core/select-datoms db [:vaet eid])];; select all references
            (recur (into eids ;; add all subcomponents for recurring.
                         (comp (filter
                                (fn [datom]
                                  (let [^Attribute a (eva.attribute/resolve-attribute db (:a datom))]
                                    (and (.isComponent a)
                                         (= :db.type/ref (.valueType a))))))
                               (map :v))
                         eavt)
                   (conj seen eid)
                   (into res ->retracts (concat eavt vaet)))))))))

(defn cas [^Database db e a v-old v-new]
  (let [attr ^Attribute (eva.attribute/resolve-attribute db a)]
    (when-not (= :db.cardinality/one (.cardinality attr))
      (raise-cas-failure :ambiguous-target
                         "aborting CAS"
                         {:e e, :a a, :expected v-old}))
    (let [ref? (= :db.type/ref (.valueType attr)) ;; Resolve reference-type values
          e' (entid-strict db e)
          v-old (if ref? (.entid db v-old) v-old)
          v-new (if ref? (.entid db v-new) v-new)
          d-old (first (core/select-datoms db [:eavt e' a]))]
      (if-not (= (:v d-old) v-old)
        (raise-cas-failure :failed-comparison
                           "aborting CAS"
                           {:e e, :a a, :expected v-old, :found (:v d-old)})
        (when (not= v-old v-new)
          (cond-> []
            (some? (:v d-old)) (conj [:db/retract e' a v-old])
            (some? v-new)      (conj [:db/add e' a v-new])))))))
