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

(ns eva.query.dialect.functions
  (:require [eva.attribute :refer [cardinality]]
            [eva.core :refer [select-datoms multi-select-datoms-ordered]]
            [eva.query.error :refer [raise-builtin]]
            [eva.error :refer [raise]]))

(defn get-else [coll-of-db-eid-attr-default]
  (let [grouped (group-by first coll-of-db-eid-attr-default)]
    (apply concat
           (for [[db-sym coll-of-db-eid-attr-default] grouped]
             (let [db (-> (ns-resolve *ns* 'sym->edb) (var-get) (get db-sym))]
               (when (nil? db)
                 (raise-builtin :function (format "missing? cannot resolve the db '%s'." db-sym)
                                {:edb-syms (keys (-> (ns-resolve *ns* 'sym->edb) (var-get)))
                                 :ns-name (ns-name *ns*)}))
               (let [->ea (fn [[_ eid attr _]] [eid attr])
                     ->default (fn [[_ _ _ default]] default)
                     qs (->> coll-of-db-eid-attr-default
                             (map ->ea)
                             (apply vector :eavt)
                             (multi-select-datoms-ordered db))]
                 (map #(if-not (empty? %) (:v (first %)) (->default %2))
                      qs
                      coll-of-db-eid-attr-default)))))))

;; TODO: Implement an optimised `ground` rather than it just being `identity`
(def ground identity)

(defn missing? [coll-of-db-eid-attr]
  (let [grouped (group-by first coll-of-db-eid-attr)]
    (apply concat
           (for [[db-sym coll-of-db-eid-attr] grouped]
             (let [db (-> (ns-resolve *ns* 'sym->edb) (var-get) (get db-sym))]
               (when (nil? db)
                 (raise-builtin :function (format "missing? cannot resolve the db '%s'." db-sym)
                                {:edb-syms (keys (ns-resolve *ns* 'sym->edb) (var-get))
                                 :ns-name (ns-name *ns*)}))
               (map empty?
                    (multi-select-datoms-ordered db (apply vector :eavt (map next coll-of-db-eid-attr)))))))))
