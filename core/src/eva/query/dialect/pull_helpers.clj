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

(ns eva.query.dialect.pull-helpers
  "Contains helpers for reading data from a conformed pull spec.

   Mostly involves normalizing pattern-data-literal from the grammar
   into a form that's a bit more amenable for processing."
  (:require [eva.attribute :refer [rm_*]]))

(defn update-attr [norm attr-name m]
  (update-in norm [:attr-specs attr-name] merge m))

(defmulti add-to-normalized-pdl (fn [norm [attr-spec]] attr-spec))

(defn normalize-pdl [pdl]
  (reduce add-to-normalized-pdl {:attr-specs {}} pdl))

(defmethod add-to-normalized-pdl :attr-name
  [norm [_ [name-type attr-name]]]
  (update-attr norm attr-name {:normalized-attr-name (rm_* attr-name)
                               :attr-type name-type}))

(defmethod add-to-normalized-pdl :wildcard [norm _]
  (assoc norm :wildcard? true))

(defmethod add-to-normalized-pdl :attr-expr [norm expr]
  (add-to-normalized-pdl norm (second expr)))

(defmethod add-to-normalized-pdl :limit-expr [norm [_ limit-expr]]
  (let [{:keys [attr-name limit]} limit-expr
        [name-type attr-name] attr-name]
    (update-attr norm attr-name {:normalized-attr-name (rm_* attr-name)
                                 :attr-type name-type
                                 :limit limit})))

(defmethod add-to-normalized-pdl :default-expr [norm [_ default-expr]]
  (let [{:keys [attr-name value]} default-expr
        [name-type attr-name] attr-name]
    (update-attr norm attr-name {:normalized-attr-name (rm_* attr-name)
                                 :attr-type name-type
                                 :default value})))

(defn add-map-spec-entry [norm [k v]]
  (case [(first k) (first v)]
    [:attr-name :pattern]
    (let [[_ [name-type attr-name]] k
          [_ pattern] v]
      (update-attr norm
                   attr-name
                   {:normalized-attr-name (rm_* attr-name)
                    :attr-type name-type
                    :pattern (normalize-pdl pattern)}))

    [:attr-name :recursion-limit]
    (let [[_ [name-type attr-name]] k
          [_ [_ rec-limit]] v]
      (update-attr norm
                   attr-name
                   {:normalized-attr-name (rm_* attr-name)
                    :attr-type name-type
                    :recursion-limit rec-limit}))

    [:limit-expr :pattern]
    (let [[_ {:keys [attr-name limit]}] k
          [name-type attr-name] attr-name
          [_ pattern] v]
      (update-attr norm
                   attr-name
                   {:normalized-attr-name (rm_* attr-name)
                    :attr-type name-type
                    :limit limit
                    :pattern (normalize-pdl pattern)}))

    [:limit-expr :recursion-limit]
    (let [[_ {:keys [attr-name limit]}] k
          [name-type attr-name] attr-name
          [_ [_ rec-limit]] v]
      (update-attr norm
                   attr-name
                   {:normalized-attr-name (rm_* attr-name)
                    :attr-type name-type
                    :limit limit
                    :recursion-limit rec-limit}))))


(defmethod add-to-normalized-pdl :map-spec [norm [_ map-spec]]
  (reduce add-map-spec-entry norm map-spec))
