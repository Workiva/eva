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

(ns eva.attribute
  (:require [eva.error :as err]
            [schema.core :as s]
            [map-experiments.smart-maps.bijection :refer [bijection]]
            [clojure.string :as cstr]
            [plumbing.core :as pc])
  (:import [eva.Attribute]))

(err/deferror-group attribute-non-resolution
  (:attribute-resolution [:identifier])
  (irresolvable-attribute "Encountered an invalid attribute specification")
  (unresolvable-attribute "This database does not contain the provided attribute"))

(def attr-bij
  (bijection  :db/id          :id
              :db/ident       :ident
              :db/cardinality :cardinality
              :db/valueType   :value-type
              :db/unique      :unique
              :db/noHistory   :no-history
              :db/isComponent :is-component
              :db/fulltext    :fulltext
              :db/indexed     :indexed))

(defprotocol ResolveAttribute
  (resolve-attribute [source identifier] "Resolves and returns the attribute given an identifier. Returns nil if the attribute cannot be resolved.")
  (resolve-attribute-strict [source identifier] "Resolves and returns the attribute given an identifier. Throws if the attribute cannot be resolved."))

(s/defrecord Attribute [id
                        ident
                        cardinality
                        value-type
                        unique
                        no-history
                        is-component
                        fulltext
                        indexed]
  eva.Attribute
  (id [_] id)
  (ident [_] ident)
  (cardinality [_] cardinality)
  (valueType [_] value-type)
  (isComponent [_] (boolean is-component))
  (unique [_] unique)
  (hasAVET [_] true)
  (hasFullText [_] (boolean fulltext))
  (hasNoHistory [_] false)
  (isIndexed [_] (if (some? indexed) (boolean indexed) true)))

(defn id
  ([^Attribute attr] (.id attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) (id))))
(defn ident
  ([^Attribute attr] (.ident attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) (ident))))
(defn cardinality
  ([^Attribute attr] (.cardinality attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) (cardinality))))
(defn value-type
  ([^Attribute attr] (.valueType attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) (value-type))))
(defn unique
  ([^Attribute attr] (.unique attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) (unique))))
(defn has-avet?
  ([^Attribute attr] (.hasAVET attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) has-avet?)))
(defn has-full-text?
  ([^Attribute attr] (.hasFullText attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) has-full-text?)))
(defn is-component?
  ([^Attribute attr] (.isComponent attr))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) is-component?)))
(defn ref-attr?
  ([attr] (= :db.type/ref (value-type attr)))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) ref-attr?)))
(defn card-many?
  ([attr] (= :db.cardinality/many (cardinality attr)))
  ([source attr-id] (->> attr-id (resolve-attribute-strict source) card-many?)))

(defn through-attr-bij [a] (get attr-bij a a))
(defn db-attr-map->Attribute* [attr-map]
  (pc/map-keys #(get attr-bij % %) attr-map))

(s/defn db-attr-map->Attribute [attr-map]
  (->> attr-map
       (pc/map-keys #(get attr-bij % %))
       map->Attribute))

(defn reverse-attr?
  "Does the keyword's name start with an underscore ?"
  [kw] (-> kw name (.charAt 0) (= \_)))

(defn rm_
  "Removes '_' from position 0 of keyword names, preserving namespace if defined."
  [kw]
  (assert (= (cstr/starts-with? "_" (name kw))))
  (keyword (namespace kw)
           (-> kw name (.substring 1))))

(defn rm_*
  "idempotent rm_"
  [kw]
  (if (reverse-attr? kw) (rm_ kw) kw))

(defn ref-attr?*
  "ref-attr?, but doesn't care if the attr is reversed with _"
  [db a]
  (or (and (reverse-attr? a)
           (ref-attr? db (rm_ a)))
      (ref-attr? db a)))
