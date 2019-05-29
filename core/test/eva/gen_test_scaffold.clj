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

(ns eva.gen-test-scaffold
  (:require [clojure.test.check.generators :as gen]
            [clojure.data.generators :as cg]
            [eva.core :refer [select-datoms]]
            [eva.bytes :refer [bbyte-array]]
            [eva.attribute :as attr]
            [eva.entity-id :as entity-id]
            [plumbing.core :as pc]))

(def ^:dynamic *n* (atom 0))
;; Given a db instance create...

;; ... a new ident
;; ... a new tempid

(defn ->tempid-without-n-gen [parts]
  (gen/fmap entity-id/tempid (gen/elements parts)))

(defn tagged-tempid [db]
  (entity-id/tempid (rand-nth
                     (flatten (seq (dissoc (:parts db) :db.part/db :db.part/tx))))
                    (swap! *n* dec)))

(def gen-cardinality (gen/elements [:db.cardinality/many
                                    :db.cardinality/one]))
(def gen-value-type (gen/elements [:db.type/boolean
                                   :db.type/instant
                                   :db.type/bytes
                                   :db.type/uri
                                   :db.type/uuid
                                   :db.type/string
                                   :db.type/keyword
                                   :db.type/bigdec
                                   :db.type/float
                                   :db.type/bigint
                                   :db.type/double
                                   :db.type/long]))

(def gen-unique (gen/elements [:db.unique/identity
                               :db.unique/value]))
(def gen-doc gen/string)
(def gen-ident gen/keyword-ns)
(def gen-db-part (gen/elements [0 :db.part/db]))

(defn rand-eid-fn [db]
  (->> (select-datoms db [:eavt])
       (map (memfn e))
       dedupe
       rand-nth))

;; Generated values, sans ref, should not
;; affect the generative testing.  We will create
;; them explicity here
(defn vt->val-gen-fn [db value-type]
  (case value-type
    :db.type/instant cg/date
    :db.type/boolean cg/boolean
    :db.type/bytes   #(bbyte-array (cg/byte-array cg/byte))
    :db.type/uri     #(java.net.URI. (str "urn:uuid:" (cg/uuid)))
    :db.type/uuid    cg/uuid
    :db.type/string  cg/string
    :db.type/keyword #(rand-nth (gen/sample gen/keyword-ns))
    :db.type/ref     (partial rand-eid-fn db)
    :db.type/bigdec  cg/bigdec
    :db.type/float   cg/float
    :db.type/bigint  cg/bigint
    :db.type/double  cg/double
    :db.type/long    cg/long
    ;; TODO: See if there's a not-entirely-awful way of doing this.
    :db.type/fn      nil))

;; NOTE: we do not need the :db/index     attribute; we assume it to be true
;;       we do not support  :db/noHistory attribute yet
;;       we do not support  :db/fulltext  attribute yet
;; TODO: byte+unique is invalid
(defn ->install-attr-map
  [dbpart tempid ident value-type cardinality]
  {:db.install/_attribute dbpart
   :db/id tempid
   :db/ident ident
   :db/valueType value-type
   :db/cardinality cardinality})

(def req-attribute-gen
  (gen/fmap (partial apply ->install-attr-map)
            (gen/tuple
             gen-db-part
             (->tempid-without-n-gen [:db.part/db 0])
             gen-ident
             gen-value-type
             gen-cardinality)))

(defn add-optional-attributes
  [m doc unique component]
  (cond-> m
    (some? doc)       (assoc :db/doc doc)
    (some? unique)    (assoc :db/unique unique)
    (some? component) (assoc :db/isComponent component)))

(defn sanitize-attr
  [m]
  (let [unique (:db/unique m)
        vt (:db/valueType m)]
    (condp = [unique vt]
      ;; NOTE: we eliminate unique/value type/boolean
      ;;       not because it's invalid, but because it makes
      ;;       the testing throw exceptions
      [:db.unique/value :db.type/boolean]
      (assoc m :db/unique :db.unique/identity)

      ;; TODO: refuse to create byte-type unique attrs
      [:db.unique/value :db.type/bytes]
      (assoc m :db/valueType :db.type/long)

      [:db.unique/identity :db.type/bytes]
      (assoc m :db/valueType :db.type/long)

      ;; else
      m)))

(def attribute-gen
  (gen/fmap
   sanitize-attr
   (gen/fmap (partial apply add-optional-attributes)
             (gen/tuple
              req-attribute-gen
              (gen/frequency [[1 gen-doc]
                              [1 (gen/return nil)]])
              (gen/frequency [[1 gen-unique]
                              [1 (gen/return nil)]])
              (gen/frequency [[1 (gen/return true)]
                              [1 (gen/return nil)]])))))

(defn ->install-part-map [dbpart tempid ident]
  {:db.install/_partition dbpart
   :db/id tempid
   :db/ident ident})

(def partition-gen
  (gen/fmap (partial apply ->install-part-map)
            (gen/tuple
             gen-db-part
             (->tempid-without-n-gen [:db.part/db 0])
             gen-ident)))

(defn attr->new-datom [db attr]
  (let [vt (attr/value-type attr)
        val-fn (vt->val-gen-fn db vt)
        ident (attr/ident attr)
        card (attr/cardinality attr)]
    (cond
      (= ident :db.install/attribute)
      [(rand-nth (gen/sample attribute-gen))]

      (= ident :db.install/partition)
      [(rand-nth (gen/sample partition-gen))]

      ;; NOTE: No-oping for now
      (= vt :db.type/fn)
      [[]]

      ;; Blacklist attributes that don't make a lot of sense in isolation
      (contains? #{} ident)
      [[]]

      (= card :db.cardinality/many)
      (rand-nth
       [[[:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]]
        [[:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]
         [:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]]
        [[:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]
         [:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]
         [:db/add
          (tagged-tempid db)
          (attr/ident attr)
          (val-fn)]]])

      :else
      [[:db/add
        (tagged-tempid db)
        (attr/ident attr)
        (val-fn)]])))

;; cases testing should cover:
;; install attribute
;; install partition
;; :db/fn attr
;; cardinality many attr
;; unique value
;; unique identity

;; constraints that should be tested:
;; vt bytes (ie at least implies !unique)

;; TODO: schema alternation impl & testing:
;; NOTE: lots of this goes away with AVET being always-on
(def blacklist-attrs
  [:db/valueType
   :db/cardinality
   :db/unique
   :db/isComponent
   :db/noHistory
   :db/fulltext
   :db/fn
   :db/txInstant
   :db/ident
   :db.install/valueType])

(defn blacklist-ids [db]
  (map (:idents db) blacklist-attrs))

(defn gen-attribute-selection [db]
  (gen/vector (gen/elements (vals (reduce dissoc (:attrs db) (blacklist-ids db))))
              1 10))

;; Given a database, create a generator for transaction payloads.
(defn ->tx-gen [db]
  (gen/not-empty (gen/fmap (pc/fn->>
                            (map (partial  attr->new-datom db))
                            (reduce concat)
                            (remove empty?))
                           (gen-attribute-selection db))))
