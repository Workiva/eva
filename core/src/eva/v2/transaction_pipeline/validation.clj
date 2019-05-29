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

(ns eva.v2.transaction-pipeline.validation
  "Provides the validation logic for a finalized batch of tx-data."
  (:require [eva.v2.transaction-pipeline.type.basic :refer [add? retract? ->Add]]
            [eva.v2.transaction-pipeline.protocols :refer [note-ref-attr note-tx-inst note-validated note-now]]
            [eva.v2.transaction-pipeline.error :as tx-err]
            [utiliva.core :refer [group-by]]
            [eva.entity-id :refer [permify-id tempid] :as entid]
            [eva.functions :refer [compile-db-fn]]
            [eva.attribute :as ea]
            [eva.error :refer [raise insist]]
            [eva.value-types :refer [valid-value-type?]]
            [recide.sanex :as sanex]
            [eva.config :refer [config-strict config]]
            [recide.sanex.logging :as logging]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:import [java.util Date])
  (:refer-clojure :exclude [group-by]))

(defprotocol TxDataAccess
  (by-attr      [tx-data] "Access the tx-data by attribute. Performs non-thread-safe caching.")
  (by-eid       [tx-data] "Access the tx-data by eid. Performs non-thread-safe caching."))

(deftype TxData [data ^:volatile-mutable _by-attr ^:volatile-mutable _by-eid]
  TxDataAccess
  (by-attr [_]
    (if-not (nil? _by-attr)
      _by-attr
      (let [grouped (group-by :attr data)]
        (set! _by-attr grouped)
        grouped)))
  (by-eid [_]
    (if-not (nil? _by-eid)
      _by-eid
      (let [grouped (group-by :e data)]
        (set! _by-eid grouped)
        grouped))))

(defn ->tx-data [report] (TxData. (:tx-data report) nil nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Generic Attribute Validation

(defn validate-cardinality-one [attr cmds]
  (when-let [violators
             (->> cmds
                  (group-by (filter add?) :e)
                  (some #(when (> (count (val %)) 1) (val %))))]
    (raise :transact-exception/cardinality-one-violation
           (format "Cannot add multiple values for cardinality one attribute" violators)
           {:attribute        (into {} attr)
            :tx-data          violators
            ::sanex/sanitary? false})))

(defn validate-uniqueness [attr cmds]
  (when (and (not (empty? cmds))
             (not (apply distinct? (map :v cmds))))
    (raise :transact-exception/duplicate-unique-entities
           "Cannot create duplicate unique entities with the same attribute and value but different eids."
           {:attribute        (into {} attr)
            :tx-data          (seq cmds)
            ::sanex/sanitary? false})))

(defn validate-db-fn [attr cmds]
  (when (config-strict :eva.transaction-pipeline.compile-db-fns)
    (doseq [cmd cmds :let [v (:v cmd)]]
      (try
        (compile-db-fn v)
        (catch Throwable t
          (raise :transact-exception/cannot-compile-db-fn
                 "Attempted to compile database function and failed."
                 {:attribute        (into {} attr)
                  :fn               v
                  ::sanex/sanitary? true}
                 t))))))

(defn attr-bytes-size-fn [attr]
  (case (ea/value-type attr)
    :db.type/string (fn [^String s] (alength (.getBytes s "UTF-8")))
    :db.type/bytes (fn [^eva.bytes.BBA b] (alength ^bytes (.ba b)))
    nil))

(defn validate-bytes-limits [attr cmds]
  (when-let [violating-attr-size (->> cmds
                                      (map :v)
                                      (map (attr-bytes-size-fn attr))
                                      (filter #(>= % (config :eva.transaction-pipeline.byte-size-limit)))
                                      first)]
    (if (config :eva.transaction-pipeline.limit-byte-sizes)
      (raise :transact-exception/value-too-large
           (format "Transaction contains a %s value that is %d bytes, but that attribute limits values to %d bytes."
                   (ea/value-type attr)
                   violating-attr-size
                   (config :eva.transaction-pipeline.byte-size-limit))
           {:attribute (into {} attr)
            :tx-data cmds
            ::sanex/sanitary? false})
      (logging/warnf "%s will soon be limited to %d bytes, but an item of size %d was found."
                     (ea/value-type attr)
                     (config :eva.transaction-pipeline.byte-size-limit)
                     violating-attr-size))))

(defn generic-validation [attr report tx-data cmds]
  (when (ea/unique attr)                              (validate-uniqueness attr cmds))
  (when (= :db.cardinality/one (ea/cardinality attr)) (validate-cardinality-one attr cmds))
  (when (= :db.type/fn         (ea/value-type  attr)) (validate-db-fn attr cmds))
  (when (= :db.type/ref        (ea/value-type  attr)) (note-ref-attr report (ea/id attr)))
  (when (attr-bytes-size-fn attr)                     (validate-bytes-limits attr cmds)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Attribute-Specific Validation

(defn ->missing-attrs
  "Given the collection of cmds and the collection of attr-idents return the
   seq of attr-idents that are *not* represented in the cmds."
  [cmds attr-idents]
  (seq (filter
        (complement
         (into #{} (for [c cmds] (ea/ident (:attr c)))))
        attr-idents)))

(defmulti validate-attribute
  "Validates the set of commands with the given attribute against the db-before,
   modifies the report and db-after as appropriate to process the transaction."
  (fn [attr report tx-data cmds] (ea/ident attr))
  :default "default")

(defn ensure-no-retractions [cmds]
  (when-let [retract (some #(when (retract? %) %) cmds)]
    (raise :transact-exception/cannot-modify-schema
           (format "Cannot process retraction on already-installed schema: %s" (str retract))
           {:tx-data (str retract) ::sanex/sanitary? false})))

(defn ensure-installing-attribute [tx-data cmd]
  (let [eid (:e cmd)
        eid-cmds (get (by-eid tx-data) 0)]
    (when (empty? (filter
                   (fn check-is-installing [install-cmd]
                     (and (add? install-cmd)
                          (= eid (:v install-cmd))
                          (= :db.install/attribute (ea/ident (:attr install-cmd)))))
                   eid-cmds))
      (raise :transact-exception/no-corresponding-attribute-installation
             (format "Cannot process command %s without corresponding :db.install/attribute" cmd)
             {:tx-data cmd
              ::sanex/sanitary? false}))))

(defmethod validate-attribute :db/valueType [attr report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds]
    (ensure-installing-attribute tx-data cmd)))

(defmethod validate-attribute :db/cardinality [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds]
    (ensure-installing-attribute tx-data cmd)))

(defmethod validate-attribute :db/unique [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds]
    (ensure-installing-attribute tx-data cmd)))

(defmethod validate-attribute :db/isComponent [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds]
    (ensure-installing-attribute tx-data cmd)))

;; NOTE: the following are properties inherent to the default database schema in
;;       /core/src/eva/utils.clj
(def ^:dynamic *byte-type-eid* 27)
(def ^:dynamic *tx-partition* 1)
(def ^:dynamic *card-many* 36)

(defn ->prev-tx-inst [report] (-> report :db-before :log-entry :tx-inst))
(defn ->now [report] (-> report :meta deref :now))

(defmethod validate-attribute :db/txInstant [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (when-not (= 1 (count cmds))
    (tx-err/raise-invalid-tx-inst
     "you cannot install two txInstants or modify another transaction's txInstant"
     {:cmds cmds ::sanex/sanitary? false}))
  (when-not (and (= *tx-partition* (entid/partition (:e (first cmds)))))
    (tx-err/raise-invalid-tx-inst
     "tx-instants can only be installed on the new reified transaction-entity"
     {::sanex/sanitary? true}))
  (let [^Date prev-tx-inst (->prev-tx-inst report)
        ^Date new-tx-inst (-> cmds first :v)
        ^Date now (->now report)]
    (when-not (<= (.getTime prev-tx-inst)
                  (.getTime new-tx-inst)
                  (.getTime now))
      (tx-err/raise-invalid-tx-inst
       "provided :db/txInstant must be between the previous log entry :db/txInstant and now"
       {:prev-tx-inst prev-tx-inst
        :new-tx-inst new-tx-inst
        :now now
        ::sanex/sanitary? true}))
    (note-tx-inst report (first cmds))))

(defn reserved-namespace? [nspace]
  (when nspace
    (let [nspace-prefix (clojure.string/split nspace #"\.")]
      (contains? #{"db"} (first nspace-prefix)))))

(defmethod validate-attribute :db/ident
  [_ report tx-data cmds]
  (doseq [cmd cmds
          :let [nspace (namespace (:v cmd))]]
    (when (reserved-namespace? nspace)
      (raise :transact-exception/reserved-namespace
             (format "Cannot process command %s. The 'db' namespace idents are reserved and cannot be modified." cmd)
             {:tx-data cmd ::sanex/sanitary? false}))))

(defn byte-type-and-unique? [ops]
  (let [value-type (some #(when (= :db/valueType
                                   (ea/ident (:attr %)))
                            (:v %))
                         ops)
        unique (some #(when (= :db/unique
                               (ea/ident (:attr %)))
                        (:v %))
                     ops)]
    (and unique (= value-type *byte-type-eid*))))

(defn byte-type? [ops]
  (let [value-type (some #(when (= :db/valueType
                                   (ea/ident (:attr %)))
                            (:v %))
                         ops)]
    (= value-type *byte-type-eid*)))

(defn card-many-and-unique? [ops]
  (let [cardinality (some #(when (= :db/cardinality
                                    (ea/ident (:attr %)))
                             (:v %))
                          ops)
        unique (some #(when (= :db/unique
                               (ea/ident (:attr %)))
                        (:v %))
                     ops)]
    (and unique (= cardinality *card-many*))))

(defn leading-underscore-ident? [ops]
  (let [ident (some #(when (= :db/ident (ea/ident (:attr %))) (:v %)) ops)]
    (= \_ (first (name ident)))))

(defmethod validate-attribute :db.install/attribute
  [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds
          :let [ops (get (by-eid tx-data) (:v cmd))]]
    (if-let [missing (->missing-attrs ops [:db/valueType :db/cardinality :db/ident])]
      (raise :transact-exception/incomplete-install-attribute
             "Cannot install incomplete attribute; missing required schema attributes"
             {:tx-data (seq ops)
              :missing missing
              ::sanex/sanitary? false})
      (do (when (leading-underscore-ident? ops)
            (raise :transact-exception/invalid-attribute-ident
                   "Cannot install attribute with an ident name that has a leading underscore"
                   {:tx-data (seq ops) ::sanex/sanitary? false}))
          (when (card-many-and-unique? ops)
            (raise :transact-exception/incompatible-attribute-properties
                   "Cannot install cardinality-many unique attributes"
                   {:tx-data (seq ops) ::sanex/sanitary? false}))
          (when (byte-type? ops)
            (raise :transact-exception/incompatible-attribute-properties
                   "Byte-typed attributes are not yet supported"
                   {:tx-data (seq ops) ::sanex/sanitary? false}))
          (when (byte-type-and-unique? ops)
            (raise :transact-exception/incompatible-attribute-properties
                   "Cannot install unique byte-typed attributes"
                   {:tx-data (seq ops) ::sanex/sanitary? false}))))))

(defmethod validate-attribute :db.install/partition
  [_ report tx-data cmds]
  (ensure-no-retractions cmds)
  (doseq [cmd cmds
          :let [ops (get (by-eid tx-data) (:v cmd))]]
    (when-let [missing (->missing-attrs ops [:db/ident])]
      (raise :transact-exception/incomplete-install-partition
             "Cannot install incomplete partition; missing required attributes"
             {:tx-data (seq ops)
              :missing missing
              ::sanex/sanitary? false}))))

(defmethod validate-attribute :db/noHistory [_ report tx-data cmds]
  (raise :transact-exception/noHistory-NYI
         "The :db/noHistory attribute property is not yet implemented."
         {:tx-data cmds
          ::sanex/sanitary? false}))

(defmethod validate-attribute :db/fulltext [_ report tx-data cmds]
  (raise :transact-exception/fulltext-NYI
         "The :db/fulltext attribute property is not yet implemented."
         {:tx-data cmds
          ::sanex/sanitary? false}))

(defmethod validate-attribute :db.install/valueType [_ report tx-data cmds]
  (raise :transact-exception/install-valueType-NYI
         "The :db.install/valueType attribute is not yet implemented."
         {:tx-data cmds
          ::sanex/sanitary? false}))

(defmethod validate-attribute "default" [attr report tx-data cmds] true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core

(defn generate-tx-inst-cmd [report]
  (let [tx-inst-attr (ea/resolve-attribute (:db-before report) :db/txInstant)
        now (->now report)
        e (permify-id (tempid :db.part/tx))]
    (->Add e (ea/id tx-inst-attr) now tx-inst-attr)))

(defn within-skew?
  "Is the (+ skew-window later) date >= the prior date?"
  [^Date later ^Date prior skew-window]
  (>= (+ (.getTime later) skew-window)
      (.getTime prior)))

(defn derive-global-now [failure-fn
                         ^Date local-now
                         ^Date last-tx-inst
                         ^Long skew-window]
  (if (>= (.getTime local-now) (.getTime last-tx-inst))
    local-now
    (if (and (some? skew-window)
             (within-skew? local-now last-tx-inst skew-window))
      (do (logging/warnf "transactor is re-using previous tx-inst since current clock is behind: %s %s %s" local-now last-tx-inst skew-window)
        last-tx-inst)
      (failure-fn local-now last-tx-inst skew-window))))

;; alternate strategy: system/exit
(defn raise-skew-exception [local-now last-tx-inst skew-window]
  (tx-err/raise-clock-skew
   "This transactor's clock is more than the allotted skew window behind the previous transaction log entry. Please report this exception to the Eva team."
   {:local-now local-now
    :last-tx-inst last-tx-inst
    :skew-window skew-window
    ::sanex/sanitary? true}))

(defn current-time [] (java.util.Date.))

(d/defn ^{::d/aspects [traced]} validate
  "Given a report which has fully expanded, resolved ids, and eliminated
   redundancy, validate the set of changes against the :db-before."
  ([report]
   (let [tx-inst? (atom nil)
         prev-tx-inst (->prev-tx-inst report)
         now (derive-global-now raise-skew-exception
                                (current-time)
                                prev-tx-inst
                                (config :eva.transaction-pipeline.clock-skew-window))
         report (note-now report now)]
     (loop [tx-data (->tx-data report)
            [[attr cmds :as head] & rest] (seq (by-attr tx-data))]
       ;; we've validated all attributes
       (if (nil? head)
         ;; if we've validated a user-defined tx-inst we can return the report directly.
         (if-let [tx-inst (-> report :meta deref :tx-inst)]
           (note-validated report)
           ;; if we haven't, we must generate one and add it.
           (let [tx-inst-cmd (generate-tx-inst-cmd report)]
             (validate (:attr tx-inst-cmd) report tx-data [tx-inst-cmd])
             (note-validated (update report :tx-data conj tx-inst-cmd))))
         ;; validate all datoms under this attribute and recur.
         (do (validate attr report tx-data cmds)
             (recur tx-data rest))))))
  ([attr report tx-data cmds]
   (validate-attribute attr report tx-data cmds)
   (generic-validation attr report tx-data cmds)))

(defn- raise-if-not-valid!
  [command]
  (let [attr (:attr command)
        vt (ea/value-type attr)
        v' (:v command)]
    (if-not (valid-value-type? vt v')
      (raise :transact-exception/incorrect-type
             "Wrong value type for attribute"
             {:command command
              ::sanex/sanitary? false
              :attribute (ea/ident attr)
              :expected vt
              ;; TODO: received type is lie - originally supplied value could
              ;; be casted by `eva.v2.transaction-pipeline.type.basic/cast-value`
              ;; Probably the lie is not very big (?) since (type v') would differ
              ;; from oringal value only if attribute is a reference and value is not
              ;; an integer or lookup-ref (then what it is?)
              :received (type v')}))))

(d/defn ^{::d/aspects [traced]} check-commands-value-types!
  "Given a report which has only Adds/Retracts it does type
   checks for their values: (i.e :db.type/long attribute
   cannot have value of type string)"
  [report]
  (doall (for [command (:tx-data report)]
           (raise-if-not-valid! command)))
  report)
