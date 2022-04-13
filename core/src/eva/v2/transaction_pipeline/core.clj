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

(ns eva.v2.transaction-pipeline.core
  "Core logic for the transaction pipeline, provides the entry point for transaction
   processing via the 'transact' function"
  (:require [barometer.aspects :refer [timed]]
            [eva.core :refer [index-roots tx-num entry->datoms]]
            [eva.entity-id :as entity-id]
            [eva.error :refer [insist] :as err]
            [recide.sanex :as sanex]
            [eva.utils.logging :refer [logged]]
            [eva.v2.database.log :refer [map->TransactionLogEntry LOG_ENTRY_VERSION]]
            [eva.v2.transaction-pipeline.protocols :refer :all]
            [eva.v2.transaction-pipeline.type
             [basic :refer [->add ->retract add-op? retract-op? pack]]
             [tx-fn :refer [db-fn-op? ->tx-fn]]
             [map :refer [->map-entity]]]
            [eva.v2.transaction-pipeline.error :refer [raise-unrecognized-command] :as tp-err]
            [eva.v2.transaction-pipeline.resolve.maps :as rmaps]
            [eva.v2.transaction-pipeline.resolve.ids :as rids]
            [eva.v2.transaction-pipeline.resolve.tx-fns :as txfns]
            [eva.v2.transaction-pipeline.validation :as valid]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:import [java.util List Map]))

(def writer-id (memoize #(random-uuid)))

(defrecord TxReport [db-before db-after tempids tx-data meta]
  TxReportProcessing
  (note-ref-attr [report ref-attr-eid] (swap! meta update :ref-type-attrs (fnil conj #{}) ref-attr-eid) report)
  (note-tx-inst  [report inst] (swap! meta assoc :tx-inst inst) report)
  (note-validated [report] (swap! meta assoc :validated? true) report)
  (note-now [report now] (swap! meta assoc :now now) report)

  (resolve-tx-fns [report] (txfns/resolve-tx-fns report))
  (flatten-maps [report] (rmaps/resolve-map-entities report))
  (resolve-ids [report] (rids/resolve-ids report))
  (eliminate-redundancy [report] (rids/eliminate-redundancy report))
  (validate [report] (valid/validate report))
  (check-commands-value-types! [report] (valid/check-commands-value-types! report))
  (generate-tx-log-entry [_]
    (let [meta (deref meta)]
      (insist (:validated? meta) "Cannot generate a tx-log-entry from an unvalidated report.")
      (let [index-roots (index-roots db-before)
            id-info @entity-id/*id-info*
            tx-inst-cmd (:tx-inst meta)
            tx-eid (:e tx-inst-cmd)
            tx-inst (:v tx-inst-cmd)
            novelty (mapv pack tx-data)
            tx-log-entry (map->TransactionLogEntry
                          {:index-roots index-roots
                           :cur-tx-eid tx-eid
                           :cur-max-id (:max-id id-info)
                           :tx-inst tx-inst
                           :writer-id (writer-id)
                           :tx-num (inc (tx-num db-before))
                           :ref-type-attrs (:ref-type-attrs meta)
                           :version LOG_ENTRY_VERSION
                           :novelty novelty})]
        tx-log-entry)))
  (generate-old-signature-results [report]
    (let [tx-log-entry (generate-tx-log-entry report)]
      {:log-entry tx-log-entry
       :tx-data (entry->datoms tx-log-entry)
       :successful true
       :tempids tempids})))

(extend-protocol CoerceToCommand
  Map
  (coerce-to-command [m db] (->map-entity db m))
  List
  (coerce-to-command [l db]
    (let [fst (first l)]
      (cond
        (add-op? fst)      (->add db l)
        (retract-op? fst)  (->retract db l)
        (db-fn-op? db fst) (->tx-fn db l)
        :else (raise-unrecognized-command
               "received list, but its first element should be :db/add, :db/retract, or a transaction function."
               {:unrecognized-command l
                ::sanex/sanitary? false}))))
  Object
  (coerce-to-command [o _]
    (raise-unrecognized-command
     "received invalid object. Expected a List command or a Map entity."
     {:unrecognized-command o
      ::sanex/sanitary? false})))

(defn realize-as-command [db o] (coerce-to-command o db))

(d/defn ^{::d/aspects [traced]} init-report [db tx-data]
  (when-not (instance? List tx-data)
    (raise-unrecognized-command
     "data submitted for transaction must be a List."
     {:unrecognized-command tx-data,
      ::sanex/sanitary? false}))
  (map->TxReport
   {:db-before db
    :tx-data   (into [] (map (partial realize-as-command db)) tx-data)
    :meta (atom {})}))

(defn tx-result
  "Given transaction info, will return the transaction result if the transaction
   succeeded, else nil."
  [tx-info] (::tx-result tx-info))

(defn tx-exception
  "Given transaction info, will return the exception that caused failure, else nil"
  [tx-info] (::exception tx-info))

(defn successful-tx-info [res] {::tx-result res})
(defn exceptional-tx-info [res] {::exception res})

(d/defn ^{::d/aspects [(logged) traced timed]} transact
  "Returns a result object capturing either success or failure of the given transaction.
   Can be inspected via 'tx-result' and 'tx-exception'

   Essentially -- returns an exception fauxnad for success/failure of the transaction."
  [db tx-data]
  ;; TODO: Simplify id generation and handling
  (try
    (binding [entity-id/*id-info* (atom (-> entity-id/id-info-base
                                            (assoc :tx-n (- entity-id/max-e))))]
      (successful-tx-info (->> (init-report db tx-data)
                               resolve-tx-fns
                               flatten-maps
                               resolve-ids
                               check-commands-value-types!
                               eliminate-redundancy
                               validate
                               generate-old-signature-results)))
    (catch Throwable t
      (->> t
           (err/override-codes tp-err/error-codes)
           (err/handle-api-error)
           (exceptional-tx-info)))))
