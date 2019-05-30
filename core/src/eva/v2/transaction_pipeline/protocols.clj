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

(ns eva.v2.transaction-pipeline.protocols
    "Protocols used in transaction pipeline.")

(defprotocol TxReportProcessing
  (note-ref-attr [report ref-attr-eid])
  (note-tx-inst [report inst])
  (note-validated [report])
  (note-now [report now])
  (resolve-tx-fns [report] "Resolve all transaction functions to MapEntities, Adds, or Retracts")
  (flatten-maps [report] "Flatten all MapEntities to Adds")
  (resolve-ids [report] "Resolve the datoms contained in the report")
  (eliminate-redundancy [report])
  (validate [report] "Validate the fully-resolved transaction report, producing a db-after")
  (check-commands-value-types! [report] "Checks if value matches the type Adds/Retracts commands")
  (generate-tx-log-entry [report])
  (generate-old-signature-results [report]))

(defprotocol CoerceToCommand
  (coerce-to-command [o db] "Reifies explicit types for commands to add, retract, resolve map-entity, run a tx-fn"))
