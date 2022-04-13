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

(ns eva.db-update
  "Higher level api functions for working with databases")

(defprotocol UpdatableDB
  (safe-advance-db [db entry]
    "Given a transaction log entry, advances the database to a state where the (already validated) tx has been applied. Requires non-speculative log entries. If the log-entry indicates that the indexes have flushed, will (re)load the database state with the new indexes from the backing store.")
  (speculatively-advance-db [db speculative-log-entry]
    "Create a new fully-local database snapshot based on a speculative log entry.")
  (advance-db* [db entry]
    "Update the database state based on the entry. Agnostic to whether or not the log entry is speculative.")
  (advance-db-to-tx [db tx-log target-tx-num]
    "Given the log and a transaction number advance the database to the target tx.")
  (flush-overlay [db tx-num]
    "Returns a new db instance with all datoms <= tx-num removed from the overlay."))
