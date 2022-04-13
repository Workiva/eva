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

(ns eva.v2.system.protocols)

;; Components

(defprotocol DatabaseConnection
  (db-snapshot [dbc])
  (log [dbc])
  (reload-if-stale [dbc]
    "Will check if the database connection's state has become stale. If it has,
     it will reload from storage.")
  (repair-and-reload [dbc]
    "Attempts to reload the database connection state from the backing store.
     Will repair the head of the transaction log if it is out of sync with the
     most recently persisted log entry. Mutates state, returns original connection.")
  (update-latest-index-roots
    [dbc index-updates]
    "Called by a message handler receives an index-update message.
     Mutates the database-connection by merging in the latest index-root updates.")
  (process-tx
    [dbc tx-data]
    "Validates a transaction.
     Returns tx-report containing: :log-entry, :db-before, :db-after, :tempids, :tx-data")
  (commit-tx
    [dbc tx-data]
    "Process a transaction, appending it to the tx-log if successful. Returns a tx-report")
  (advance-to-tx [dbc tx-num]
    "Advances the database connection to tx-num if it is not yet at at least tx-num.
     Assumes the persisted transaction log has been updated to >= tx-num.
     Returns the most recent database state, which will be at >= tx-num.")
  (handle-transaction-message [dbc tx-msg]
    "Handles a transaction message, updating local state and producing a transaction
     report containing :db-before :db-after :tempids and :tx-data"))

(defprotocol DatabaseConnectionState
  (advance-to-tx-num [dbcs tx-num]
    "Advances the database connection to tx-num if it is not yet at at least tx-num.
     Assumes the persisted transaction log has been updated to >= tx-num.
     Returns the most recent database state, which will be at >= tx-num.")
  (commit-transaction-report [dbcs tx-report] [dbcs tx-report timeout-ms]
    "Takes a transaction report containing :log-entry, :db-before, :db-after,
    :tempids, and :tx-data. Will write the log entry to the transaction log and
    return a new DatabaseConnectionState object with the updated transaction log,
    database snapshot, and index roots.")
  (update-index-roots [dbcs index-roots])
  (reload [dbcs])
  (repair-log [dbcs])
  (append-to-log [dbcs log-entry timeout-ms])
  (advance-db-from-log-entry [dbcs log-entry])
  (stale-snapshot? [dbcs db-snapshot]
    "Is the given snapshot stale wrt a fresh read of the transaction log state?"))

(defprotocol Transactor
  (process-transaction [txor tx-data])
  (process-index-updates [txor index-updates]))

(defprotocol Indexer
  (pending-updates? [is tx-log-entry]
    "If there are updated indexes that are not in the log entry, return them, else nil")
  (maybe-flush! [is]
    "If the staged indexes met the minimum criteria for flushing,
     flushes the staged index updates to persistent storage.")
  (advance-indexes! [is tx-log-entry]
    "Update the indexer state to at least the provided tx-log-entry")
  (process-entry [is tx-log-entry]
    "Advance the indexer state to include the tx-log-entry. If the indexes are
     flushed as a result, or if the indexer has pending updates, will return a
     map of updated index roots."))

;; Messaging

(defprotocol ResponderManager
  (open-responder! [mn addr f opts]
    "Starts a responder for addr using the function f with opts.

     Params:
      addr - string representing the queue to respond to.
      f    - a function that takes a single argument, the request object, and returns a response.
      opts - a map of options

     If the function f throws, the exception will be caught, logged, and returned to the requestor.")
  (close-responder! [mn addr]
    "Closes the responder registered for addr."))

(defprotocol RequestorManager
  (open-requestor! [mn addr opts]
    "Starts a requestor for addr with opts. Returns the requestor.")
  (close-requestor! [mn addr]
    "Closes the requestor registered for addr.")
  (request! [mn addr request-msg]
    "Submits request-msg to a responder consuming from addr.

     Returns a future that should eventually contain the response to the request."))

(defprotocol PublisherManager
  (open-publisher! [mn addr opts]
    "Starts a publisher to addr with opts. Returns the publisher.")
  (close-publisher! [mn addr]
    "Closes the publisher for addr")
  (publish! [mn addr publish-msg]
    "Submits publish-msg to all subscribers consuming from addr. Async by default."))

(defprotocol SubscriberManager
  (subscribe!   [mn subscriber-id addr f opts]
    "Starts a subscriber for addr using the function f with opts.

     Params:
      addr - string representing the queue to subscribe to.
      f    - a function that takes a single argument, the published object.
      otps - a map of options

     If the function f throws, the exception will be caught and logged.")
  (unsubscribe! [mn subscriber-id addr]
    "Closes the subscriber for address addr"))

(defprotocol ErrorListenerManager
  (register-error-listener [mn key f args])
  (unregister-error-listener [mn key]))
