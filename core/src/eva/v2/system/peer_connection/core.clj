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

(ns eva.v2.system.peer-connection.core
  (:require [eva.v2.messaging.node.manager.alpha :as messenger-node]
            [eva.v2.messaging.node.manager.types :as messenger]
            [eva.v2.storage.system :as storage]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.database.core :as database]
            [eva.v2.database.log :as db-log]
            [eva.v2.messaging.address :as address]
            [eva.v2.system.protocols :as p]
            [eva.v2.system.database-connection.core :as dbc-core]
            [eva.v2.utils.spec :refer [conform-spec]]
            [eva.v2.utils.completable-future :as cf]
            [eva.config :as config]
            [eva.core :as core]
            [tesserae.core :as tess]
            [utiliva.uuid :refer [squuid]]
            [quartermaster.core :as qu]
            [eva.error :as err]
            [eva.utils.logging :refer [logged]]
            [recide.core :refer [try*] :as rec]
            [recide.sanex.logging :as log]
            [eva.v2.system.peer-connection.error :as peerr]
            [barometer.aspects :refer [timed]]
            [eva.print-ext]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as c])
  (:import [java.util.function Function]
           [java.util.concurrent CompletableFuture TimeUnit TimeoutException]
           [clojure.lang IExceptionInfo]
           [org.apache.activemq.artemis.api.core ActiveMQException]
           [javax.jms JMSRuntimeException]
           [eva.error.v1 EvaException EvaErrorCode]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::id uuid?)

(s/def ::config
  (s/merge ::storage/config
           ::messenger/config
           (s/keys :req [::id
                         ::address/transaction-submission
                         ::address/transaction-publication
                         ::database/id])))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(def ^:dynamic *default-peer-id* (squuid))

(d/defn ^{::d/aspects [traced (logged) timed]} handle-published-tx
  [{:as peer-connection :keys [database-connection database-id]}
   {:as pub-tx-msg :keys [tx-num database-id]}]
  (if (= database-id (:database-id pub-tx-msg))
    (p/advance-to-tx @database-connection tx-num)
    (log/warn "connection for %s ignoring call to advance to %s for %s"
              database-id
              tx-num
              (:database-id pub-tx-msg))))

(defn ^Function java-func [f] (reify Function (apply [_ x] (f x))))

(defn ensure-started
  "Does the same as qu/ensure-initialized! but has its own EvaException & error code."
  [conn]
  (when (qu/terminated? conn)
    (throw (EvaException. EvaErrorCode/CONNECTION_INACTIVE
                          {:peer-id (:peer-id conn)}))))

(defn- onboard-messenger
  [{:as conn :keys [transaction-addr transaction-pub-addr peer-id]}
   messenger]
  (let [messenger @messenger]
    (p/open-requestor! messenger transaction-addr {})
    (p/subscribe! messenger peer-id transaction-pub-addr (partial handle-published-tx conn) {})))

(d/defn ^{::d/aspects [traced (logged) timed]} transactAsync-impl
  [{:as peer-connection
    :keys [config messenger transaction-addr database-id database-connection peer-id]}
   tx-data]
  (try (let [response-promise (p/request! @messenger transaction-addr {:tx-data tx-data
                                                                       :database-id database-id})
             result (cf/promise)]
         (future (try (let [res @response-promise] ;; TODO: -- lazy-chaining better; eager future risks thread leak
                        (try (cf/deliver result (p/handle-transaction-message @database-connection res))
                             (catch Throwable t
                               (log/error t
                                          "Caught unknown exception while handling background transaction result")
                               (cf/deliver result t))))
                      (catch Throwable t
                        (cf/deliver result (.getCause t)))))
         result)
       (catch Throwable e
         (let [root-cause (err/root-cause e)]
           (cond (instance? ActiveMQException root-cause)
                 (throw (EvaException. EvaErrorCode/TRANSACTOR_CONNECTION_FAILURE
                                       {:peer-id peer-id
                                        :database-id database-id}
                                       root-cause))
                 (instance? JMSRuntimeException root-cause)
                 (throw (EvaException. EvaErrorCode/TRANSACTOR_CONNECTION_FAILURE
                                       {:peer-id peer-id
                                        :database-id database-id}
                                       root-cause))
                 (instance? IExceptionInfo e)
                 (throw (cond (err/error? e)  ;; eva errors can be thrown.
                              e
                              :else ;; non-eva exceptioninfo errors should be wrapped.
                              (EvaException. EvaErrorCode/UNKNOWN_ERROR
                                             {:peer-id peer-id
                                              :database-id database-id}
                                             e)))
                 :else
                 (throw (EvaException. EvaErrorCode/UNKNOWN_ERROR
                                       {:peer-id peer-id
                                        :database-id database-id}
                                       e)))))))

(defrecord Connection [config
                       resource-id
                       database-id
                       database-connection
                       peer-id
                       transaction-addr
                       transaction-pub-addr
                       messenger
                       release-fn]
  qu/SharedResource
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [resource-id (qu/new-resource-id)
                     db-connection (qu/acquire dbc-core/database-connection-manager resource-id config)
                     initiated-self (assoc this
                                           :resource-id (atom resource-id)
                                           :database-connection db-connection)
                     messenger (qu/acquire messenger-node/messenger-nodes resource-id config (partial onboard-messenger initiated-self))]
                    (try
                      ;; NOTE: there's a small race window where the peer can have initialized its database state,
                      ;;       but miss new transactions from the transactor. This closes that window.
                      ;; TODO: This could be optimized a bit by treating the db-connection resource as a
                      ;;       'promise' that has not been satisfied until its needed.
                      (p/reload-if-stale @db-connection)
                      (assoc initiated-self :messenger messenger)
                      (catch Throwable t
                        (try ;; TODO: this can be improved
                          (qu/release db-connection)
                          (finally
                            (qu/release messenger true)))
                        (throw t))))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (do (let [res-id (qu/resource-id this)]
            (qu/release database-connection true)
            (reset! resource-id nil)
            (try (p/close-requestor! @messenger transaction-addr)
                 (p/unsubscribe! @messenger peer-id transaction-pub-addr)
                 (catch Throwable t)) ;; It's an unrelated problem
            (qu/release messenger true)
            (assoc this
                   :database-connection nil
                   :messenger nil)))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do (qu/force-terminate database-connection)
          (qu/force-terminate messenger)
          (qu/terminate this))))
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  eva.Connection
  (db [this]
    (err/with-api-error-handling
      (ensure-started this)
      (p/db-snapshot @database-connection)))
  (dbSnapshot [this]
    (err/with-api-error-handling
      (ensure-started this)
      (p/db-snapshot @database-connection)))
  (syncDb [this]
    (err/with-api-error-handling
      (ensure-started this)
      (p/reload-if-stale @database-connection)))
  ;; Wraps our internal log object to eliminate direct interface accesses.
  (log [this]
    (err/with-api-error-handling
      (ensure-started this)
      (db-log/->PersistedVectorLog (p/log @database-connection))))
  (latestT [this]
    (err/with-api-error-handling
      (ensure-started this)
      (-> ^eva.Database (p/db-snapshot @database-connection) (.basisT))))
  (release [this]
    (err/with-api-error-handling
      (release-fn this true)
      (assoc this
             :database-connection nil
             :messenger nil
             :resource-id nil)))
  (transact [this tx-data]
    (err/with-api-error-handling
      (ensure-started this)
      (let [res (.transactAsync this tx-data)]
        (try
          (.get res (config/config-strict :eva.transact-timeout) TimeUnit/MILLISECONDS)
          ;; return future
          res
          ;; rethrow timeout-exception immediately
          (catch TimeoutException e
            (peerr/raise-timeout :transact
                                 "aborting transact."
                                 {:timeout-ms (config/config-strict :eva.transact-timeout)
                                  :tx-data-size (count tx-data)}))
          ;; otherwise return future to be derefed
          (catch Throwable e res)))))
  ;; TODO: re-inline this by adding options to the registration of the message handlers
  ;;       to inject timing / tracing / logging
  (transactAsync [this tx-data]
    (err/with-api-error-handling
      (ensure-started this)
      (transactAsync-impl this tx-data))))

(defn- config->peer-id [c] (or (::id c) *default-peer-id*))

(defn connect* [{:as config}]
  {:pre [(conform-spec ::config config)]}
  (let [peer-id (config->peer-id config)
        database-id (::database/id config)
        transaction-addr (::address/transaction-submission config)
        transaction-pub-addr (::address/transaction-publication config)
        release-fn (::release-fn config)] ;; added by connection manager
    (try
      (map->Connection
       {:config config
        :database-id database-id
        :peer-id peer-id
        :transaction-addr transaction-addr
        :transaction-pub-addr transaction-pub-addr
        :release-fn release-fn})
      (catch Throwable e
        (let [root-cause (err/root-cause e)]
          (cond (or (instance? ActiveMQException root-cause)
                    (instance? JMSRuntimeException root-cause))
                (peerr/raise-connect-failure :message-queue
                                             "messaging component threw an exception."
                                             {:peer-id peer-id
                                              :database-id database-id}
                                             root-cause)
                (rec/error? e ::qu/*)
                (peerr/raise-connect-failure :resource-manager
                                             "failed to acquire an internal resource."
                                             {:peer-id peer-id
                                              :database-id database-id}
                                             e)
                (err/error? e) ;; eva errors can be thrown
                (throw e)
                :else
                (peerr/raise-connect-failure :unrecognized-cause
                                             "connect* encountered an unrecognized error."
                                             {:peer-id peer-id
                                              :database-id database-id}
                                             e)))))))

(qu/defmanager peer-connection-manager
  :discriminator
  (fn [_ config]
    [(::database/id config) (::values/partition-id config)])
  :constructor
  (fn [_ config]
    (log/info "Connecting to database with id" (:eva.v2.database.core/id config))
    (-> config
        (assoc ::release-fn (qu/auto-releaser config))
        (connect*))))

(d/defn ^{::d/aspects [traced (logged) timed]} connect [config]
  (assert (::database/id config))
  (try*
   @(qu/acquire peer-connection-manager :peer-connect config)
   (catch ::qu/* e
     (if (-> ^Throwable e (.getCause) (err/error? :peer-connect/*))
       (throw (.getCause e))
       (peerr/raise-connect-failure :resource-manager
                                    "peer-connection-manager raised an error."
                                    {:peer-id (config->peer-id config)
                                     :database-id (::database/id config)} e)))))
