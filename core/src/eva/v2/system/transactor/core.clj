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

(ns eva.v2.system.transactor.core
  (:require [quartermaster.core :as qu]
            [eva.core :as core]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.messaging.node.manager.alpha :as messenger-node]
            [eva.v2.messaging.node.manager.types :as messenger]
            [eva.v2.messaging.address :as address]
            [eva.v2.system.protocols :as p]
            [eva.v2.system.database-connection.core :as dbc]
            [eva.v2.storage.system :as storage]
            [eva.v2.database.core :as database]
            [barometer.core :as m]
            [barometer.aspects :refer [timed concurrency-measured]]
            [eva.contextual.core :as cntx]
            [eva.contextual.tags :as cntx-tags]
            [eva.contextual.config :as cntx-config]
            [eva.error :refer [raise error?]]
            [eva.utils.logging :refer [logged]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [clojure.spec.alpha :as s]
            [eva.v2.utils.spec :refer [conform-spec]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced tracing]]
            [com.stuartsierra.component :as c]
            [recide.core :refer [try*]])
  (:import [java.util.List]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::id uuid?)

(s/def ::config
  (s/merge
   ::storage/config
   ::messenger/config
   (s/keys :req [::id
                 ::database/id
                 ::address/transaction-submission
                 ::address/transaction-publication
                 ::address/index-updates])))

(def long? (partial instance? Long))
(s/def ::transaction-id long?)

(s/def ::transaction-data
  (partial instance? java.util.List))

(s/def ::temporary-ids (s/map-of long? long?))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defn ->txd-pub [transactor-id database-id tx-report]
  {:tx-num (-> tx-report :log-entry :tx-num)
   :database-id database-id})

(defn ->txd-msg [transactor-id database-id tx-report]
  {:tempids (:tempids tx-report)
   :log-entry (:log-entry tx-report)
   :database-id database-id})

(def ^:dynamic *restart-delay-ms* 1500)

(defn on-connection-error:try-restart-transactor
  "This is an error-listener function installed on the transactor's Messenger.

  When a messaging connection encounters an exception, error-listener functions will be called.
  In the event of such an error, this function attempts to re-initiate the messenger
  *once*. 
  
  If re-initiation fails then the JVM will exit (ie. fast fail), and
  an external process-manager is expected to restart the process."
  [ex messenger messenger-id]
  (future
    (log/warn ex "transactor messaging encountered error, attempting restart")
    (when (pos-int? *restart-delay-ms*) (Thread/sleep *restart-delay-ms*))
    (try* (qu/reinitiate messenger messenger-id)
          (catch (:not ::qu/no-such-handle) t
            (log/error t "transactor messaging restart encountered fatal error, aborting process")
            (System/exit -100))
          (catch Throwable t nil)))) ;; swallow the rest

(defn- onboard-messenger
  [{:as transactor :keys [transactor-id transaction-pub-addr transaction-addr index-updates-addr]}
   messenger-reference]
  (locking messenger-reference
    (let [messenger-snapshot (qu/resource* messenger-reference)
          messenger-id (qu/resource-id messenger-snapshot)
          transactor (assoc transactor :messenger messenger-reference)]
      (p/register-error-listener messenger-snapshot
                                 transactor-id
                                 on-connection-error:try-restart-transactor
                                 [messenger-reference messenger-id])
      (p/open-publisher! messenger-snapshot transaction-pub-addr {})
      (p/subscribe! messenger-snapshot
                    transactor-id
                    index-updates-addr
                    (partial p/process-index-updates transactor)
                    {})
      (p/open-responder! messenger-snapshot transaction-addr (partial p/process-transaction transactor) {}))))

(def ^:dynamic *max-concurrent-modification-retries* 10)

(def concurrent-modification-meter (m/meter "meter of retries from concurrent modifications"))
(m/register m/DEFAULT (str *ns* ".concurrent-modification-meter") concurrent-modification-meter)

(defn update-index-lag-gauge [txor database-id tx-report]
  (let [log-entry (:log-entry tx-report)
        gauge     (cntx/->gauge)
        lag       (- (:tx-num log-entry)
                     (->> log-entry :index-roots vals (map :tx-num) (apply min)))]
    ((:set! (meta gauge)) lag)))

(d/defn ^{::d/aspects [concurrency-measured ;; TODO: should be cntx/concurrency-measured [::cntx-tags/database-id] ?
                       (cntx/timed ::cntx-config/override [::cntx-tags/database-id])
                       (cntx/capture '{::cntx-tags/database-id database-id})
                       (logged)
                       traced]}

  process-transaction-impl
  [{:as txor :keys [txor-timer messenger database-connection transaction-pub-addr database-id config transactor-id]}
   {:as request :keys [tx-data]}]
  (if-not (= database-id (:database-id request))
    (log/warnf "transactor for %s is ignoring transaction for %s" database-id (:database-id request))
    (do (log/debug "transactor is processing request.")
        (loop [attempt 0]
          (if (>= attempt *max-concurrent-modification-retries*)
            (do
              (log/warn "The transactor exceeded its maximum number of attempts to process a transaction. There is excessive concurrent modification on the backing transaction log.")
              (raise ::max-concurrent-transaction-attempts-exceeded
                     "The transactor exceeded its maximum number of attempts to process the transaction. There is excessive concurrent modification on the backing transaction log."
                     {::sanex/sanitary? true
                      :database-id database-id}))
            (let [tx-report (p/commit-tx @database-connection tx-data)]
              (if (dbc/stale-write-failure? tx-report)
                (do
                  (m/update concurrent-modification-meter)
                  (log/warn "Found the transaction log to be concurrently modified or in a bad state. Attempting repair / refresh and retrying transaction.")
                  (p/repair-and-reload @database-connection)
                  (recur (inc attempt)))
                (do
                  (log/debug "transaction commit! Publishing.")
                  (update-index-lag-gauge txor database-id tx-report)
                  (p/publish! @messenger transaction-pub-addr (->txd-pub transactor-id database-id tx-report))
                  (->txd-msg transactor-id database-id tx-report)))))))))

(d/defn ^{::d/aspects [traced (logged) timed]} process-index-updates-impl
  [{:as txor :keys [database-id database-connection config transactor-id]}
   {:as updates :keys [index-updates]}]
  (log/debug (format "transactor %s receipt of index publish %s" transactor-id updates))
  (if-not (= database-id (:database-id updates))
    (log/warnf "transactor for %s is ignoring index updates for %s" database-id (:database-id updates))
    (do (log/debug "Transactor received index updates to be applied on next transaction:" index-updates)
        (p/update-latest-index-roots @database-connection index-updates))))

(defn- clean-up-messaging
  [messenger transactor-id transaction-addr index-updates-addr transaction-pub-addr]
  (p/close-responder! messenger transaction-addr)
  (p/unsubscribe! messenger transactor-id index-updates-addr)
  (p/close-publisher! messenger transaction-pub-addr))

(defrecord Transactor [resource-id
                       database-id
                       transactor-id
                       transaction-addr
                       transaction-pub-addr
                       index-updates-addr
                       database-connection ;; atom
                       messenger ;; atom
                       config
                       state]
  qu/SharedResource
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [_ (log/debug "starting transactor with: "
                                  {:database-id database-id
                                   :transaction-pub-addr transaction-pub-addr
                                   :transaction-addr transaction-addr
                                   :index-updates-addr index-updates-addr})
                     resource-id (qu/new-resource-id)
                     database-connection (qu/acquire dbc/database-connection-manager resource-id config)
                     this (assoc this
                                 :resource-id (atom resource-id)
                                 :database-connection database-connection)
                     messenger (qu/acquire messenger-node/messenger-nodes resource-id config (partial onboard-messenger this))]
                    (assoc this :messenger messenger))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id (qu/resource-id this)]
        (try
          (clean-up-messaging @messenger transactor-id transaction-addr index-updates-addr transaction-pub-addr)
          (catch Throwable t)) ;; NOTE: These could fail -- that's okay.
        (qu/release messenger true)
        (qu/release database-connection true)
        (assoc this
               :database-connection nil
               :messenger nil))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do
        (qu/force-terminate messenger)
        (qu/force-terminate database-connection)
        (qu/terminate this))))
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  p/Transactor
  ;; TODO: re-inline these by adding options to the registration of the message handlers
  ;;       to inject timing / tracing / logging
  (process-transaction [this request]
    (qu/ensure-initiated! this "transaction cannot be processed.")
    (process-transaction-impl this request))
  (process-index-updates [this updates]
    (qu/ensure-initiated! this "index updates cannot be processed.")
    (process-index-updates-impl this updates)))

(defn transactor*
  "Constructs an uninitiated transactor from config."
  [{:as config}]
  {:pre [(conform-spec ::config config)]}
  (map->Transactor {:transactor-id (::id config)
                    :database-id (::database/id config)
                    :transaction-addr (::address/transaction-submission config)
                    :index-updates-addr (::address/index-updates config)
                    :transaction-pub-addr (::address/transaction-publication config)
                    :config config}))

(qu/defmanager transactor-manager
  :discriminator
  (fn [_ config] [(::database/id config) (::values/partition-id config) (::id config)])
  :constructor
  (fn [_ config] (transactor* config)))

(d/defn ^{::d/aspects [traced (logged) timed]} transactor
  "Exists for the tracing & metrics on transactor creation. Acquires the transactor resource."
  [user-id {:as config}]
  @(qu/acquire transactor-manager user-id config))
