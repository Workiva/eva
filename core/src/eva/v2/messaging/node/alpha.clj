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

(ns eva.v2.messaging.node.alpha
  (:require [clojure.spec.alpha :as s]
            [eva.error :refer [raise]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [eva.v2.messaging.jms.alpha.core :as nc]
            [eva.v2.system.protocols :as p]
            [utiliva.core :refer [locking-vswap!]]
            [quartermaster.core :as qu]
            [eva.v2.utils.completable-future :as completable-future]
            [com.stuartsierra.component :as component])
  (:import (java.lang AutoCloseable)))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::config map?) ;; TODO: specify better

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defn ^:private build-node-id []
  {::id (random-uuid)})

(defonce ^:dynamic node-id (build-node-id))

(qu/defmanager connection-factories
  :discriminator
  (fn [_ jms-uri] jms-uri)
  :constructor
  (fn [_ jms-uri] (nc/connection-factory-from-uri jms-uri)))

(defrecord Messenger [resource-id
                      broker-uri
                      connection-factory
                      publishers-vol ;; volatile {addr --> publisher}
                      subscribers-vol ;; volatile {[addr subscriber-id] --> subscriber}
                      requestors-vol ;; volatile {addr --> requestor}
                      responders-vol] ;; volatile {addr --> responder}
  qu/SharedResource
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [resource-id (qu/new-resource-id)
                     connection-factory (qu/acquire connection-factories resource-id broker-uri)]
        (assoc this
               :resource-id (atom resource-id)
               :connection-factory connection-factory
               :publishers-vol (volatile! {})
               :subscribers-vol (volatile! {})
               :requestors-vol (volatile! {})
               :responders-vol (volatile! {})))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id @resource-id]
        (reset! resource-id nil)
        (qu/release connection-factory true)
        (doseq [responder (vals @responders-vol)]
          (.close ^AutoCloseable responder))
        (vreset! responders-vol {})
        (doseq [publisher (vals @publishers-vol)]
          (.close ^AutoCloseable publisher))
        (vreset! publishers-vol {})
        (doseq [requestor (vals @requestors-vol)]
          (.close ^AutoCloseable requestor))
        (vreset! requestors-vol {})
        (doseq [subscriber (vals @subscribers-vol)]
          (.close ^AutoCloseable subscriber))
        (vreset! subscribers-vol {})
        (assoc this
               :connection-factory nil
               :publishers-vol nil
               :subscribers-vol nil
               :requestors-vol nil
               :responders-vol nil))))
  (force-terminate [this] (qu/terminate this))

  p/ResponderManager
  (open-responder! [mn addr f opts]
    (qu/ensure-initiated! mn "cannot open responder.")
    (-> responders-vol
        (locking-vswap!
         (fn [responders-map]
           (if-not (contains? responders-map addr)
             (assoc responders-map
                    addr
                    (nc/responder
                     (merge {:connection-factory @connection-factory
                             :request-queue (nc/queue addr)
                             :request-handler f}
                            opts)))
             (raise :messaging/responder-already-exists
                    (format "responder for address %s already exists" addr)
                    {:addr addr
                     ::sanex/sanitary? true}))))
        (get addr)))
  (close-responder! [mn addr]
    (qu/ensure-initiated! mn "cannot close responder.")
    (locking-vswap!
     responders-vol
     (fn [responders-map]
       (if-let [responder (get responders-map addr)]
         (do (.close ^AutoCloseable responder)
             (dissoc responders-map addr))
         (log/warnf "ignoring call to stop nonextant responder %s" addr))))
    true)

  p/RequestorManager
  (open-requestor! [this addr opts]
    (qu/ensure-initiated! this "cannot open requestor.")
    (-> (locking-vswap!
         requestors-vol
         (fn [requestors-map]
           (if-not (contains? requestors-map addr)
             (assoc requestors-map
                    addr
                    (nc/requestor
                     (merge {:connection-factory @connection-factory
                             :request-queue (nc/queue addr)}
                            opts)))
             (raise :messaging/requestor-already-exists
                    (format "requestor for address %s already exists" addr)
                    {:addr addr
                     ::sanex/sanitary? true}))))
        (get addr)))
  (close-requestor! [mn addr]
    (qu/ensure-initiated! mn "cannot close requestor.")
    (locking-vswap!
     requestors-vol
     (fn [requestors-map]
       (if-let [requestor (get requestors-map addr)]
         (do (.close ^AutoCloseable requestor)
             (dissoc requestors-map addr))
         (log/warnf "ignoring call to stop nonextant requestor %s" addr))))
    true)
  (request! [this addr msg]
    (qu/ensure-initiated! this "cannot send request.")
    (let [requestor (get @requestors-vol addr)]
      (if (some? requestor)
        (nc/send-request! requestor msg)
        (raise :messaging/requestor-does-not-exist
               (format "no requestor for %s has been created" addr)
               {:addr addr
                ::sanex/sanitary? true}))))

  p/PublisherManager
  (open-publisher! [this addr opts]
    (qu/ensure-initiated! this "cannot open publisher.")
    (-> (locking-vswap!
         publishers-vol
         (fn [publishers-map]
           (if-not (contains? publishers-map addr)
             (assoc publishers-map
                    addr
                    (nc/publisher
                     (merge {:connection-factory @connection-factory
                             :topic (nc/topic addr)}
                            opts)))
             (raise :messaging/publisher-already-exists
                    (format "publisher for address %s already exists" addr)
                    {:addr addr
                     ::sanex/sanitary? true}))))
        (get addr)))
  (close-publisher! [this addr]
    (qu/ensure-initiated! this "cannot close publisher.")
    (locking-vswap!
     publishers-vol
     (fn [publishers-map]
       (if-let [publisher (get publishers-map addr)]
         (do (.close ^AutoCloseable publisher)
             (dissoc publishers-map addr))
         (log/warnf "ignoring call to stop nonextant publisher %s" addr)))))
  (publish! [this addr msg]
    (qu/ensure-initiated! this "cannot publish.")
    (let [publisher (get @publishers-vol addr)]
      (if (some? publisher)
        (nc/publish! publisher msg)
        (raise :messaging/publisher-does-not-exist
               (format "no publisher for %s has been created" addr)
               {:addr addr
                ::sanex/sanitary? true}))))

  p/SubscriberManager
  (subscribe! [this subscriber-id addr f opts]
    (qu/ensure-initiated! this "cannot subscribe.")
    (-> (locking-vswap!
         subscribers-vol
         (fn [subscribers-map]
           (if-not (contains? subscribers-map [addr subscriber-id])
             (assoc subscribers-map
                    [addr subscriber-id]
                    (nc/subscriber
                     (merge {:connection-factory @connection-factory
                             :subscriber-id subscriber-id
                             :handler f
                             :topic (nc/topic addr)}
                            opts)))
             (raise :messaging/subscribers-already-exists
                    (format "subscriber %s for address %s already exists" subscriber-id addr)
                    {:addr addr
                     ::sanex/sanitary? true
                     :subscriber-id subscriber-id}))))
        (get [addr subscriber-id])))
  (unsubscribe! [this subscriber-id addr]
    (qu/ensure-initiated! this "cannot unsubscribe.")
    (locking-vswap!
     subscribers-vol
     (fn [subscribers-map]
       (if-let [subscriber (get subscribers-map [addr subscriber-id])]
         (do (.close ^AutoCloseable subscriber)
             (dissoc subscribers-map [addr subscriber-id]))
         (do (log/warnf "ignoring call to stop nonextant publisher %s" addr)
             subscribers-map)))))

  p/ErrorListenerManager
  ;; no-ops for node.alpha
  (register-error-listener [this key f args])
  (unregister-error-listener [this key]))

(defn messenger [broker-uri]
  (map->Messenger {:broker-uri broker-uri}))

(qu/defmanager messenger-nodes
  :discriminator (fn [_ config] (:broker-uri config))
  :constructor (fn [broker-uri _] (messenger broker-uri)))
