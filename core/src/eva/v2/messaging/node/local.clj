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

(ns eva.v2.messaging.node.local
  (:require [eva.v2.system.protocols :as p]
            [eva.v2.utils.completable-future :as cf]
            [recide.sanex :as sanex]
            [quartermaster.core :as qu]
            [clojure.tools.logging :as log]
            [eva.error :refer [raise]]))

(defn send* [msgr f & args]
  (locking msgr
    (future
      (try (apply f msgr args)
           (catch Throwable t
             (log/error t "Local messenger send failed"))))))

(defn responder [addr f opts]
  {::type ::responder :addr addr :f f :opts opts})

(defn requestor [addr opts]
  {::type ::requestor :addr addr :opts opts :promises (atom {})})

(defn publisher [addr opts]
  {::type ::publisher :addr addr :opts opts})

(defn subscriber [id addr f opts]
  {::type ::subscriber :id id :addr addr :f f :opts opts})

(defn pub->sub! [{:as subscriber :keys [f]} msg]
  (f msg))

(defn publish!* [{:as publisher :keys [addr]} node msg]
  (let [subs (get-in node [:subscribers addr])]
    (doseq [[sub-id sub] (seq subs)]
      (send* sub pub->sub! msg)))
  publisher)

(defn publish! [node publisher msg]
  (send* publisher publish!* node msg))

(defn response! [{:as requestor :keys [promises]} id res]
  (let [prom (get @promises id)]
    (when prom
      (swap! promises dissoc id)
      (cf/deliver prom res))))

(defn respond! [{:as responder :keys [f]}
                {:as enveloped-msg :keys [msg reply-to]}]
  (let [[id requestor] reply-to
        res (try (f msg) (catch Throwable t t))]
    (send* requestor response! id res)))

(defn send-request!* [{:as requestor :keys [addr promises]} node msg res]
  (let [id (random-uuid)
        responder (get-in node [:responders addr])]
    (swap! promises assoc id res)
    (send* responder respond! {:msg msg :reply-to [id requestor]})))

(defn send-request! [node requestor msg]
  (let [res (cf/promise)]
    (send* requestor send-request!* node msg res)
    res))

(defrecord LocalMessengerState [responders
                                requestors
                                publishers
                                subscribers]
  p/ResponderManager
  (open-responder! [this addr f opts]
    (if-not (contains? responders addr)
      (assoc-in this
                [:responders addr]
                (responder addr f opts))
      (raise :messaging/responder-already-exists
             (format "responder for address %s already exists" addr)
             {:addr addr
              ::sanex/sanitary? true})))

  (close-responder! [this addr]
    (if-let [responder (get responders addr)]
      (update this :responders dissoc addr)
      (log/warnf "ignoring call to stop nonextant responder %s" addr)))

  p/RequestorManager
  (open-requestor! [this addr opts]
    (if-not (contains? requestors addr)
      (assoc-in this
                [:requestors addr]
                (requestor addr opts))
      (raise :messaging/requestor-already-exists
             (format "requestor for address %s already exists" addr)
             {:addr addr
              ::sanex/sanitary? true})))

  (close-requestor! [this addr]
    (if-let [requestor (get requestors addr)]
      (update this :requestors dissoc addr)
      (log/warnf "ignoring call to stop nonextant requestor %s" addr)))

  (request! [this addr msg]
    (if-let [requestor (get requestors addr)]
      (send-request! this requestor msg)
      (raise :messaging/requestor-does-not-exist
             (format "no requestor for %s has been created" addr)
             {:addr addr
              ::sanex/sanitary? true})))

  p/PublisherManager
  (open-publisher! [this addr opts]
    (if-not (contains? publishers addr)
      (assoc-in this
                [:publishers addr]
                (publisher addr opts))
      (raise :messaging/publisher-already-exists
             (format "publisher for address %s already exists" addr)
             {:addr addr, ::sanex/sanitary? true})))

  (close-publisher! [this addr]
    (if-let [publisher (get publishers addr)]
      (update this dissoc :publishers addr)
      (log/warnf "ignoring call to stop nonextant publisher %s" addr)))

  (publish! [this addr msg]
    (let [publisher (get publishers addr)]
      (if (some? publisher)
        (publish! this publisher msg)
        (raise :messaging/publisher-does-not-exist
               (format "no publisher for %s has been created" addr)
               {:addr addr, ::sanex/sanitary? true})))
    this)

  p/SubscriberManager
  (subscribe! [this subscriber-id addr f opts]
    (if-not (get-in subscribers [addr subscriber-id])
      (assoc-in this
                [:subscribers addr subscriber-id]
                (subscriber subscriber-id addr f opts))
      (raise :messaging/subscribers-already-exists
             (format "subscriber %s for address %s already exists" subscriber-id addr)
             {:addr addr
              ::sanex/sanitary? true
              :subscriber-id subscriber-id})))

  (unsubscribe! [this subscriber-id addr]
    (if-let [subscriber (get-in subscribers [addr subscriber-id])]
      (update-in this [:subscribers addr] dissoc subscriber-id)
      (log/warnf "ignoring call to stop nonextant publisher %s" addr))))

(defrecord LocalMessenger [id status state]
  qu/SharedResource
  (initiated? [_] @status)
  (status* [_] {})
  (resource-id [_] id)
  (initiate [this] (reset! status true) this)
  (terminate [this] (reset! status false) this)
  (force-terminate [_] (reset! status false))
  
  p/ResponderManager
  (open-responder! [this addr f opts] (swap! state p/open-responder! addr f opts) this)
  (close-responder! [this addr] (swap! state p/close-responder! addr) this)

  p/RequestorManager
  (open-requestor! [this addr opts] (swap! state p/open-requestor! addr opts) this)
  (close-requestor! [this addr] (swap! state p/close-requestor! addr) this)
  (request! [this addr request-msg] (p/request! @state addr request-msg))

  p/PublisherManager
  (open-publisher! [this addr opts] (swap! state p/open-publisher! addr opts) this)
  (close-publisher! [this addr] (swap! state p/close-publisher! addr) this)
  (publish! [this addr publish-msg] (p/publish! @state addr publish-msg))

  p/SubscriberManager
  (subscribe!   [this subscriber-id addr f opts] (swap! state p/subscribe! subscriber-id addr f opts) this)
  (unsubscribe! [this subscriber-id addr] (swap! state p/unsubscribe! subscriber-id addr) this)

  p/ErrorListenerManager
  (register-error-listener [this key f args])
  (unregister-error-listener [this key]))

(defn local-messenger []
  (map->LocalMessenger {:id (qu/new-resource-id :local-messenger)
                        :status (atom true)
                        :state (atom (map->LocalMessengerState {}))}))
