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

(ns eva.v2.messaging.node.local-simulation
  "This messaging implementation was forked from
   the local messaging node to provide a less
   restricted more-nondeterministic broker."
  (:require [eva.v2.system.protocols :as p]
            [eva.v2.utils.completable-future :as cf]
            [recide.sanex :as sanex]
            [quartermaster.core :as qu]
            [clojure.tools.logging :as log]
            [clojure.set :as s]
            [eva.error :refer [raise]])
  (:import [java.util UUID]))

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

(defn get-responder [node addr]
  (rand-nth (seq (s/select #(= (:addr %) addr) (:responders node)))))

(defn get-subscribers [node addr]
  (shuffle (seq (s/select #(= (:addr %) addr) (:subscribers node)))))

(defn pub->sub! [{:as subscriber :keys [f]} msg]
  (f msg))

(defn publish!* [{:as publisher :keys [addr]} node msg]
  (let [subs (get-subscribers node addr)]
    (doseq [sub (seq subs)]
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
  (let [id (UUID/randomUUID)
        responder (get-responder node addr)]
    (swap! promises assoc id res)
    (send* responder respond! {:msg msg :reply-to [id requestor]})))

(defn send-request! [node requestor msg]
  (let [res (cf/promise)]
    (send* requestor send-request!* node msg res)
    res))

;; magic flag for creating multiple local instances where there should not be.
(def ^:dynamic *instance-id* 0)

(defrecord LocalMessengerState [responders
                                requestors
                                publishers
                                subscribers]
  p/ResponderManager
  (open-responder! [this addr f opts]
    (update this
            :responders
            (fnil conj #{})
            (assoc (responder addr f opts) ::instance-id *instance-id*)))

  (close-responder! [this addr]
    (assoc this
           :responders
           (into #{}
                 (remove #(and (= (:addr %) addr)
                               (= (::instance-id %) *instance-id*)))
                 responders)))

  p/RequestorManager
  (open-requestor! [this addr opts]
    (assoc-in this
              [:requestors addr]
              (requestor addr opts)))

  (close-requestor! [this addr]
    (update this :requestors dissoc addr))

  (request! [this addr msg]
    (if-let [requestor (get requestors addr)]
      (send-request! this requestor msg)
      (raise :messaging/requestor-does-not-exist
             (format "no requestor for %s has been created" addr)
             {:addr addr
              ::sanex/sanitary? true})))

  p/PublisherManager
  (open-publisher! [this addr opts]
    (assoc-in this
              [:publishers addr]
              (publisher addr opts)))

  (close-publisher! [this addr]
    (update this dissoc :publishers addr))

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
    (update this
            :subscribers
            (fnil conj #{})
            (assoc (subscriber subscriber-id addr f opts) ::instance-id *instance-id*)))

  (unsubscribe! [this subscriber-id addr]
    (assoc this
           :subscribers
           (into #{}
                 (remove #(and (= (:addr %) addr)
                               (= (:id %) subscriber-id)
                               (= (::instance-id %) *instance-id*)))
                 subscribers))))

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
  (request! [this addr request-msg]
    (p/request! @state addr request-msg))

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

(defmacro with-instance-id [id & body]
  `(binding [*instance-id* ~id] ~@body))

(defrecord FacadeMessenger [id instance-id shared-messenger status]
  qu/SharedResource
  (initiated? [_] @status)
  (status* [_] {})
  (resource-id [_] id)
  (initiate [this] (reset! status true) this)
  (terminate [this] (reset! status false) this)
  (force-terminate [_] (reset! status false))

  p/ResponderManager
  (open-responder! [this addr f opts]
    (with-instance-id instance-id (p/open-responder! shared-messenger addr f opts))
    this)
  (close-responder! [this addr]
    (with-instance-id instance-id (p/close-responder! shared-messenger addr))
    this)

  p/RequestorManager
  (open-requestor! [this addr opts]
    (with-instance-id instance-id (p/open-requestor! shared-messenger addr opts))
    this)

  (close-requestor! [this addr]
    (with-instance-id instance-id (p/close-requestor! shared-messenger addr))
    this)

  (request! [this addr request-msg]
    (with-instance-id instance-id (p/request! shared-messenger addr request-msg)))

  p/PublisherManager
  (open-publisher! [this addr opts]
    (with-instance-id instance-id (p/open-publisher! shared-messenger addr opts))
    this)

  (close-publisher! [this addr]
    (with-instance-id instance-id (p/close-publisher! shared-messenger addr))
    this)

  (publish! [this addr publish-msg]
    (with-instance-id instance-id (p/publish! shared-messenger addr publish-msg)))

  p/SubscriberManager
  (subscribe!   [this subscriber-id addr f opts]
    (with-instance-id instance-id (p/subscribe! shared-messenger subscriber-id addr f opts))
    this)

  (unsubscribe! [this subscriber-id addr]
    (with-instance-id instance-id (p/unsubscribe! shared-messenger subscriber-id addr))
    this)

  p/ErrorListenerManager
  (register-error-listener [this key f args])
  (unregister-error-listener [this key]))

(defn facade-messenger [shared-messenger instance-id]
  (map->FacadeMessenger {:id (qu/new-resource-id :facade-messenger)
                         :status (atom true)
                         :instance-id instance-id
                         :shared-messenger shared-messenger}))
