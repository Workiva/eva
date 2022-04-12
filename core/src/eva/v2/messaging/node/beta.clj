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

(ns eva.v2.messaging.node.beta
  "Minimal Reimplementation of node.alpha using the jms.beta
  code to make it JMS 1.1 compatible."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [eva.error :refer [raise]]
            [quartermaster.core :as qu]
            [utiliva.core :refer [locking-vswap!]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [eva.v2.system.protocols :as p]
            [eva.v2.messaging.jms.beta.connection-factory :as jms-cf]
            [eva.v2.messaging.jms.beta.patterns :as jms-patterns]
            [eva.v2.messaging.jms.beta.core :as jms]
            [eva.v2.messaging.jms.alpha.message :as alpha-msg]
            [com.stuartsierra.component :as component]
            [clojure.set :as set])

  (:import (javax.jms Session Message ExceptionListener Connection)
           (java.lang AutoCloseable)
           (clojure.lang ArityException)))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::config map?) ;; TODO: specify better

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defonce ^:dynamic node-id {::id (random-uuid)})

(def ^:private ^:dynamic *default-broker-type* "org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory")

(defn ^:private configured-broker-type
  "Returns the ConnectionFactory class name based on the value of
  the EVA_V2_MESSAGING_NODE_BETA_BROKER_TYPE environment variable.
  Supported values are:
    - \"artemis\" to use Artemis JMS client
    - \"activemq\" or \"amazonmq\" to use the ActiveMQ JMS client

  If the environment variable is not set, then the value of *default-broker-type* is used."
  []
  (if-some [t (or (System/getProperty "eva.v2.messaging.node.beta.broker-type")
                  (System/getenv "EVA_V2_MESSAGING_NODE_BETA_BROKER_TYPE"))]
    (condp = (str/lower-case t)
      #{":artemis" "artemis"} "org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory"
      #{":activemq" "activemq"
        ":amazonmq" "amazonmq"} "org.apache.activemq.ActiveMQConnectionFactory")
    *default-broker-type*))

(defn ^:private connection-factory
  "Dynamically constructs an instance of a ConnectionFactory given:
    - ::broker-type the class name of the ConnectionFactory
    - ::broker-uri connection uri for the broker
    - ::broker-user username to authenticate against the broker
    - ::broker-password password to authenticate against the broker

  If ::broker-type is no provided, then a default broker-type will
  be retrieved using (configured-broker-type).
  If ::broker-user and ::broker-password are not provided, then
  the ConnectionFactory will be constructed without any user or password."
  [{:keys [::broker-type
           ::broker-uri
           ::broker-user
           ::broker-password]}]
  (let [broker-type (or broker-type (configured-broker-type))]
    (condp = broker-type
      "org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory"
      (if (and broker-user broker-password)
        (jms-cf/new-connection-factory broker-type (str broker-uri) broker-user broker-password)
        (jms-cf/new-connection-factory broker-type (str broker-uri)))

      "org.apache.activemq.ActiveMQConnectionFactory"
      (if (and broker-user broker-password)
        (jms-cf/new-connection-factory broker-type broker-user broker-password (str broker-uri))
        (jms-cf/new-connection-factory broker-type (str broker-uri))))))

(defn ^:private init-messenger
  "init-messenger provides the implementation of SharedResource/initiate
  and SHOULD NOT be used outside of that context. It is extracted here
  for:
    1. documentation purposes;
    2. debugging purposes.

  Initiates messenger (a Messenger) given a broker-config map.
  If already initiated, the messenger is returned unchanged.
  If NOT already initiated, the messenger is initiated with:
    1. a new resource-id is generated;
    2. a ConnectionFactory is created from the broker-config;
    3. an atom containing a map listener-key -> listener-fn;
    4. an instance of javax.jms.ExceptionListener that calls all listener-fns
       in the atom defined in #3;
    5. a javax.jms.Connection is created, set with the ExceptionListener from #4, and started.
    6. volatiles containing maps to hold any requestors, responders, publishers, or subscribers
       created by the MessengerNode;
    7. assocs the preceding items into messenger"
  [messenger
   {:as broker-config
    :keys [::broker-type
           ::broker-uri
           ::broker-user
           ::broker-password]}]
  (if (qu/initiated? messenger)
    messenger
    (let [resource-id (qu/new-resource-id)
          cf (connection-factory broker-config)
          connection-error-listeners (atom {})
          exception-listener (reify ExceptionListener
                               (onException [_ ex]
                                 (doseq [[id listener] @connection-error-listeners
                                         :let [{:keys [::on-connection-error ::args]} listener]]
                                   (try
                                     (try (apply on-connection-error ex args)
                                          (catch ArityException e
                                            (log/warn e "wrong number of args passed to exception-listener-function:" [id listener]))
                                          (catch ClassCastException e
                                            (if (some-> e (.getMessage) (.contains "cannot be cast to clojure.lang.IFn"))
                                              (log/warn e "exception-listener is not a function" [id listener])
                                              (throw e))))
                                     (catch Throwable e
                                       (log/warn e "exception occurred calling connection-error-listener"))))))]
      (assoc messenger
             :resource-id (atom resource-id)
             :broker-config broker-config
             :connection-factory cf
             :connection (doto (jms/create-connection cf {:start? false})
                           (.setExceptionListener exception-listener)
                           (.start))
             :connection-error-listeners connection-error-listeners
             :connection-exception-listener exception-listener
             :publishers-vol (volatile! {})
             :subscribers-vol (volatile! {})
             :requestors-vol (volatile! {})
             :responders-vol (volatile! {})))))

(defn ^:private add-connection-error-listener
  "Registers an error listener function with the messenger:
  - messenger is a Messenger;
  - key is an identifier that uniquely identifies the listener function;
  - f is a function that will be applied as (apply f exception args);
  - args is a sequence that will be applied to the preceding function."
  [{:as messenger :keys [connection-error-listeners]} key f args]
  (swap! connection-error-listeners
         (fn [listeners]
           (if (contains? listeners key)
             (throw (IllegalStateException. (str "connection-error-listener already exists for key " key)))
             (assoc listeners key {::on-connection-error f
                                   ::args args})))))

(defn ^:private remove-connection-error-listener
  "Removes the error listener function that was registered with key"
  [{:as messenger :keys [connection-error-listeners]} key]
  (swap! connection-error-listeners dissoc key))

(def ^:private data-message-type
  "String that identifies a data message.
   Expected usage:
    1. set on a message: (.setJMSType aMessage data-message-type);
    2. read from a message: (= data-message-type (.getJMSType aMessage))"
  (str *ns* "/" "data-message"))

(def ^:private error-message-type
  "String that identifies an error message.
   Expected usage:
    1. set on a message: (.setJMSType aMessage error-message-type) ;
    2. read from a message: (= error-message-type (.getJMSType aMessage))"
  (str *ns* "/" "error-message"))

(def ^:private message-protocol
  "Implements MessageProtocol from eva.v2.messaging.jms.beta.patterns.
   This message-protocol is used by the MessengerNode to:
    1. distinguish data and error messages;
    2. read data and error messages;
    3. create/write data and error messages."
  (reify jms-patterns/MessageProtocol
    (new-data-message [_ session] (doto (.createBytesMessage ^Session session)
                                    (.setJMSType data-message-type)))

    (data-message? [_ msg]
      (if-some [msg-type (.getJMSType ^Message msg)]
        ;; fast-path
        (= msg-type data-message-type)
        ;; slower, backwards-compatible path
        (contains? (#'alpha-msg/interpret-message msg) ::alpha-msg/data)))

    (write-data-message [_ msg data]
      (if (instance? Throwable data)
        (throw (IllegalArgumentException. "Cannot write throwable as data"))
        (alpha-msg/write-content! msg data)))

    (read-data-message [_ msg]
      (let [result (alpha-msg/read-content msg)]
        (if (instance? Throwable result)
          (throw (IllegalStateException. "data-message contained throwable"))
          result)))

    (error-message? [_ msg]
      (if-some [msg-type (.getJMSType ^Message msg)]
        ;; fast-path
        (= msg-type error-message-type)
        ;; slower, backwards-compatible path
        (contains? (#'alpha-msg/interpret-message msg) ::alpha-msg/throwable)))

    (new-error-message [_ session] (doto (.createBytesMessage ^Session session)
                                     (.setJMSType error-message-type)))

    (write-error-message [_ msg err]
      (if-not (instance? Throwable err)
        (throw (IllegalArgumentException. "error must be a Throwable"))
        (alpha-msg/write-content! msg err)))

    (read-error-message [_ msg]
      (let [result (alpha-msg/read-content msg)]
        (if (instance? Throwable result)
          result
          (throw (IllegalStateException. "error message did not contain throwable")))))))

;; Messenger node implementation
(defrecord Messenger [resource-id
                      broker-config
                      connection-factory
                      ;; connection instance
                      connection
                      ;; connection ExceptionListener
                      connection-exception-listener
                      ;; connection error listeners
                      connection-error-listeners
                      ;; volatile {addr --> publisher}
                      publishers-vol
                      ;; volatile {[addr subscriber-id] --> subscriber}
                      subscribers-vol
                      ;; volatile {addr --> requestor}
                      requestors-vol
                      ;; volatile {addr --> responder}
                      responders-vol]
  qu/SharedResource
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (initiate [messenger]
    (locking messenger
      (assert broker-config)
      (init-messenger messenger broker-config)))
  (terminate [messenger]
    (locking messenger
      (if (qu/terminated? messenger)
        messenger
        (let [res-id @resource-id]
          (reset! resource-id nil)
          (doto connection
            (.close))
          (letfn [(close-all! [closeables]
                    (doseq [^AutoCloseable c closeables]
                      (.close c)))]
            (doseq [vol [responders-vol
                         requestors-vol
                         publishers-vol
                         subscribers-vol]]
              (close-all! (vals @vol))
              (vreset! vol {})))
          (assoc messenger
                 :connection-factory nil
                 :connection nil
                 :connection-error-listeners nil
                 :connection-exception-listener nil
                 :requestors-vol nil
                 :responders-vol nil
                 :subscribers-vol nil
                 :publishers-vol nil)))))
  (force-terminate [messenger] (qu/terminate messenger))

  p/ResponderManager
  (open-responder! [mn addr f opts]
    (qu/ensure-initiated! mn "cannot open responder, Messenger is not initiated")
    (-> responders-vol
        (locking-vswap!
         (fn [responders-map]
           (if-not (contains? responders-map addr)
             (assoc responders-map
                    addr
                    (jms-patterns/responder
                     connection
                     (merge opts
                            {:request-queue      addr
                             :message-protocol message-protocol
                             :request-handler    f})))
             (raise :messaging/responder-already-exists
                    (format "responder for address %s already exists" addr)
                    {:addr addr
                     ::sanex/sanitary? true}))))
        (get addr)))

  (close-responder! [mn addr]
    (qu/ensure-initiated! mn "cannot close responder, Messenger not initiated")
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
                    (jms-patterns/requestor
                     connection
                     (merge opts
                            {:message-protocol message-protocol
                             :request-queue    addr})))
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
    (if-let [requestor (get @requestors-vol addr)]
      (jms-patterns/send-request! requestor msg)
      (raise :messaging/requestor-does-not-exist
             (format "no requestor for %s has been created" addr)
             {:addr addr
              ::sanex/sanitary? true})))

  p/PublisherManager
  (open-publisher! [this addr opts]
    (qu/ensure-initiated! this "cannot open publisher.")
    (-> (locking-vswap!
         publishers-vol
         (fn [publishers-map]
           (if-not (contains? publishers-map addr)
             (assoc publishers-map
                    addr
                    (jms-patterns/publisher
                     connection
                     (merge opts
                            {:message-protocol message-protocol
                             :topic            addr})))
             (raise :messaging/publisher-already-exists
                    (format "publisher for address %s already exists" addr)
                    {:addr addr, ::sanex/sanitary? true}))))
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
        (jms-patterns/publish! publisher msg)
        (raise :messaging/publisher-does-not-exist
               (format "no publisher for %s has been created" addr)
               {:addr addr, ::sanex/sanitary? true}))))

  p/SubscriberManager
  (subscribe! [this subscriber-id addr f opts]
    (qu/ensure-initiated! this "cannot subscribe.")
    (-> (locking-vswap!
         subscribers-vol
         (fn [subscribers-map]
           (if-not (contains? subscribers-map [addr subscriber-id])
             (assoc subscribers-map
                    [addr subscriber-id]
                    (jms-patterns/subscriber
                     connection
                     (merge opts
                            {:subscriber-id      subscriber-id
                             :message-protocol   message-protocol
                             :handler            f
                             :topic              addr})))
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
  (register-error-listener [this key f args] (add-connection-error-listener this key f args))
  (unregister-error-listener [this key] (remove-connection-error-listener this key)))

(defn create-messenger
  "Creates a new, un-initiated Messenger containing the broker-config"
  [broker-config]
  (map->Messenger {:broker-config broker-config}))

(defn extract-broker-config
  "Extracts and converts a broker-config from a messaging config.
  This mainly involves renaming various keys from unqualified to
  qualified forms."
  [config]
  (-> config
      (set/rename-keys {:broker-type ::broker-type
                        :broker-uri ::broker-uri
                        :broker-user ::broker-user
                        :broker-password ::broker-password})
      (select-keys [::broker-type ::broker-uri ::broker-user ::broker-password :messenger-node-config/type])))

(qu/defmanager messenger-nodes
  :discriminator (fn [_ config] (extract-broker-config config))
  :constructor (fn [broker-config _] (create-messenger broker-config)))
