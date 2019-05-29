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

(ns eva.v2.messaging.jms.alpha.core
  (:require [clojure.data.fressian :as fressian]
            [eva.v2.utils.completable-future :as completable-future]
            [recide.sanex :refer [sanitize]]
            [eva.v2.fressian :as eva-fress]
            [eva.error :refer [error raise]]
            [recide.utils :as ru]
            [recide.sanex.logging :refer [warn] :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s])
  (:import (clojure.lang Keyword)
           (com.google.common.collect MapMaker)
           (java.lang AutoCloseable)
           (java.util.concurrent ConcurrentMap CompletableFuture)
           (javax.jms ConnectionFactory Destination Message MessageListener JMSContext JMSProducer JMSConsumer Queue ExceptionListener JMSException Topic JMSRuntimeException BytesMessage)
           (org.apache.activemq.artemis.api.core ActiveMQException ActiveMQExceptionType DiscoveryGroupConfiguration)
           (org.apache.activemq.artemis.api.jms ActiveMQJMSClient JMSFactoryType)
           (org.apache.activemq.artemis.utils UUIDGenerator)
           (org.apache.activemq.artemis.jms.client ActiveMQJMSConnectionFactory ActiveMQConnectionFactory)
           (java.net URI)))

(defn- generate-string-uuid []
  ;; This is how Artemis internally generates a connection id/name
  (-> (UUIDGenerator/getInstance) (.generateStringUUID)))

(defn ^ActiveMQJMSConnectionFactory connection-factory-from-uri [uri]
  (ActiveMQJMSClient/createConnectionFactory (str uri) (generate-string-uuid)))

(defn ^ActiveMQJMSConnectionFactory connection-factory-ha-with-discovery-group
  [^DiscoveryGroupConfiguration dgc]
  (ActiveMQJMSClient/createConnectionFactoryWithHA dgc JMSFactoryType/CF))

(defn ^URI connection-factory-uri [^ActiveMQConnectionFactory cf] (.toURI cf))

(defn ^JMSContext create-context
  ([connection-factory] (create-context connection-factory JMSContext/AUTO_ACKNOWLEDGE))
  ([connection-factory ack-behavior] (.createContext ^ConnectionFactory connection-factory ack-behavior)))

;; Queue and Topic References
(defn ^Queue queue [queue-name] (ActiveMQJMSClient/createQueue (str queue-name)))
(defn ^Topic topic [topic-name] (ActiveMQJMSClient/createTopic (str topic-name)))

;; Producers and Consumers
(defn ^JMSProducer producer [^JMSContext c] (.createProducer c))
(defn ^JMSConsumer consumer [^JMSContext c ^Destination d] (.createConsumer c d))

;; Message Creation
(defprotocol MessageCreation
  (-create-message [x])
  (-create-text-message [x] [x s])
  (-create-bytes-message [x]))

(extend-type JMSContext
  MessageCreation
  (-create-message [c] (.createMessage c))
  (-create-text-message [c] (.createTextMessage c))
  (-create-text-message [c s] (.createTextMessage c s))
  (-create-bytes-message [c] (.createBytesMessage c)))

(defn message [x] (-create-message x))
(defn text-message
  ([x] (-create-text-message x))
  ([x s] (-create-text-message x s)))

(defn wrap-msg
  "Build an artisanal exception monad around the message contents"
  [msg]
  (if (instance? Throwable msg)
    {:throwable? true  :payload (ru/serialize-throwable msg)}
    {:throwable? false :payload msg}))

(defn bytes-message-payload
  "Read out the contents of the maybe-exception monad payload.

   Returns POCO or Throwable."
  [^BytesMessage msg]
  (let [len (.getBodyLength msg)
        ret (byte-array len)]
    (.readBytes msg ret)
    (let [wrapped-msg (fressian/read ret :handlers eva-fress/eva-messaging-read-handlers)]
      (if (:throwable? wrapped-msg)
        (ru/deserialize-throwable (:payload wrapped-msg))
        (:payload wrapped-msg)))))

(defn bytes-message
  "Wraps and serializes either a POCO or a throwable into a message object"
  ([x] (-create-bytes-message x))
  ([x msg-or-throwable]
   (doto ^BytesMessage (-create-bytes-message x)
     (.writeBytes (.array (fressian/write (wrap-msg msg-or-throwable) :handlers eva-fress/eva-messaging-write-handlers))))))

;; Error Discrimination

(def error-type->error-key
  {ActiveMQExceptionType/INTERNAL_ERROR            ::internal-error
   ActiveMQExceptionType/NOT_CONNECTED             ::not-connected
   ActiveMQExceptionType/CONNECTION_TIMEDOUT       ::connection-timeout
   ActiveMQExceptionType/DISCONNECTED              ::disconnected
   ActiveMQExceptionType/QUEUE_DOES_NOT_EXIST      ::queue-does-not-exist
   ActiveMQExceptionType/QUEUE_EXISTS              ::queue-exists
   ActiveMQExceptionType/OBJECT_CLOSED             ::object-closed
   ActiveMQExceptionType/INVALID_FILTER_EXPRESSION ::invalid-filter-expression
   ActiveMQExceptionType/ILLEGAL_STATE             ::illegal-state
   ActiveMQExceptionType/SECURITY_EXCEPTION        ::security-exception
   ;;ActiveMQExceptionType/ADDRESS_DOES_NOT_EXIST    ::address-does-not-exist
   ActiveMQExceptionType/ADDRESS_EXISTS            ::address-exists
   ActiveMQExceptionType/UNSUPPORTED_PACKET        ::unsupported-packet})

(def error-key->error-type (set/map-invert error-type->error-key))

(defn error-type [x]
  (condp instance? x
    ActiveMQExceptionType x
    JMSException (recur (.getCause ^JMSException x))
    JMSRuntimeException (recur (.getCause ^JMSRuntimeException x))
    ActiveMQException (.getType ^ActiveMQException x)
    Keyword (error-key->error-type x)))

(defn error-type= [x y] (= (error-type x) (error-type y)))

;; Request/Response Pattern

(defn add-connection-error-watch [r key f] (add-watch (:connection-error r) key f))
(defn remove-connection-error-watch [r key] (remove-watch (:connection-error r) key))
(defn connection-error [r] @(:connection-error r))
(defn connection-error? [r] (some? (connection-error r)))

(defn invalid-request?
  "Returns an ExceptionInfo if the provided request does not satisfy the spec"
  [spec request msg]
  (when (and spec (not (s/valid? spec request)))
    (error :messaging/invalid-request
           msg {:spec spec :request request :msg msg})))

;; Requestor Implementation
(defrecord Requestor [^ConnectionFactory connection-factory
                      ^JMSContext context
                      ^Destination request-queue
                      ^Destination reply-queue
                      ^JMSProducer request-producer
                      ^JMSConsumer reply-consumer
                      ^ConcurrentMap reply-futures
                      connection-error
                      outbound-spec
                      inbound-spec]
  AutoCloseable
  (close [_]
    (.close reply-consumer)
    (.close context))
  MessageCreation
  (-create-message [_] (.createMessage context))
  (-create-text-message [_] (.createTextMessage context))
  (-create-text-message [_ s] (.createTextMessage context s))
  (-create-bytes-message [_] (.createBytesMessage context)))

(declare requestor-message-listener)

(defn- ^ConcurrentMap weak-value-map [] (-> (MapMaker.) (.weakValues) (.makeMap)))

(defn requestor
  ([{:keys [connection-factory request-queue outbound-spec inbound-spec]}]
   {:pre [(instance? ConnectionFactory connection-factory)
          (instance? Queue request-queue)]}
   (let [connection-error (atom nil)
         context (doto (create-context connection-factory)
                   (.setExceptionListener (reify ExceptionListener
                                            (onException [_ ex]
                                              (log/error (sanitize ex) "connection error occurred in Requestor" request-queue)
                                              (swap! connection-error (constantly ex)))))
                   (.start))
         reply-queue (.createTemporaryQueue context)
         request-producer (producer context)
         reply-consumer (consumer context reply-queue)
         ;; use weak-value-map so that if sender discards the future,
         ;; then it will eventually be dropped from the map
         reply-futures (weak-value-map)
         req (map->Requestor {:context          context
                              :request-queue    request-queue
                              :reply-queue      reply-queue
                              :request-producer request-producer
                              :reply-consumer   reply-consumer
                              :reply-futures    reply-futures
                              :connection-error connection-error
                              :outbound-spec    outbound-spec
                              :inbound-spec     inbound-spec})]
     (.setMessageListener reply-consumer (requestor-message-listener reply-futures request-queue inbound-spec))
     req)))

(defn ^:private requestor-message-listener [^ConcurrentMap reply-futures
                                            request-queue
                                            inbound-spec]
  (let [format-str (format "inbound response %s from queue %s does not conform to spec %s"
                           "%s" request-queue inbound-spec)]
    (reify MessageListener
      (onMessage [_ reply]
        (let [cid (.getJMSCorrelationID reply)]
          (let [fut (.remove reply-futures cid)]
            (if (completable-future/promise? fut)
              (let [payload (bytes-message-payload reply)]
                (if-let [err (invalid-request? inbound-spec payload (format format-str payload))]
                  (completable-future/deliver fut err)
                  (completable-future/deliver fut payload)))
              (log/trace "message received but no pending request found; discarding message" reply))))))))

(defn ^CompletableFuture send-request!
  ([^Requestor r message-object]
   (when-some [err @(:connection-error r)]
     (raise ::connection-error
            (str "Requestor has error " err)
            {:connection-error err}
            err))
   (when-let [err (invalid-request? (:outbound-spec r)
                                    message-object
                                    (format "invalid outbound request %s on queue %s" message-object (:request-queue r)))] (throw err))
   (let [^Message message (bytes-message (:context r) message-object)
         ^JMSProducer producer (:request-producer r)
         ^Destination request-queue (:request-queue r)
         cid (-> (UUIDGenerator/getInstance) (.generateStringUUID))
         req-msg (doto message
                   (.setJMSCorrelationID cid)
                   (.setJMSReplyTo (:reply-queue r)))
         ^ConcurrentMap reply-futures (:reply-futures r)
         new-ret (completable-future/promise)
         existing-ret (.putIfAbsent reply-futures cid new-ret)]
     (when existing-ret (throw (IllegalStateException. (str "existing promise found for correlation-id: " cid))))
     (.send producer request-queue req-msg)
     new-ret)))

;; Responder Implementation
(defrecord Responder [^ConnectionFactory connection-factory
                      ^JMSContext context
                      ^Destination request-queue
                      ^JMSConsumer request-consumer
                      interceptors
                      request-handler
                      connection-error]
  AutoCloseable
  (close [_]
    (.close request-consumer)
    (.close context)))

(declare responder-message-listener)

(defn responder
  ([{:keys [connection-factory
            request-queue
            request-handler
            interceptors
            inbound-spec
            outbound-spec]}]
   {:pre [(instance? ConnectionFactory connection-factory)
          (instance? Queue request-queue)
          (fn? request-handler)]}
   (let [context (create-context connection-factory)
         request-consumer (doto (consumer context request-queue)
                            (.setMessageListener (responder-message-listener context
                                                                             request-queue
                                                                             inbound-spec
                                                                             request-handler)))
         connection-error (atom nil)
         r (map->Responder {:connection-factory connection-factory
                            :context            context
                            :request-queue      request-queue
                            :request-consumer   request-consumer
                            :request-handler    request-handler
                            :interceptors       interceptors
                            :connection-error   connection-error})]
     (.setExceptionListener context (reify ExceptionListener
                                      (onException [_ ex]
                                        (log/error (sanitize ex) "connection error occurred in Responder" request-queue)
                                        (swap! connection-error (constantly ex)))))
     (.start context)
     r)))

(defn rebuild-responder [^Responder r]
  (.close r)
  (responder r))

(defn ^:private responder-message-listener [context request-queue inbound-spec request-handler]
  (let [format-str (format "invalid inbound request %s on queue %s" "%s" request-queue)
        ;; NOTE: curries context, must be kept thread-safe
        ->message (fn ->message [rid msg] (doto ^Message (bytes-message context msg) (.setJMSCorrelationID rid)))]
    (reify MessageListener
      (onMessage [_ request]
        (let [rid (.getJMSCorrelationID request)
              respond-to (.getJMSReplyTo request)
              p (producer context)
              ^Message resp-msg
              (->message
               rid
               (try
                 (let [payload (bytes-message-payload request)]
                   (if-let [err (invalid-request? inbound-spec
                                                  payload
                                                  (format format-str payload))]
                     (do (log/warnf "ignoring non-conforming inbound request %s on queue %s from %s. Responding with exception."
                                    payload request-queue respond-to)
                         err)
                     (request-handler payload)))
                 (catch Throwable e e)))]
          (.send p respond-to resp-msg))))))

;; Publisher
(defrecord Publisher [^ConnectionFactory connection-factory
                      ^JMSContext context
                      ^Topic topic
                      ^JMSProducer producer
                      outbound-spec
                      connection-error]
  AutoCloseable
  (close [_] (.close context)))

(defn publisher
  "Builds a publisher object for unidirectional async messaging to multiple consumers"
  [{:keys [connection-factory
           context
           topic
           outbound-spec]}]
  {:pre [(instance? ConnectionFactory connection-factory)
         (instance? Topic topic)]}
  (let [connection-error (atom nil)
        context (doto (create-context connection-factory)
                  (.setExceptionListener (reify ExceptionListener
                                           (onException [_ ex]
                                             (log/error (sanitize ex) "connection error occurred in Publisher" topic)
                                             (swap! connection-error (constantly ex)))))
                  (.start))
        producer (producer context)]
    (map->Publisher
     {:connection-factory connection-factory
      :connection-error connection-error
      :context context
      :topic topic
      :producer producer
      :outbound-spec outbound-spec})))

;; TODO: asyncify
(defn publish!
  ([^Publisher p message-object]
   (when-some [err @(:connection-error p)]
     (raise ::connection-error
            (str "Publisher has error " err)
            {:connection-error err}
            err))
   (when-let [err (invalid-request? (:outbound-spec p)
                                    message-object
                                    (format "invalid publish %s on topic %s" message-object (:request-queue p)))] (throw err))
   (let [^Message message (bytes-message (:context p) message-object)
         ^JMSProducer producer (:producer p)
         ^Destination topic (:topic p)
         cid (-> (UUIDGenerator/getInstance) (.generateStringUUID))
         req-msg (doto message
                   (.setJMSCorrelationID cid)
                   (.setJMSReplyTo (:reply-queue p)))]
     (.send producer topic req-msg))))

;; Subscriber

(defrecord Subscriber [^ConnectionFactory connection-factory
                       ^Topic topic
                       ^JMSConsumer subscription-consumer
                       ^JMSContext context
                       connection-error]
  AutoCloseable
  (close [_]
    (.close subscription-consumer)
    (.close context)))

(defn ^:private subscription-listener [subscriber-id topic inbound-spec handler]
  (let [format-str (format "inbound publish %s from topic %s to subscriber %s does not conform to spec %s"
                           "%s" topic subscriber-id inbound-spec)]
    (reify MessageListener
      (onMessage [_ reply]
        (let [payload (bytes-message-payload reply)]
          (if-let [err (invalid-request? inbound-spec payload (format format-str payload))]
            (warn (sanitize err) "discarding nonconforming publish")
            ;; future to exit the messaging handler asap
            (future
              (try (handler payload)
                   (catch Throwable t
                     (log/warnf t
                                "handler for %s on topic %s threw unexpected exception."
                                subscriber-id
                                topic))))))))))

(defn subscriber [{:keys [connection-factory
                          topic
                          subscriber-id
                          inbound-spec
                          handler]}]
  {:pre [(instance? ConnectionFactory connection-factory)
         (instance? Topic topic)
         (fn? handler)]}
  (let [context (create-context connection-factory)
        connection-error (atom nil)
        subscription-consumer
        (doto (consumer context topic)
          (.setMessageListener (subscription-listener subscriber-id topic inbound-spec handler)))]
    (.setExceptionListener context (reify ExceptionListener
                                     (onException [_ ex]
                                       (log/error (sanitize ex) "connection error occurred in Subscriber" topic subscriber-id)
                                       (swap! connection-error (constantly ex)))))
    (.start context)
    (map->Subscriber {:connection-factory connection-factory
                      :context context
                      :topic topic
                      :subscription-consumer subscription-consumer
                      :inbound-spec inbound-spec
                      :connection-error connection-error
                      :handler handler})))
