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

(ns eva.v2.messaging.jms.beta.patterns
  (:require [clojure.spec.alpha :as s]
            [eva.v2.messaging.jms.beta.core :as jms])
  (:import (java.util.concurrent ConcurrentMap CompletableFuture)
           (com.google.common.collect MapMaker)
           (javax.jms Connection Session Queue MessageProducer MessageConsumer MessageListener Message Destination JMSException Topic)
           (java.lang AutoCloseable)
           (java.time Instant)
           (java.util.function BiFunction)))

;; ## The Exscriber
;;
;; A function which transports the exscript into a sideband.
;; If the exscriber is nil, then it is treated as a no-op, and the exscript is discarded.
(def ^:private ^:redef exscriber (constantly nil))

;; ### Setting the Exscriber
;;
;; Use `set-exscriber!` to globally set the exscriber for this namespace.
;; While thread-safe, this should generally be done at system-startup before
;; starting any components produced from this namespace.
(defn set-exscriber! [exscribe]
  (when-not (fn? exscribe) (throw (IllegalArgumentException. "not a function")))
  (alter-var-root #'exscriber (constantly exscribe))
  (await exscriber))

;; ## Exscribing within this namespace
;;
;; Use the `exscribe!` function within this namespace to transport information
;; into the sideband.
(defn ^:private ->exscript
  ([type] (->exscript type {}))
  ([type data]
   (let [t (Thread/currentThread)
         base-data {:exscript.information/type   type
                    :exscript.origin/inst        (Instant/now)
                    :exscript.origin.thread/id   (.getId t)
                    :exscript.origin.thread/name (.getName t)}]
     (into base-data data))))

(defn ^:private exscribe!
  ([type data] (exscribe! exscriber type data))
  ([exscriber type data] (try (exscriber (->exscript type data))
                              (catch Throwable e nil))))

;; # Message Semantics

;; ## Message Expiration
;;
;; Messages produced by components in this namespace:
;;
;; 1. MUST set an explicit message expiration on all produced messages.
;; 2. MAY use a `:message-expiration` value provided during component creation.
;; 3. MUST DEFAULT the `:message-expiration` value to `*default-message-expiration*`
(def ^:dynamic *default-message-expiration* (* 1 60 60 1000)) ;; 1 hour

;; ## Message Types
;;
;; Messages produced by components in this namespace
;; MUST contain a `::message-type` header containing one of:
(def ^:private message-types #{::request ::reply ::broadcast ::error})

;; Keywords in message-properties are transported as strings of the form: `"namespace/name"`
(defn ^:private kw-str [k]
  (when-not (keyword? k) (throw (IllegalArgumentException. "keyword required")))
  (if-some [ns (namespace k)]
    (str ns "/" (name k))
    (throw (IllegalArgumentException. "keyword namespace required"))))

;; Use `message-type` to read the `::message-type` of messages.

#_(defn ^:private read-message-type [^Message m] (some-> m (.getStringProperty message-type-header) (keyword)))

(defn read-message-type [^Message m] (some-> m (.getJMSType) (keyword)))

(defn set-message-type! [^Message m t]
  {:pre [(keyword? t)]}
  (.setJMSType m (kw-str t)))

(defn ^:private message-exscript [^Message m]
  (letfn [(dest-data [^Destination dest]
            (condp instance? dest
              Queue {:jms/queue (.getQueueName ^Queue dest)}
              Topic {:jms/topic (.getTopicName ^Topic dest)}))]
    (when m
      {:jms.message/id             (.getJMSMessageID m)
       :jms.message/timestamp      (some-> m (.getJMSTimestamp) (Instant/ofEpochMilli))
       :jms.message/correlation-id (.getJMSCorrelationID m)
       :jms.message/reply-to       (some-> m (.getJMSReplyTo) (dest-data))
       :jms.message/delivery-mode  (.getJMSDeliveryMode m)
       :jms.message/destination    (some-> m (.getJMSDestination) (dest-data))
       :jms.message/redelivered?   (.getJMSRedelivered m)
       :jms.message/type           (.getJMSType m)
       :jms.message/expiration     (some-> m (.getJMSExpiration) (Instant/ofEpochMilli))
       :jms.message/priority       (.getJMSPriority m)})))

;; ## Encoding, Decoding, and Transferring Domain Information
;;
;; Messages are NOT the primary domain objects of most systems.
;; Rather messages encode and transport domain information between
;; distributed sub-systems *using* messages as the transport container.
;;
;; Message construction and encoding is abstracted into the `MessageProtocol`:

(defprotocol MessageProtocol
  ;; ### Data Messages
  ;;
  ;; Data messages convey the primary information
  ;; about the information domain being communicated.
  ;; The type of `Message` used, the method of identifying
  ;; a data message, and the body encoding/decoding logic
  ;; is encapsulated in the following protocol functions:
  (new-data-message [mc session])
  (data-message? [mp msg])
  (write-data-message [mc msg data])
  (read-data-message [mc msg])

  ;; ### Error messages
  ;;
  ;; Error messages convey error states and information.
  ;; The method of creating and encoding error-messages
  ;; is separated from data-messages. For example, you might
  ;; which to use java-serialization to transport the Exception
  ;; objects across the wire, while using a different encoding
  ;; method for your data-messages.
  (new-error-message [mc session])
  (error-message? [mp msg])
  (write-error-message [mc msg err])
  (read-error-message [mc msg]))

(s/def ::message-protocol #(satisfies? MessageProtocol %))

;; ### Example
;;
;; Here is an example of a `MessageProtocol` that implements encoding for data-messages using
;; `javax.jms.TextMessage` and EDN serialization. However error-messages are encoded using
;; `javax.jms.ObjectMessage` and java-serialization.
;;
(comment
  (def edn-data+serialized-errors
    (reify
      MessageProtocol
      (data-message? [_ msg] (not= ::error (read-message-type msg)))
      (new-data-message [_ session] (.createTextMessage ^Session session))
      (write-data-message [_ msg data] (.setText ^javax.jms.TextMessage msg (pr-str data)))
      (read-data-message [_ msg] (some-> ^javax.jms.TextMessage msg
                                         (.getText)
                                         (clojure.edn/read-string)))
      (error-message? [_ msg] (= ::error (read-message-type msg)))
      (new-error-message [_ session] (doto (.createObjectMessage ^Session session)
                                       (set-message-type! ::error)))
      (write-error-message [_ msg err] (.setObject ^ObjectMessage msg err))
      (read-error-message [_ msg] (.getObject ^ObjectMessage msg))))

  (let [domain-data {:x 1 :y 2 :z 3}
        session (stuff-to-create-the-session!)
        request-msg (-> (new-data-message edn-message-protocol session)
                        (write-data-message domain-data)
                        (writeMessageBody domain-data))]
    (assert (data-message? request-msg))
    (assert (= domain-data (read-data-message request-message)))))

;; # Conditional Locking
;;
;; Sessions (and session derived objects like Producers) are not thread safe.
;; Components in this namespace may optionally support concurrent use of Sessions
;; by holding a `:concurrent?` boolean field, and passing the value to `maybe-locking`
;; as well as the Session object to lock on. If the value of the `concurrent?` form
;; is true, then the body is executed inside a lock on the Session. Otherwise the body
;; executes without a lock.
(defmacro ^:private maybe-locking [concurrent? lock-on & body]
  `(let [body-fn# (fn [] ~@body)]
     (if ~concurrent?
       (locking ~lock-on (body-fn#))
       (body-fn#))))

;; # Request/Response Pattern
;;
;; From [Wikipedia](https://en.wikipedia.org/wiki/Request%E2%80%93response):

;; > Request–response is a message exchange pattern in which a requestor
;; > sends a request message to a replier system which receives and processes
;; > the request, ultimately returning a message in response. This is a simple,
;; > but powerful messaging pattern which allows two applications to have a
;; > two-way conversation with one another over a channel. This pattern is
;; > especially common in client–server architectures.
;;

;; A Requestor:
;;
;; - is the origination point for a request-response cycle;
;; - is created from a JMS `Connection`;
;; - implements `AutoClosable`, and should be closed when no longer in use;
;; - holds 1 active JMS `Session`;
;; - is thread-safe only when `(true? (:concurrent? a-requestor))`;
;; - sends request-messages to a *specific* destination queue;
;; - listens for reply-messages on a temporary queue that is deleted when the the requestor is closed;
;; - returns a `CompletableFuture` in response to `send-request!` calls.

(defrecord Requestor [^Boolean concurrent?                  ; Will the requestor be used from multiple threads?
                      ;; JMS Objects
                      ^Session session                      ; Session used for requests
                      ^Queue request-queue                  ; Destination for requests
                      ^Queue reply-queue                    ; Requestor-specific destination for replies
                      ^MessageProducer request-producer     ; Used to submit the requests
                      ^MessageConsumer reply-consumer       ; Consumes reply messages
                      ;; Pending replies
                      ^ConcurrentMap reply-futures          ; Futures for requests awaiting a response
                      ;; Request Message Settings
                      request-delivery-mode                 ; Controls durability of request messages
                      request-expiration                    ; Expiration/TTL of requests
                      request-type                          ; keyword denoting that the message is a request
                      reply-type                            ; keyword denoting that the message is a reply
                      error-type                            ; keyword denoting that the message is an error
                      ;; MessageProtocol
                      message-protocol]
  AutoCloseable
  (close [_]
    ;; close the producer first to prevent new requests
    (.close request-producer)
    ;; close the consumer second to allow any last minute replies
    (.close reply-consumer)
    ;; finally close the session
    (.close session)))

(s/def ::concurrent? boolean?)
(s/def ::session #(instance? Session %))
(s/def ::request-queue (s/or :name string?
                             :queue #(instance? Queue %)))

(defn requestor?
  "Tests if x is a Requestor"
  [x]
  (instance? Requestor x))

(declare requestor-message-listener)

;; A concurrent, weak-value map allows map-values to be garbage-collected when no
;; other references to a map-value exist. The map will auto-clean removed values
;; when accessed.
(defn ^:private ^ConcurrentMap weak-value-map
  "Constructs a new, empty ConcurrentMap which uses WeakReferences
  to hold its values. Values held in this map may be garbage-collected
  if not held by any other references."
  []
  (-> (MapMaker.) (.weakValues) (.makeMap)))

(defn ^Requestor requestor
  "Constructs a new Requestor on the JMS connection.
  The following options are accepted:

    - :request-queue         :: required. The name of the queue where requests will be sent.

    - :message-protocol      :: required. Implementation of MessageProtocol used to encode request messages
                                 and decode response messages.

    - :concurrent?           :: optional, default=true. When true, the requestor is thread-safe under concurrency.
                                 When false the requestor is NOT thread-safe.

    - :request-delivery-mode :: optional, default=:non-persistent. The delivery-mode used for request messages.

    - :request-expiration    :: optional, default=*default-message-expiration*. The time-to-live for request messages."
  ([connection
    {:keys [concurrent?
            request-queue
            request-delivery-mode
            request-expiration
            message-protocol]
     :or {concurrent? true
          request-delivery-mode :non-persistent
          request-expiration *default-message-expiration*}}]
   {:pre [(instance? Connection connection)
          (integer? request-expiration)
          (satisfies? MessageProtocol message-protocol)]}
   (let [;; Use weak-value-map so that if sender discards the future,
         ;; then it will eventually be GC'd and dropped from the map.
         reply-futures (weak-value-map)
         session (jms/create-session connection {:acknowledge-mode :auto})
         request-queue (jms/queue session request-queue)
         reply-queue (.createTemporaryQueue session)
         request-producer (jms/message-producer session request-queue {:delivery-mode request-delivery-mode
                                                                       :message-ttl   request-expiration})
         reply-consumer (.createConsumer session reply-queue)
         req (map->Requestor {:session                session
                              :concurrent?            concurrent?
                              :request-queue          request-queue
                              :request-producer       request-producer
                              :request-delivery-mode  request-delivery-mode
                              :request-expiration     request-expiration
                              :reply-queue            reply-queue
                              :reply-consumer         reply-consumer
                              :reply-futures          reply-futures
                              :message-protocol       message-protocol})]
     (.setMessageListener reply-consumer (requestor-message-listener req))
     req)))

;; `complete-future!` is defined here with 2 arities as a transducer, transform function.
;; This allows complete-future! to be composed with a user-provided `reply-xform` transducer
;; that transforms the reply-message before delivery to the reply-future.
(defn ^:private complete-future!
  "Delivers a value to a CompletableFuture. If value is Throwable, then
  CompletableFuture#completeExceptionally is use.
  Single-arity call is a no-op for use in reducing or transducing contexts."
  ([^CompletableFuture fut] fut)
  ([^CompletableFuture fut val]
   (if (instance? Throwable val)
     (.completeExceptionally fut val)
     (.complete fut val))))

(declare requestor-on-reply)

(defn ^:private requestor-message-listener
  "Creates the MessageListener used by a Requestor to receive response messages."
  [{:as requestor
    :keys [^ConcurrentMap reply-futures
           ^Queue request-queue
           ^Queue reply-queue
           message-protocol]}]
  (let [listener-info {::pattern       ::request-response
                       ::roles         #{::requestor ::requestor-message-listener}
                       :java/class     (.getName MessageListener)
                       :java/method    "onMessage"
                       ::request-queue (.getQueueName request-queue)
                       ::reply-queue   (.getQueueName reply-queue)}
        context {::reply-futures reply-futures
                 ::message-protocol message-protocol
                 ::listener-info listener-info}]
    (reify MessageListener
      (onMessage [_ reply] (requestor-on-reply context reply)))))

(defn ^:private requestor-on-reply
  [{:keys [::reply-futures
           ::message-protocol
           ::listener-info]}
   reply]
  (let [cid (.getJMSCorrelationID reply)
        reply-info (merge listener-info {::request-id cid
                                         ::reply-id   (.getJMSMessageID reply)})
        delivered-reply-future? (atom false)]
    ;; Use computeIfPresent to trigger delivery of completable future *only* if one is present
    (.computeIfPresent
     ^ConcurrentMap reply-futures
     cid
     (reify BiFunction
       (apply [_ _ fut]
         (try
           (complete-future!
            fut
            (cond
              (data-message? message-protocol reply) (read-data-message message-protocol reply)
              (error-message? message-protocol reply) (read-error-message message-protocol reply)
              :else (throw (IllegalStateException. "unknown message"))))
           (reset! delivered-reply-future? true)
           ;; attempt exscription, but proceed even if it fails
           (try
             (exscribe! ::request-progress (merge reply-info {::request-progress ::delivered-reply}))
             (catch Throwable e nil))
            ;; return nil to trigger removal of future from reply-futures map
           nil
           (catch Throwable e
             (complete-future! ^CompletableFuture fut e)
             (reset! delivered-reply-future? true)
              ;; attempt exscription, but proceed even if it fails
             (try
               (exscribe! ::request-progress (merge reply-info {::request-progress ::delivered-error}))
               (catch Throwable e nil))
              ;; return nil to trigger removal of future from reply-futures map
             nil)))))
    ;; if no reply future existed, then @delivered-reply-future? == false,
    ;; and the message listener should exist cleanly to indicate message
    ;; acknowledgement which results in the reply being dropped
    (when-not @delivered-reply-future?
      ;; attempt exscription, but proceed even if it fails
      (try
        (exscribe! ::request-progress (merge reply-info {::response-progress ::dropped-reply}))
        (catch Throwable e nil)))))

(defn rebuild-requestor
  "Closes Requestor r and then creates a new Requestor
  on Connection c that uses the same settings as the original
  Requestor r."
  [^Connection c ^Requestor r]
  (.close r)
  (requestor c r))

(defn ^CompletableFuture send-request!
  "Uses Requestor r to send data to a remote Responder service."
  ([^Requestor r data]
   (let [concurrent? (:concurrent? r)
         ^Session session (:session r)
         ^MessageProducer producer (:request-producer r)
         ^Queue request-queue (:request-queue r)
         ^Queue reply-queue (:reply-queue r)
         ^ConcurrentMap reply-futures (:reply-futures r)
         message-protocol (:message-protocol r)
         write-data! (partial write-data-message message-protocol)
         fut-reply (CompletableFuture.)
         cid (str (random-uuid))
         request-info {:log/level             :trace
                       :clojure.function/name `send-request!
                       ::request-queue        (jms/destination-name request-queue)
                       ::reply-queue          (jms/destination-name reply-queue)
                       ::request-id           cid}]
     (if-some [existing-ret (.putIfAbsent reply-futures cid fut-reply)]
       (throw (IllegalStateException. (str "existing promise found for correlation-id: " cid)))
       (do
         (exscribe! ::request-progress (assoc request-info ::request-progress ::before-request-send))
         (maybe-locking concurrent? session                 ; if requestor marked concurrent, protectively lock session-based operations
                        (try
                          (let [^Message req-msg (doto (new-data-message message-protocol session)
                                                   (set-message-type! ::request)
                                                   (.setJMSCorrelationID cid)
                                                   (.setJMSReplyTo reply-queue)
                                                   (write-data! data))]
                            (jms/send-message! producer req-msg)
                            (exscribe! ::request-progress (assoc request-info ::request-progress ::after-request-send))
                            fut-reply))))))))

;; ### Requestor Example
;;
(comment
  (require '[eva.v2.messaging.jms.beta.connection-factory :refer [new-connection-factory]])
  (require '[eva.v2.messaging.jms.beta.core :as jms])
  (def my-connection-factory (new-connection-factory "org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory"
                                                     "tcp://localhost:5445"
                                                     "test-user"
                                                     "test-password"))
  (let [connection (jms/create-connection my-connection-factory)
        service-requestor (requestor my-connection
                                     {:request-queue "some.service"
                                      :message-protocol edn-data+serialized-errors})]
    (.start connection)
    (try
      (println @(send-request! service-requestor {:function 'clojure.core/merge
                                                  :args     [{:x 1} {:y 2}]}))
      (finally (.close service-requestor)
               (.stop connection)))))

;; ## Responder
;;
(defrecord Responder [^Session session
                      ^Session listener-session
                      ^Queue request-queue
                      ^MessageConsumer request-consumer
                      message-protocol
                      request-handler
                      reply-delivery-mode
                      reply-expiration]
  AutoCloseable
  (close [_]
    (.close request-consumer)
    (.close session)))

(declare responder-message-listener)

(defn ^Responder responder
  "Creates a new Responder on connection using the settings specified
  in the option map:

    - :request-queue       :: required, the name of the queue to listen on for requests.

    - :message-protocol    :: required, implementation of MessageProtocol that defines
                              how request messages are decoded/read and how response messages
                              are created/encoded.

    - :request-handler     :: required, a function that accepts the decoded request data and returns
                              the data that should be sent in response.

    - :reply-delivery-mode :: optional, the delivery-mode to use for reply/response messages.

    - :reply-expiration    :: optional, the time-to-live for reply/response messages."
  ([connection
    {:keys [request-queue
            message-protocol
            request-handler
            reply-delivery-mode
            reply-expiration]}]
   {:pre [(instance? Connection connection)
          (fn? request-handler)
          (satisfies? MessageProtocol message-protocol)]}
   (let [session (jms/create-session connection {:acknowledge-mode :auto})
         request-queue (jms/queue session request-queue)
         request-consumer (.createConsumer session request-queue)
         listener-session (jms/create-session connection {:acknowledge-mode :auto})
         responder (map->Responder {:session             session
                                    :listener-session    listener-session
                                    :request-queue       request-queue
                                    :request-consumer    request-consumer
                                    :message-protocol    message-protocol
                                    :request-handler     request-handler
                                    :reply-delivery-mode reply-delivery-mode
                                    :reply-expiration    reply-expiration})]
     (.setMessageListener  request-consumer (responder-message-listener responder))
     responder)))

(defn responder?
  "Tests if x is a Responder"
  [x]
  (instance? Responder x))

(defn rebuild-responder
  "Closes Responder r and then creates a new Responder
  on Connection c that uses the same settings as the original
  responder r."
  [^Connection c ^Responder r]
  (.close r)
  (responder c r))

;; ## Cases for processing a request message
;; 1. Requests that CANNOT be replied to:
;;    a. Request lacks reply-to destination; could not route to requestor
(defn ^:private on-request-without-reply-to [responder ^Message request]
  (exscribe! ::request-progress
             (merge
              (message-exscript request)
              {:log/level              :trace
               :log/message            "dropping request without reply-to address"
               ::request-progress      ::dropped-request
               ::dropped-request       ::no-reply-address
               ::request-acknowledged? true
               ::request-processed?    false
               ::request-replied?      false})))

;;    b. Request lacks correlation-id; could route, but requestor cannot deliver to reply-future
(defn ^:private on-request-without-correlation-id [responder ^Message request]
  (exscribe! ::request-progress
             (merge
              (message-exscript request)
              {:log/level              :trace
               :log/message            "dropping request without correlation-id"
               ::request-progress      ::dropped-request
               ::dropped-request       ::no-correlation-id
               ::request-acknowledged? true
               ::request-processed?    false
               ::request-replied?      false})))

;; 2. pass request to handler
;;
(defn ^:private on-processable-request [{:as responder
                                         :keys [listener-session
                                                reply-delivery-mode
                                                reply-expiration
                                                message-protocol
                                                request-handler]}
                                        ^Message request]
  (let [write-data! (partial write-data-message message-protocol)
        write-error! (partial write-error-message message-protocol)
        cid (.getJMSCorrelationID request)
        ^Destination reply-to (.getJMSReplyTo request)
        ^MessageProducer producer (jms/message-producer listener-session reply-to
                                                        {:delivery-mode reply-delivery-mode
                                                         :message-ttl   reply-expiration})
        request-data (read-data-message message-protocol request)]
    (try
      (let [reply-data (request-handler request-data)]
        (jms/send-message! producer (doto (new-data-message message-protocol listener-session)
                                      (.setJMSCorrelationID cid)
                                      (write-data! reply-data))))
      (catch Throwable reply-err
        (jms/send-message! producer (doto (new-error-message message-protocol listener-session)
                                      (.setJMSCorrelationID cid)
                                      (write-error! reply-err)))))))

;; ## Responder Message Listener
;;
;; Aggregates the previous request-cases.
;;
(defn ^:private responder-message-listener
  "Constructs the MessageListener used by a Requestor to listen for request messages"
  [responder]
  (reify MessageListener
    (onMessage [_ request]
      (cond
        ;; 1. Cannot Reply
        ;; 1.a
        (nil? (.getJMSReplyTo request)) (on-request-without-reply-to responder request)
        ;; 1.b
        (nil? (.getJMSCorrelationID request)) (on-request-without-correlation-id responder request)
        ;; 2. Processable Request
        :else (on-processable-request responder request)))))

;; # Publish/Subscribe Pattern
;;

;; ## Publisher
;;
(defrecord Publisher [^Session session
                      ^Boolean concurrent?
                      ^Topic topic
                      delivery-mode
                      time-to-live
                      ^MessageProducer producer
                      message-protocol]
  AutoCloseable
  (close [_]
    (.close producer)
    (.close session)))

(defn publisher? [x] (instance? Publisher x))

;; ### Constructing a Publisher
;;
(defn publisher
  "Builds a publisher object for unidirectional async messaging to multiple consumers"
  [^Connection connection
   {:keys [concurrent?
           topic
           delivery-mode
           time-to-live
           message-protocol]
    :or {concurrent? true
         delivery-mode :non-persistent
         time-to-live *default-message-expiration*}}]
  {:pre [(satisfies? MessageProtocol message-protocol)]}
  (let [session (jms/create-session connection {:acknowledge-mode :auto})
        topic (jms/topic session topic)
        producer (jms/message-producer session topic {:delivery-mode delivery-mode
                                                      :message-ttl   time-to-live})]
    (map->Publisher
     {:session session
      :concurrent? concurrent?
      :topic topic
      :producer producer
      :message-protocol message-protocol})))

;; ### Publishing a message
;;
(defn publish!
  "Broadcasts a message to the Topic of the publisher"
  ([{:as publisher
     :keys [^Session session
            concurrent?
            message-protocol
            ^MessageProducer producer]}
    data]
   (maybe-locking concurrent? session
                  (let [^Message message (new-data-message message-protocol session)]
                    (.send producer (write-data-message message-protocol message data))))))

;; ## Subscriber
;;
(defrecord Subscriber [^Session session
                       ^Topic topic
                       ^MessageConsumer consumer
                       message-protocol
                       handler]
  AutoCloseable
  (close [_]
    (.close consumer)
    (.close session)))

(defn subscriber? [x] (instance? Subscriber x))

;; ### Creating a Subscriber
;;

(declare subscription-listener)

(defn subscriber
  "Creates a new Subscriber on the connection that will
  listen for messages broadcast to the topic.

  The following options are required:
  - :topic            :: required, the name of the topic to subscribe to.
  - :message-protocol :: required, an instance of MessageProtocol used to decode messages.
  - :handler          :: a function of 2 arguments s (the Subscriber) and d (the data contained the message),
                         called whenever a message is received."
  [^Connection connection
   {:keys [topic
           message-protocol
           handler]}]
  {:pre [(some? topic)
         (satisfies? MessageProtocol message-protocol)
         (fn? handler)]}
  (let [session (jms/create-session connection {:acknowledge-mode :auto})
        topic (jms/topic session topic)
        consumer (.createConsumer session topic)
        sub (map->Subscriber {:session session
                              :topic topic
                              :consumer consumer
                              :message-protocol message-protocol
                              :handler handler})]
    (.setMessageListener consumer (subscription-listener sub))
    sub))

(defn rebuild-subscriber
  "Closes Subscriber s and then creates a new Subscriber with the same settings
  on Connection c"
  [^Connection c ^Subscriber s]
  (.close s)
  (subscriber c s))

(defn ^:private subscription-listener
  "Creates the MessageListener used by Subscribers to listen for messages on their Topic"
  [{:as subscriber :keys [message-protocol handler topic]}]
  {:pre [(fn? handler)]}
  (reify MessageListener
    (onMessage [_ msg]
      (let [msg-info (message-exscript msg)]
        (exscribe! ::published-message (merge msg-info
                                              {:log/level :trace
                                               ::publish-progress ::message-received}))
        (try (handler (read-data-message message-protocol msg))
             (exscribe! ::published-message (merge msg-info
                                                   {:log/level :trace
                                                    ::publish-progress ::message-processed}))
             (catch Throwable e
               (if-not (.getJMSRedelivered msg)
                 ;; message has never been redelivered before,
                 ;; so allow one retry to rethrowing exception
                 ;; preventing successful return of onMessage
                 ;; which prevents auto acknowledgement
                 (do (exscribe! ::published-message (merge msg-info
                                                           {::publish-progress ::error-processing-message
                                                            :log/level :warn
                                                            :log/message "error handling message; allowing redelivery"
                                                            :log/exception e
                                                            ::allow-redelivery? true}))
                     (throw e))
                 ;; Otherwise message *is* being redelivered,
                 ;; so we'll consider this failure permanent,
                 ;; allow onMessage to return successfully,
                 ;; which acknowledges the message
                 (exscribe! ::published-message (merge msg-info
                                                       {::publish-progress ::error-processing-message
                                                        :log/level         :warn
                                                        :log/message "error handling messages; redelivery will NOT be attempted"
                                                        :log/exception     e
                                                        ::allow-redelivery? false})))))))))
