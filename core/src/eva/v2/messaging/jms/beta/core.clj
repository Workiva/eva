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

(ns eva.v2.messaging.jms.beta.core
  (:require [clojure.spec.alpha :as s])
  (:import (java.time Instant)
           (javax.jms Destination
                      Session
                      MessageProducer
                      Topic
                      Queue
                      Connection
                      ConnectionFactory
                      DeliveryMode
                      BytesMessage
                      Message
                      MessageConsumer
                      MessageListener)))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::delivery-mode-value #{DeliveryMode/PERSISTENT
                               DeliveryMode/NON_PERSISTENT})
(s/def ::delivery-mode-keyword #{:persistent :non-persistent})
(s/def ::delivery-mode (s/or :name ::delivery-mode-keyword
                             :value ::delivery-mode-value))

(defn delivery-mode-keyword
  "Coerces a valid delivery-mode value to the human readable
  keywords:
    - :persistent
    - :non-persistent

  If passed, these keywords are returned unchanged.

  Any other value will result in an IllegalArgumentException."
  [mode]
  (condp = mode
    DeliveryMode/PERSISTENT :persistent
    DeliveryMode/NON_PERSISTENT :non-persistent
    :persistent :persistent
    :non-persistent :non-persistent))

(s/fdef delivery-mode-keyword
  :args (s/cat :mode ::delivery-mode)
  :ret ::delivery-mode-keyword)

(defn ^Integer delivery-mode-value
  "Coerces a representation of a delivery mode to
  the integer value recognized by JMS.
  Valid keyword inputs are:
    - :persistent
    - :non-persistent

  If a valid integer value is passed, it will be returned
  unchanged. Invalid integer values will throw an
  IllegalArgumentException.

  Any other value will result in an IllegalArgumentException."
  [mode]
  (condp = mode
    DeliveryMode/PERSISTENT DeliveryMode/PERSISTENT
    DeliveryMode/NON_PERSISTENT DeliveryMode/NON_PERSISTENT
    :persistent DeliveryMode/PERSISTENT
    :non-persistent DeliveryMode/NON_PERSISTENT))

(s/fdef delivery-mode-value
  :args (s/cat :mode ::delivery-mode)
  :ret ::delivery-mode-value)

(s/def ::acknowledge-mode-value #{Session/AUTO_ACKNOWLEDGE
                                  Session/CLIENT_ACKNOWLEDGE
                                  Session/DUPS_OK_ACKNOWLEDGE})

(s/def ::acknowledge-mode-keyword #{:auto :client :dups-ok})
(s/def ::acknowledge-mode (s/or :name ::acknowledge-mode-keyword
                                :value ::acknowledge-mode-value))

(defn acknowledge-mode-keyword
  "Coerces a valid acknowledge-mode value to the human-readable
  keywords:
    - :auto
    - :client
    - :dups-ok

  If any of these keywords are passed, they will be returned unchanged.
  Any other value will result in an IllegalArgumentException."
  [mode]
  (condp contains? mode
    #{:auto Session/AUTO_ACKNOWLEDGE} :auto
    #{:client Session/CLIENT_ACKNOWLEDGE} :client
    #{:dups-ok Session/DUPS_OK_ACKNOWLEDGE} :dups-ok
    (throw (IllegalArgumentException. (str "invalid acknowledge-mode: " mode ";"
                                           "must be one of: "
                                           ":auto, :client, :dups-ok, "
                                           "java.jms.Session/AUTO_ACKNOWLEDGE, "
                                           "java.jms.Session/CLIENT_ACKNOWLEDGE, "
                                           "or java.jms.Session/DUPS_OK_ACKNOWLEDGE")))))

(s/fdef acknowledge-mode-keyword
  :args (s/cat :mode ::acknowledge-mode)
  :ret ::acknowledge-mode-keyword)

(defn acknowledge-mode-value
  "Coerces the value to a JMS acknowledge-mode integer value.
  If a valid acknowledge-mode integer value is passed, it will be
  returned unchanged.
  The following keywords are also accepted:
    - :auto -> javax.jms.Session/AUTO_ACKNOWLEDGE
    - :client -> javax.jms.Session/CLIENT_ACKNOWLEDGE
    - :dups-ok -> javax.jms.Session/DUPS_OK_ACKNOWLEDGE

    Any other value will result in an IllegalArgumentException."
  [mode]
  (condp contains? mode
    #{:auto Session/AUTO_ACKNOWLEDGE} Session/AUTO_ACKNOWLEDGE
    #{:client Session/CLIENT_ACKNOWLEDGE} Session/CLIENT_ACKNOWLEDGE
    #{:dups-ok Session/DUPS_OK_ACKNOWLEDGE} Session/DUPS_OK_ACKNOWLEDGE
    (throw (IllegalArgumentException. (str "invalid acknowledge-mode: " mode ";"
                                           "must be one of: "
                                           ":auto, :client, :dups-ok, "
                                           "java.jms.Session/AUTO_ACKNOWLEDGE, "
                                           "java.jms.Session/CLIENT_ACKNOWLEDGE, "
                                           "or java.jms.Session/DUPS_OK_ACKNOWLEDGE")))))

(s/fdef acknowledge-mode-value
  :args (s/cat :mode ::acknowledge-mode)
  :ret ::acknowledge-mode-value)

(defn connection-factory?
  "Predicate that tests if the argument is a javax.jms.ConnectionFactory"
  [cf]
  (instance? ConnectionFactory cf))

(s/def ::connection-factory connection-factory?)

(defn connection?
  "Predicate that tests if the argument is a javax.jms.Connection"
  [c]
  (instance? Connection c))

(s/def ::connection connection?)
(s/def ::connection-opts (s/keys :opt-un [::start? ::user ::password]))
(s/def ::start? boolean?)
(s/def ::user string?)
(s/def ::password string?)

(defn ^Connection create-connection
  "Produces a javax.jms.Connection from a javax.jms.ConnectionFactory"
  ([^ConnectionFactory cf]
   (create-connection cf {}))
  ([^ConnectionFactory cf
    {:as opts
     :keys [start? user password]
     :or {start? false}}]
   (cond (and user password) :ok
         user (throw (IllegalArgumentException.
                      (str "Incomplete connection options: provided :user but missing :password")))
         password (throw (IllegalArgumentException.
                          (str "Incomplete connection options: provided :password missing :user"))))
   (let [c (if user
             (.createConnection cf user password)
             (.createConnection cf))]
     (when start? (.start c))
     c)))

(s/fdef create-connection
  :args (s/cat :cf connection-factory? :opts (s/? ::connection-opts))
  :ret connection?)

(defn session?
  "Predicate that tests if the argument is a javax.jms.Session"
  [s]
  (instance? Session s))

(s/def ::session-opts (s/keys :opt-un [::transacted? ::acknowledge-mode]))
(s/def ::transacted? boolean?)

(defn ^Session create-session
  "Produces a javax.jms.Session on a javax.jms.Connection.

  Accepts an optional, options map that can specify:
    - whether the session is transactional
    - the session's acknowledge-mode

  Defaults to creating a non-transactional session with :auto acknowledge-mode."
  ([^Connection c] (create-session c {}))
  ([^Connection c
    {transacted? :transacted?
     ack-mode    :acknowledge-mode
     :or         {transacted? false
                  ack-mode    :auto}}]
   (.createSession c transacted? (acknowledge-mode-value ack-mode))))

(s/fdef create-session
  :args (s/cat :c connection? :opts (s/? ::session-opts))
  :ret session?)

(defn session-options
  "Given a javax.jms.Session, returns a map of the options used to
  create/configure that session.

  The resulting option map is valid to pass to create-session."
  [^Session session]
  {:transacted?      (.getTransacted session)
   :acknowledge-mode (condp = (.getAcknowledgeMode session)
                       Session/AUTO_ACKNOWLEDGE :auto
                       Session/CLIENT_ACKNOWLEDGE :client
                       Session/DUPS_OK_ACKNOWLEDGE :dups-ok)})

(s/fdef session-options
  :args (s/cat :session session?)
  :ret ::session-opts)

(defn destination?
  "Predicate that tests if the value is a javax.jms.Destination"
  [dest]
  (instance? Destination dest))

(defn queue?
  "Predicate that tests if the value is a javax.jms.Queue"
  [dest]
  (instance? Queue dest))

(defn topic?
  "Predicate that tests if the value is a javax.jms.Topic"
  [dest]
  (instance? Topic dest))

(s/def ::destination destination?)

(defn ^Queue queue
  "Given a Session s, and a Queue identity string q, returns a javax.jms.Queue instance
  for the identified Queue.
  If q is already an instance of javax.jms.Queue, then it is returned unchanged.
  Any other value for q will result in an IllegalArgumentException."
  [^Session s q]
  (cond (instance? Queue q) q
        (string? q) (.createQueue s ^String q)
        :else (throw (IllegalArgumentException. "expected Queue or string"))))

(s/fdef queue
  :args (s/cat :s session?
               :q (s/alt :queue-name string?
                         :queue-inst queue?))
  :ret queue?)

(defn ^Topic topic
  "Given a Session s, and a Topic identity string t, returns a javax.jms.Topic instance
  for the identified Topic.
  If t is already an instance of javax.jms.Topic, then it is returned unchanged.
  Any other value for t will result in an IllegalArgumentException."
  [^Session s t]
  (cond (instance? Topic t) t
        (string? t) (.createTopic s ^String t)
        :else (throw (IllegalArgumentException. "expected Topic or string"))))

(s/fdef topic
  :args (s/cat :s session?
               :t (s/alt :topic-name string?
                         :topic-inst topic?))
  :ret topic?)

(defn destination-name
  "Returns the string name of a Queue or Topic."
  [d]
  (condp instance? d
    Queue (.getQueueName ^Queue d)
    Topic (.getTopicName ^Topic d)))

(s/fdef destination-name
  :args (s/cat :d destination?)
  :ret string?)

(defn message-producer?
  "Predicate that tests if p is a javax.jms.MessageProducer."
  [p]
  (instance? MessageProducer p))

(s/def ::message-producer-opts (s/keys :opt-un [::delivery-mode ::message-ttl]))
(s/def ::message-ttl pos-int?)

(defn message-producer
  "Creates a javax.jms.MessageProducer on Session s that will send
  messages to Destination d.
  May optionally pass a map of {:delivery-mode dm, :message-ttl ttl}
  to configure the default delivery-mode and time-to-live for messages
  sent by the MessageProducer."
  ([^Session s ^Destination d] (message-producer s d {}))
  ([^Session s ^Destination d {dm :delivery-mode
                               ttl :message-ttl}]
   (let [^MessageProducer p (.createProducer s d)]
     (when dm (.setDeliveryMode p (delivery-mode-value dm)))
     (when ttl
       (when-not (integer? ttl) (throw (IllegalArgumentException. "message-expiration must be an integer")))
       (.setTimeToLive p ttl))
     p)))

(s/fdef message-producer
  :args (s/cat :s session? :d destination? :opts (s/? ::message-producer-opts))
  :ret message-producer?)

(defn message?
  "Predicate that tests if m is an instance of javax.jms.Message."
  [m]
  (instance? Message m))

(defn send-message!
  "Sends a Message m via MessageProducer p.

  The single arity call is a no-op that allow send-message! to be
  used in a reducing or transducing context."
  ([^MessageProducer p] p)                                  ;; no-op completing form
  ([^MessageProducer p ^Message m] (.send p m) p)
  ([^MessageProducer p ^Destination d ^Message m] (.send p d m) p))

(s/fdef send-message!
  :args (s/alt :completing (s/cat :p message-producer?)
               :producer-dest (s/cat :p message-producer? :m message?)
               :explicit-dest (s/cat :p message-producer? :d destination? :m message?))
  :ret message-producer?)

(defn message-consumer?
  "Predicate that tests if c is a javax.jms.MessageConsumer."
  [c]
  (instance? MessageConsumer c))

(s/def ::consume-messages-opts (s/keys :opt-un [::message-selector
                                                ::no-local?
                                                ::on-message]))

(s/def ::consume-message-opts* (s/keys* :opt-un [::message-selector
                                                 ::no-local?
                                                 ::on-message]))

(s/def ::message-selector string?)
(s/def ::no-local? boolean?)
(s/def ::on-message fn?)

(defn- set-message-listener!
  [^MessageConsumer consumer on-message]
  (if (fn? on-message)
    (->> (reify MessageListener
           (onMessage [_ msg] (on-message msg)))
         (.setMessageListener consumer))
    (throw (IllegalArgumentException. "on-message must be a function"))))

(defn ^MessageConsumer message-consumer
  "Creates a javax.jms.MessageConsumer on Session s which
  consumes messages sent to Destination d.

  Accepts an optional map with the following keys:
    - :message-selector :: a JMS message selection expression string;
    - :no-local? :: if true, the consumer will not receive messages that were
                    sent by producers using the same local, connection;
    - on-message :: a function that will be called asynchronously whenever
                    the consumer receives a message. The function must accept
                    a single-argument which will be the that was message received."
  ([^Session s ^Destination d] (.createConsumer s d))
  ([^Session s ^Destination d & {:keys [message-selector
                                        no-local?
                                        on-message]}]
   (let [c (.createConsumer s d message-selector (boolean no-local?))]
     (when (some? on-message) (set-message-listener! c on-message))
     c)))

(s/fdef message-consumer
  :args (s/cat :s session?
               :d destination?
               :opts ::consume-message-opts*)
  :ret message-consumer?)

(s/def ::message-id string?)
(s/def ::message-type string?)
(s/def ::timestamp inst?)
(s/def ::expiration inst?)
(s/def ::correlation-id string?)
(s/def ::reply-to ::destination)
(s/def ::priority integer?)
(s/def ::redelivered? boolean?)
(s/def ::bytes-body bytes?)

(s/def ::message-info (s/keys :opt-un [::message-id
                                       ::message-type
                                       ::timestamp
                                       ::expiration
                                       ::correlation-id
                                       ::destination
                                       ::reply-to
                                       ::delivery-mode
                                       ::priority
                                       ::redelivered?]))

(defn message-info
  "Returns a map of the metadata common to all instances of javax.jms.Message.

  Includes ONLY non-nil entries for:
   - :message-id from Message#getJMSMessageID()
   - :message-type from Message#getJMSTypes()
   - :timestamp Instant derived from Message#getJMSTimeStamp()
   - :expiration Instant derived from Message#getJMSExpiration
   - :correlation-id from Message#getJMSCorrelationID
   - :destination from Message#getJMSDestination
   - :reply-to from Message#getJMSReplyTo()
   - :delivery-mode derived from Message#getJMSDeliveryMode()
   - :priority from Message#getJMSPriority()
   - :redelivered? from Message#getJMSRedelivered()"
  [^Message m]
  (cond-> {}
    (.getJMSMessageID m) (assoc :message-id (.getJMSMessageID m))
    (.getJMSType m) (assoc :message-type (.getJMSType m))
    (.getJMSTimestamp m) (assoc :timestamp (Instant/ofEpochMilli (.getJMSTimestamp m)))
    (.getJMSExpiration m) (assoc :expiration (Instant/ofEpochMilli (.getJMSExpiration m)))
    (.getJMSCorrelationID m) (assoc :correlation-id (.getJMSCorrelationID m))
    (.getJMSDestination m) (assoc :destination (.getJMSDestination m))
    (.getJMSReplyTo m) (assoc :reply-to (.getJMSReplyTo m))
    (.getJMSDeliveryMode m) (assoc :delivery-mode (delivery-mode-keyword (.getJMSDeliveryMode m)))
    (.getJMSPriority m) (assoc :priority (.getJMSPriority m))
    (.getJMSRedelivered m) (assoc :redelivered? (.getJMSRedelivered m))))

(s/fdef message-info
  :args (s/cat :m message?)
  :ret ::message-info)

(defmacro ^:private cond-doto
  [expr & clauses]
  (assert (even? (count clauses)))
  (let [ret (gensym)]
    `(let [~ret ~expr]
       ~@(map (fn [[pred-expr form]]
                (with-meta
                  (if (seq? form)
                    `(when ~pred-expr (~(first form) ~ret ~@(next form)))
                    `(when ~pred-expr `(~form ~ret)))
                  (meta form)))
              (partition 2 clauses))
       ~ret)))

(comment
  (macroexpand-1 '(cond-doto (StringBuilder.)
                             true (.append "1")
                             false (.append "2"))))

(defn ^Message set-message-info!
  "Sets common metadata on a Message."
  [^Message m {:keys [message-id
                      message-type
                      timestamp
                      expiration
                      correlation-id
                      destination
                      reply-to
                      delivery-mode
                      priority
                      redelivered?]}]
  (cond-doto m
             message-id (.setJMSMessageID message-id)
             message-type (.setJMSType message-type)
             timestamp (.setJMSTimestamp (inst-ms timestamp))
             expiration (.setJMSExpiration (inst-ms expiration))
             correlation-id (.setJMSCorrelationID correlation-id)
             destination (.setJMSDestination destination)
             reply-to (.setJMSReplyTo reply-to)
             delivery-mode (.setJMSDeliveryMode (delivery-mode-value delivery-mode))
             priority (.setJMSPriority priority)
             redelivered? (.setJMSRedelivered redelivered?)))

(s/fdef set-message-info!
  :args (s/cat :m message?
               :info ::message-info)
  :ret message?
  :fn #(identical? (:ret %) (-> % :args :m)))

(defn bytes-message?
  "Predicate that tests if m is a javax.jms.BytesMessage"
  [m]
  (instance? BytesMessage m))

(defn read-bytes
  "Reads bytes from a javax.jms.BytesMessage.  This only works with and mutates byte-arrays

  Arities:
   1. Pass a BytesMessage: returns a byte-array containing the contents of the message.
   2. Pass BytesMessage and a byte-array: reads contents of message into the byte-array argument
      up-to the length of the byte-array; returns the byte-array.
   3. Pass BytesMessage, byte-array, and integer length: reads upto 'length' bytes into byte-array from
      the BytesMessage; returns the byte-array."
  ([^BytesMessage m]
   (let [l (.getBodyLength m)
         arr (byte-array l)]
     (read-bytes m arr l)
     arr))
  ([^BytesMessage m ^bytes arr] (.readBytes m arr) arr)
  ([^BytesMessage m ^bytes arr length] (.readBytes m arr length) arr))

(s/fdef read-bytes
  :args (s/cat :m bytes-message?
               :out-args (s/? (s/cat :arr bytes?
                                     :length (s/? integer?))))
  :ret bytes?)

(defn write-bytes
  "Writes the contents of byte-array 'arr' into the body of BytesMessage 'm'.
  May optionally pass integer 'offset' and 'length' which will write the contents
  of 'arr[offset]..arr[length-1]' into message 'm'."
  ([^BytesMessage m ^bytes arr] (.writeBytes m arr) m)
  ([^BytesMessage m ^bytes arr offset length] (.writeBytes m arr offset length) m))

(s/fdef write-bytes
  :args (s/cat :m bytes-message?
               :arr bytes?
               :offset-length (s/? (s/cat :offset pos-int?
                                          :length pos-int?)))
  :ret bytes-message?)

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;
