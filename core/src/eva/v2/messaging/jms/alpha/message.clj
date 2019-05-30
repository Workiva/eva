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

(ns eva.v2.messaging.jms.alpha.message
  (:refer-clojure :exclude [read])
  (:require [clojure.spec.alpha :as s]
            [clojure.data.fressian :as fressian]
            [eva.v2.fressian :as ef]
            [recide.utils :as ru]
            [eva.error :refer [deferror-group]])
  (:import (javax.jms BytesMessage Message)))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

;; ## Utils
;;

;; ### Message Reading
;;
(defn ^:private read-bytes [^BytesMessage msg]
  (let [l (.getBodyLength msg)
        arr (byte-array l)]
    (.readBytes msg arr)
    arr))

(defn ^:private read-fressian [^bytes ba]
  (fressian/read ba :handlers ef/eva-messaging-read-handlers))

(defn ^:private read-data [^BytesMessage m]
  (.reset m)
  (let [data (read-fressian (read-bytes m))]
    (.reset m)
    data))

;; ### Message Writing
;;
(defn ^:private write-bytes [^BytesMessage msg ^bytes ba]
  (doto msg
    (.writeBytes ba)))

(defn ^:private write-fressian [obj]
  (.array (fressian/write obj :handlers ef/eva-messaging-write-handlers)))

(defn ^:private write-data [^BytesMessage m data]
  (write-bytes m (write-fressian data)))

(defn ^:private write [msg data]
  (if-not (instance? BytesMessage msg)
    (throw (IllegalArgumentException. (str "not a javax.jms.BytesMessage; got type " (class msg) "instead")))
    (write-data msg data)))

;; ## Message Decoding and Interpretation
;;

;; ### Decoding
;;

;; #### Information returned from a decoding call

;; Indicates if decoding was successful
(s/def ::decode-success? boolean)

;; Contains the key that results from different
;; decoding branches.
(s/def ::decode-result #{::decode-error
                         ::decoded-body
                         ::unsupported-message
                         ::not-a-message})

;; ##### Decoding Branches
;;

;; Asked to decode something that is not a message.
(s/def ::not-a-message #(not (instance? Message %)))

;; Received a message that is no a BytesMessages.
(s/def ::unsupported-message (s/and #(instance? Message %)
                                    #(not (instance? BytesMessage %))))

;; Exception thrown during decoding.
(s/def ::decode-error #(instance? Exception %))

;; The body was successfully decoded.
(s/def ::decoded-body some?)

;; #### Decode function
(defn ^:private decode
  "Attempts to decode the argument as a javax.jms.BytesMessage.

  Returns a map indicating the status and result of attempting
  to decode the argument. The return map has the following required fields:
  - ::decode-success?    :: true if msg was decoded; false if decoding failed for any reason.
  - ::decode-result      :: qualified keyword indicating both:
                            1. the reason decoding succeeded or failed
                            2. the field under which additional information can be found."
  [msg]
  (cond
    (instance? BytesMessage msg)
    (let [[body err] (try [(read-data msg) nil]
                          (catch Exception e [nil e]))]
      (if err
        {::decode-success? false
         ::decode-result   ::decode-error
         ::decode-error    err}
        {::decode-success? true
         ::decode-result   ::decoded-body
         ::decoded-body    body}))

    (instance? Message msg)
    {::decode-success?     false
     ::decode-result       ::unsupported-message
     ::unsupported-message msg}

    :else
    {::decode-success?  false
     ::decode-result    ::not-a-message
     ::not-a-message    msg}))

;; The decode function will accept any argument, returning
;; the value indicating the result of the decoding attempt.
(s/fdef decode
  :args (s/cat :msg any?)
  :ret (s/keys :req [::decode-success?
                     ::decode-result]
               :opt [::decode-error
                     ::decoded-body
                     ::unsupported-message
                     ::not-a-message]))

(comment
  (decode :foo)
  (decode (reify Message))
  (decode (reify BytesMessage
            (getJMSMessageID [_] "fake-message")
            (getBodyLength [_] 0)
            (readBytes [_ arr l] 0))))

;; ### Interpretation of the Decoded Body
;;

;; #### Interpretation Information Model
;; Was the body understandable?
(s/def ::body-understood? boolean?)

;; What was the body interpreted as?
(s/def ::body-interpretation #{::unrecognized-body
                               ::body-missing-throwable-flag
                               ::body-missing-payload-field
                               ::deserialize-throwable-io-error
                               ::deserialize-throwable-class-not-found
                               ::throwable
                               ::data})

;; - The body wasn't recognized.
(s/def ::unrecognized-body (s/and #(not (instance? Message %))
                                  #(not (map? %))))

;; - The body was recognized as a map,

;;   - but was missing the `:throwable?` field
(s/def ::body-missing-throwable-flag #(not (contains? % :throwable?)))

;;   - but was missing the `:payload` field
(s/def ::body-missing-payload-field #(not (contains? % :payload)))

;;   - and contained a serialized throwable payload
(s/def ::throwable #(instance? Throwable %))

;;   - and stated it contained a throwable payload, but it couldn't be deserialized
(s/def ::deserialize-throwable-io-error #(instance? java.io.IOException %))

;;   - and stated it contained a throwable payload, but deserialized into an unknown class
(s/def ::deserialize-throwable-class-not-found #(instance? ClassNotFoundException %))

;;   - and contained a data payload
(s/def ::data #(not (instance? Throwable %)))

;; #### Classification and Interpretation functions
;;

;; Returns one of the keywords in the `::body-interpretation` spec
;; to indicate what the body was recognized as.
(defn ^:private classify-body
  "Given the body data that results from successful decode:

      Ex: (some-> (decode msg) ::decoded-body (classify-body))

  Returns the a keyword indicating if the body data is recognized,
  and if NOT, the reason why.

  Possible return values:

    - ::throwable                   :: body data was recognized as containing a serialized throwable
    - ::data                        :: body data was recognized as containing a data payload
    - ::unrecognized-body           :: body data was not recognized (ie. not a map)
    - ::body-missing-throwable-flag :: body was a map, but did not contain the required key :throwable?
    - ::body-missing-payload-field  :: body was a map, but did not contain the required key :payload"
  [body]
  (cond
    (not (map? body)) ::unrecognized-body
    (not (contains? body :throwable?)) ::body-missing-throwable-flag
    (not (contains? body :payload)) ::body-missing-payload-field
    (:throwable? body) ::throwable
    :else ::data))

(s/fdef classify-body
  :args (s/cat :body (s/or :map map?
                           :other any?))
  :ret ::body-interpretation)

;; Returns the full interpretation of a decoded body.
(defn ^:private interpret-body
  "Given the body data that results from successful decode:

      Ex: (some-> (decode msg) ::decoded-body (interpret-body))

   Returns a structured map indicating:
     - ::body-understood?     :: true if the body was interpreted and understood; false if not.
     - ::body-interpretation  :: a keyword indicating the semantic interpretation of the body;
                                 this keyword is also an additional key that will be present
                                 in the map, and whose value is the result of interpreting the body."
  [body]
  (let [body-type (classify-body body)]
    (case body-type
      ::throwable (try {::body-understood?    true
                        ::body-interpretation body-type
                        body-type             (ru/deserialize-throwable (:payload body))}
                       (catch java.io.IOException e
                         {::body-understood? false
                          ::body-interpretation ::deserialize-throwable-io-error
                          ::deserialize-throwable-io-error e})
                       (catch ClassNotFoundException e
                         {::body-understood? false
                          ::body-interpretation ::deserialize-throwable-class-not-found
                          ::deserialize-throwable-class-not-found e}))
      ::data {::body-understood?    true
              ::body-interpretation body-type
              body-type             (:payload body)}
      {::body-understood?    false
       ::body-interpretation body-type
       body-type             body})))

(s/fdef interpret-body
  :args (s/cat :body (s/or :map map?
                           :other any?))
  :ret (s/keys :req [::body-understood?
                     ::body-interpretation]
               :opt [::unrecognized-body
                     ::body-missing-throwable-flag
                     ::body-missing-payload-field
                     ::deserialize-throwable-io-error
                     ::deserialize-throwable-class-not-found
                     ::throwable
                     ::data]))

;; Applies `interpret-body` to the decoding information.
(defn ^:private interpret-decoding
  "Conditionally applies interpret-body to the result of (decode msg)
  ONLY if the decode result was successful."
  [{:as m :keys [::decode-success? ::decode-result]}]
  (cond-> m
    decode-success? (merge (interpret-body (get m decode-result)))))

(s/fdef interpret-decoding
  :args (s/cat :m (s/and (s/keys :req [::decode-success?
                                       ::decode-result]
                                 :opt [::decode-error
                                       ::decoded-body
                                       ::unsupported-message
                                       ::not-a-message])
                         #(contains? % (get % ::decode-result))))
  :ret (s/keys :req [::body-understood?
                     ::body-interpretation]
               :opt [::unrecognized-body
                     ::body-missing-throwable-flag
                     ::body-missing-payload-field
                     ::throwable
                     ::data]))

(comment
  ;; Decoding failure falls through without further interpretation
  (interpret-decoding {::decode-success? false})
  (interpret-decoding (decode :foo))
  (interpret-decoding (decode (reify Message)))
  (interpret-decoding (decode (reify BytesMessage
                                (getJMSMessageID [_] "fake-message")
                                (getBodyLength [_] 0)
                                (readBytes [_ arr l] 0))))

  ;; Interpret on successful decoding

  (interpret-decoding {::decode-success? true, ::decode-result ::decoded-body, ::decoded-body {:throwable? false, :payload [1 2 3]}})
  (interpret-decoding {::decode-success? true
                       ::decode-result   ::decoded-body
                       ::decoded-body    {:throwable? true
                                          :payload               (rc/serialize-throwable (IllegalArgumentException.))}}))

;; Attempts to decode and interpret a message.
(defn ^:private interpret-message
  "Attempts to decode and interpret a message.

  For further details, see the documentation for:
    - decode
    - classify-body
    - interpret-body
    - interpret-decoding"
  [msg]
  (-> msg (decode) (interpret-decoding)))

(s/fdef interpret-message
  :args (s/cat :msg any?)
  :ret (s/and (s/keys :req [::body-understood?
                            ::body-interpretation]
                      :opt [::unrecognized-body
                            ::body-missing-throwable-flag
                            ::body-missing-payload-field
                            ::throwable
                            ::data])
              #(contains? % (get % ::body-interpretation))))

;; ## Message Encoding and Transcription

(s/def ::transcribe-success? boolean?)
(s/def ::transcribe-result #{::transcribe-error
                             ::throwable-body
                             ::data-body
                             ::cannot-transcribe-nil})
(s/def ::transcribe-error (s/or :exception #(instance? Exception %)
                                :assertion-error #(instance? AssertionError %)))
(s/def ::cannot-transcribe-nil nil?)
(s/def ::data-or-throwable-body (s/keys :req-un [::throwable? ::payload]))
(s/def ::throwable? boolean?)
(s/def ::payload (s/or :bytes bytes?
                       :data #(not (instance? Throwable %))))

(s/def ::throwable-body (s/and ::data-or-throwable-body
                               #(-> % :throwable? true?)
                               #(-> % :payload (first) (= :bytes))))
(s/def ::data-body (s/and ::data-or-throwable-body
                          #(-> % :throwable? false?)
                          #(-> % :payload (first) (= :data))))

(comment
  (s/valid? ::throwable-body {:throwable? true
                              :payload (rc/serialize-throwable (ex-info "test" {}))})
  (s/valid? ::data-body {:throwable? false
                         :payload [1 2 3]}))

(defn ^:private transcribe
  "Attempts to transcribe x as data into a structure that will
  later be Fressian encoded into the body of a BytesMessage.

  If x is a Throwable, the structure will be:
    {:throwable? true, :payload <java-serialized-x-byte-array>}

  If x is NOT a Throwable, the structure will be:
    {:throwable? false, :payload x}

  The returned value is a map that indicates:
    - ::transcribe-success?    :: true if transcribing succeeded, otherwise false.
    - ::transcribe-result      :: a qualified-keyword that indicates the reason for success or failure.
                                  This keyword will also be present in the map, and its value will contain
                                  the successful result or details about the failure.

  On success ::transcribe-result may be:
     - ::data-body
     - ::throwable-body

  On failure, ::transcribe-result may be:
     - ::transcribe-error
     - ::cannot-transcript-nil

  To retrieve the transcript result use the pattern:
    (let [m (transcribe x)]
      (get m (::transcribe-result m)))"
  [x]
  (cond
    (instance? Throwable x)
    (let [[st err] (try [(ru/serialize-throwable x) nil]
                        (catch Exception e [nil e])
                        (catch AssertionError e [nil e]))]
      (if err
        {::transcribe-success? false
         ::transcribe-result ::transcribe-error
         ::transcribe-error err}
        {::transcribe-success? true
         ::transcribe-result ::throwable-body
         ::throwable-body {:throwable? true
                           :payload st}}))
    (some? x)
    {::transcribe-success? true
     ::transcribe-result ::data-body
     ::data-body {:throwable? false
                  :payload x}}

    :else
    {::transcribe-success? false
     ::transcribe-result ::cannot-transcribe-nil
     ::cannot-transcribe-nil x}))

(s/fdef transcribe
  :args (s/cat :x any?)
  :ret (s/keys :req [::transcribe-success?
                     ::transcribe-result]
               :opt [::transcribe-error
                     ::throwable-body
                     ::data-body
                     ::cannot-transcribe-nil]))

(s/def ::encode-body-success? boolean?)
(s/def ::encode-body-result #{::nothing-to-encode
                              ::encode-body-error
                              ::encoded-body})
(s/def ::nothing-to-encode nil?)
(s/def ::encode-body-error #(instance? Exception %))
(s/def ::encoded-body bytes?)

(defn ^:private encode-body
  "Passed the result map from a call to transcribe:

     (encode-body (transcribe x))

  encode-body xxtends the result map with:
    - ::encode-body-success?
    - ::encode-body-result

  Like other functions in this namespace, ::encode-body-result contains a qualified-keyword
  that is itself present in the result map and indexes the result of encoding (whether successful or not).

  To access the encoding result use the pattern:

    (let [m (encode-body transcribe-res)]
      (get m (::encode-body-result m)))"
  [{:as m :keys [::transcribe-success? ::transcribe-result]}]
  (cond
    (not transcribe-success?) (assoc m ::encode-body-success? false
                                     ::encode-body-result ::nothing-to-encode
                                     ::nothing-to-encode nil)
    (nil? (get m transcribe-result)) (merge m {::encode-body-success? false
                                               ::encode-body-result ::nothing-to-encode
                                               ::nothing-to-encode nil})
    (keyword? (get m transcribe-result)) (merge m {::encode-body-success? false
                                                   ::encode-body-result ::nothing-to-encode
                                                   ::nothing-to-encode nil})

    :else
    (let [payload (get m transcribe-result)
          [body err] (try [(write-fressian payload) nil]
                          (catch Exception e [nil e]))]
      (merge m
             (if err
               {::encode-body-success? false
                ::encode-body-result   ::encode-body-error
                ::encode-body-error    err}
               {::encode-body-success? true
                ::encode-body-result ::encoded-body
                ::encoded-body body})))))

(s/fdef encode-body
  :args (s/cat :m (s/and (s/keys :req [::transcribe-success? ::transcribe-result])
                         #(contains? % (get % ::transcribe-result))))
  :ret (s/and (s/keys :req [::encode-body-success?
                            ::encode-body-result]
                      :opt [::nothing-to-encode
                            ::encode-body-error
                            ::encoded-body])
              #(contains? % (get % ::encode-body-result))))

(s/def ::write-body-result #{::nothing-to-write
                             ::invalid-encoded-body
                             ::encoded-message
                             ::write-body-error})

(s/def ::nothing-to-write nil?)
(s/def ::invalid-encoded-body #(not (bytes? %)))
(s/def ::encoded-message #(instance? BytesMessage %))
(s/def ::write-body-error #(instance? Exception %))

(defn ^:private write-body
  "Consumes the result-map of (encode-body (transcribe x)) and, if successful, writes the
  encoded body into the msg.

  Like other functions in this namespace, the map m is extended with the results of this operation:
    - ::write-body-success?
    - ::write-body-result

  Following the pattern established in this namespace, the final result can be accessed using:

    (let [m (write-body encode-body-res)]
      (get m (::write-body-result m)))"
  [{:as m :keys [::encode-body-success? ::encode-body-result]} msg]
  (merge
   (assoc m ::write-body-on-message msg)
   (cond (not encode-body-success?) {::write-body-success? false
                                     ::write-body-result   ::nothing-to-write
                                     ::nothing-to-write nil}
         (nil? (get m encode-body-result)) {::write-body-success? false
                                            ::write-body-result   ::nothing-to-write
                                            ::nothing-to-write nil}
         (not (bytes? (get m encode-body-result))) {::write-body-success?  false
                                                    ::write-body-result    ::invalid-encoded-body
                                                    ::invalid-encoded-body (get m encode-body-result)}
         (not (instance? Message msg)) {::write-body-success?     false
                                        ::write-body-result       ::not-a-message
                                        ::not-a-message           msg
                                        ::supported-message-types #{(.getName BytesMessage)}}
         (not (instance? BytesMessage msg)) {::write-body-success?      false
                                             ::write-body-result        ::unsupported-message
                                             ::unsupported-message      msg
                                             ::unsupported-message-type (type msg)
                                             ::supported-message-types  #{(.getName BytesMessage)}}

         :else
         (try {::write-body-success? true
               ::write-body-result   ::encoded-message
               ::encoded-message     (write-bytes msg (get m encode-body-result))}
              (catch Exception e
                {::write-body-success? false
                 ::write-body-result ::write-body-error
                 ::write-body-error e})))))

(s/fdef write-body
  :args (s/cat :m (s/and (s/keys :req [::encode-body-success? ::encode-body-result]
                                 :opt [::nothing-to-encode
                                       ::encode-body-error
                                       ::encoded-body])
                         #(contains? % (get % ::encode-body-result)))
               :msg any?)
  :ret (s/keys :req [::write-body-success?
                     ::write-body-result]
               :opt [::encoded-message
                     ::write-body-error
                     ::nothing-to-write
                     ::invalid-encoded-body
                     ::not-a-message
                     ::unsupported-message
                     ::supported-message-types]))

(defn ^:private encode!
  "Combines transcribe, encode-body, and write-body to
  encode payload and write it into msg.

  See those functions for details on their behavior."
  [msg payload]
  (-> payload
      (transcribe)
      (encode-body)
      (write-body msg)))

;; ## Public API
;;

(deferror-group write-error
  (:jms.write-error [::payload])
  (transcribe-error "Error transcribing payload")
  (encode-body-error "Error encoding message body")
  (write-body-error "Error writing message body"))

(defn write-content!
  "Attempts to write content into the body of msg in encoded form.
  Throws a ::write-error on failure."
  [msg content]
  (let [{:keys [::transcribe-error
                ::encode-body-error
                ::write-body-error
                ::write-body-success?]} (encode! msg content)]
    (cond transcribe-error (raise-write-error :transcribe-error "" {::payload content} transcribe-error)
          encode-body-error (raise-write-error :encode-body-error "" {::payload content} transcribe-error)
          write-body-error (raise-write-error :write-body-error "" {::payload content} write-body-error)
          (not write-body-success?) (throw (IllegalStateException. "no errors occurred, but write-body-success? == false"))
          ;; TODO: finish out the other failure conditions
          :else msg)))

(deferror-group read-error
  (:jms.read-error [::message])
  (decode-error "Error decoding message body")
  (unsupported-message "Expected javax.jms.BytesMessage but received")
  (not-a-message "Expected a message but received")
  (unrecognized-body "Unrecognized message body" [::decoded-body])
  (body-missing-throwable-flag "Message body missing :throwable?" [::decoded-body])
  (body-missing-payload-field "Message body missing :payload" [::decoded-body]))

(defn decode-body-and-reset [^BytesMessage msg]
  (.reset msg)
  (let [body-bytes (read-bytes msg)
        body-data (read-fressian body-bytes)]
    (.reset msg)
    {:body-bytes  (vec body-bytes)
     :body-data   body-data
     :body-length (.getBodyLength msg)}))
(defn read-content
  "Attempts to read, decode, and interpret content from the body of msg.
  Throws a ::read-error on failure."
  [msg]
  (let [{:keys [::decode-error
                ::unsupported-message
                ::not-a-message
                ::unrecognized-body
                ::body-missing-throwable-flag
                ::body-missing-payload-field
                ::throwable
                ::data]} (-> msg (decode) (interpret-decoding))]
    (cond
      decode-error (raise-read-error :decode-error "" {::message msg} decode-error)
      unsupported-message (raise-read-error :unsupported-message (str (type msg)) {::message msg})
      not-a-message (raise-read-error :not-a-message (str (type msg)) {::message msg})
      body-missing-throwable-flag (raise-read-error :body-missing-throwable-flag "" {::message msg, ::decoded-body body-missing-throwable-flag})
      body-missing-payload-field (raise-read-error :body-missing-payload-field "" {::message msg, ::decoded-body body-missing-payload-field})
      unrecognized-body (raise-read-error :unrecognized-body "" {::message msg ::decoded-body unrecognized-body})
      throwable throwable
      data data
      :else (throw (IllegalStateException. "should not reach this state")))))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;
