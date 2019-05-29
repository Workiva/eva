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

(ns eva.error
  (:require [recide.core :as rc]
            [recide.sanex :as sanex]
            [morphe.core :as d]
            [clojure.test :as test]
            [eva.config :as cfg])
  (:import (eva.error.v1 EvaException EvaErrorCode)))

(def generic-error-type->error-code
  {:api-syntax-malformed/connect-api EvaErrorCode/INCORRECT_CONNECT_SYNTAX
   :api-syntax-malformed/transact-api EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :api-syntax-malformed/pull-api EvaErrorCode/INCORRECT_PULL_SYNTAX
   :api-syntax-malformed/query-api EvaErrorCode/INCORRECT_QUERY_SYNTAX
   :api-syntax-malformed/resolve-tempid EvaErrorCode/ILLEGAL_TEMP_ID
   :api/not-yet-implemented EvaErrorCode/METHOD_NOT_YET_IMPLEMENTED

   :peer-timeout/transact EvaErrorCode/TRANSACTION_TIMEOUT
   :peer-connect/resource-manager EvaErrorCode/PEER_CONNECTION_FAILED
   :peer-connect/unrecognized-cause EvaErrorCode/PEER_CONNECTION_FAILED
   :peer-connect/message-queue EvaErrorCode/TRANSACTOR_CONNECTION_FAILURE

   :transaction-pipeline/tx-fn-threw EvaErrorCode/TRANSACTION_FUNCTION_EXCEPTION
   :transaction-pipeline/tx-fn-illegal-return EvaErrorCode/TRANSACTION_FUNCTION_INVALID_RETURN
   :transaction-pipeline/unrecognized-command EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/arity EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/nil-e EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/nil-a EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/nil-v EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/incorrect-type EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/unallocated-entity EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/dangling-nested-map-entity EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/missing-attributes EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/missing-db-id EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
   :transact-exception/install-valueType-NYI EvaErrorCode/METHOD_NOT_YET_IMPLEMENTED
   :transact-exception/fulltext-NYI EvaErrorCode/METHOD_NOT_YET_IMPLEMENTED
   :transact-exception/noHistory-NYI EvaErrorCode/METHOD_NOT_YET_IMPLEMENTED
   :transact-exception/incomplete-install-partition EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/reserved-namespace EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/no-corresponding-attribute-installation EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/incomplete-install-attribute EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/cannot-modify-schema EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/cardinality-one-violation EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/unique-value-violation EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/incompatible-attribute-properties EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/merge-conflict EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/value-too-large EvaErrorCode/MODEL_CONSTRAINT_VIOLATION
   :transact-exception/invalid-attribute-ident EvaErrorCode/INCORRECT_TRANSACT_SYNTAX

   :pull/syntax-error EvaErrorCode/INCORRECT_PULL_SYNTAX

   :eva.v2.messaging.jms.alpha.core/connection-error EvaErrorCode/UNKNOWN_MESSAGING_ERROR

   :eva.v2.system.database-connection.core/transaction-processing-failed EvaErrorCode/UNKNOWN_TRANSACTION_FAILURE

   :concurrent.queue/access EvaErrorCode/INTERNAL_COMPONENT_FAILURE
   :concurrent.queue/unsupported EvaErrorCode/INTERNAL_COMPONENT_FAILURE

   :fressian.unreadable/* EvaErrorCode/DATASTRUCTURE_DESERIALIZATION_FAILURE

   :datastructures/io-failure EvaErrorCode/UNKNOWN_STORAGE_ERROR ;; tech debt
   :datastructures/concurrent-modification EvaErrorCode/STALE_LOCAL_DATA
   :datastructures/stale EvaErrorCode/STALE_LOCAL_DATA
   :datastructures/probable-non-monotonicity EvaErrorCode/DATASTRUCTURE_CORRUPTION

   :bbtree.storage/failure EvaErrorCode/UNKNOWN_STORAGE_ERROR
   :bbtree.storage/timeout EvaErrorCode/STORAGE_TIMEOUT
   :bbtree.faulty-persist/local-only EvaErrorCode/INTERNAL_COMPONENT_FAILURE
   :bbtree.safety/not-found EvaErrorCode/DATA_NOT_FOUND
   :bbtree.safety/no-overwrite EvaErrorCode/STALE_LOCAL_DATA
   :bbtree.safety/unpersisted-changes EvaErrorCode/INTERNAL_COMPONENT_FAILURE

   :query.invalid/* EvaErrorCode/INCORRECT_QUERY_SYNTAX
   :query.builtins/function EvaErrorCode/QUERY_FUNCTION_FAILURE
   :query.builtins/aggregate EvaErrorCode/QUERY_FUNCTION_FAILURE
   :query.bindings/insufficient EvaErrorCode/UNCOMPUTABLE_QUERY
   :query.translation/invalid-form EvaErrorCode/INCORRECT_QUERY_SYNTAX
   :query.translation/unrecognized-clause EvaErrorCode/INTERNAL_COMPONENT_FAILURE
   :query.translation/unresolved-sym EvaErrorCode/QUERY_FUNCTION_NOT_FOUND
   :query.translation/unknown-predicate EvaErrorCode/UNKNOWN_QUERY_PREDICATE
   :query.evaluation/function-error EvaErrorCode/QUERY_FUNCTION_FAILURE

   :storage.error/data EvaErrorCode/UNKNOWN_STORAGE_ERROR

   :storage.sql/unknown EvaErrorCode/UNKNOWN_STORAGE_ERROR
   :storage.sql/non-extant EvaErrorCode/STORAGE_NOT_CONNECTED
   :storage.sql/cas-failure EvaErrorCode/STALE_LOCAL_DATA
   :storage.sql/unexpected-cas-update-result EvaErrorCode/UNEXPECTED_STORAGE_REPLY

   :storage.dynamo/credentials EvaErrorCode/STORAGE_AUTH_FAILURE
   :storage.dynamo/unprocessed EvaErrorCode/INCOMPLETE_STORAGE_RESPONSE ;; ?

   :value-store.concurrent/state EvaErrorCode/INTERNAL_COMPONENT_FAILURE
   :value-store.concurrent/unknown EvaErrorCode/UNKNOWN_STORAGE_ERROR
   :value-store/NPE EvaErrorCode/UNKNOWN_STORAGE_ERROR

   :storage.error/request-cardinality-exceeded EvaErrorCode/INTERNAL_COMPONENT_FAILURE

   :jms.write-error/* EvaErrorCode/MESSAGE_DISPATCH_FAILURE
   :jms.read-error/* EvaErrorCode/MESSAGE_RECEPTION_FAILURE

   :entid-coercion/* EvaErrorCode/COERCION_FAILURE
   :ident-coercion/* EvaErrorCode/COERCION_FAILURE

   :attribute-resolution/* EvaErrorCode/COERCION_FAILURE})

(def kw->memfns
  {::error-code (memfn ^EvaException getErrorCode)})

(defn eva-exception
    ([message data] (eva-exception message data nil))
    ([message data cause]
     (let [error-code (get data
                           ::error-code
                           ;; otherwise determine by:
                           (get generic-error-type->error-code
                                (:eva/error data)
                                ;; then check for generic match:
                                (get generic-error-type->error-code
                                     (keyword (namespace (:eva/error data)) "*")
                                     ;; but default in the end to:
                                     EvaErrorCode/UNKNOWN_ERROR)))
           data (dissoc data ::error-code)]
       (EvaException. error-code message data cause))))

(def eva-error-form
  (rc/error-form {:serialization-tag "eva/error"
                  :type-keyword :eva/error
                  :cause-keyword ::cause
                  :message-keyword ::msg
                  :data-keyword ::data
                  :metadata-fns kw->memfns
                  :constructor eva-exception
                  :serialized-keyword :eva.error.serialized-throwable/v2}))

(def ^:dynamic *capture-flag* (cfg/config-strict :eva.error.capture-insists))

(rc/generate-library! eva-error-form *capture-flag*)

(defn ^Throwable root-cause
  "Returns the initial cause of an exception or error by peeling off all of
  its wrappers"
  {:added "1.3"}
  [^Throwable t]
  (loop [cause t]
    (if (and (instance? clojure.lang.Compiler$CompilerException cause)
             (not= (.source ^clojure.lang.Compiler$CompilerException cause) "NO_SOURCE_FILE"))
      cause
      (if-let [cause (.getCause cause)]
        (recur cause)
        cause))))

(def ^:dynamic *always-sanitize-exceptions*
  (cfg/config-strict :eva.error.always-sanitize-exceptions))

(defn handle-api-error
  [^Exception e]
  (if (and (instance? EvaException e) *always-sanitize-exceptions*)
    (.getSanitized ^EvaException e sanex/*sanitization-level*)
    e))

(defmacro with-api-error-handling
  "catches all non-EvaException throwables and wraps them with :eva/unknown-error."
  [& body]
  `(rc/try*
    ~@body
    (catch EvaException e#
      (throw (handle-api-error e#)))
    (catch (:not EvaException) e#
      (let [wrapper# (error :eva/unknown-error
                            "An unknown exception was raised."
                            {}
                            e#)]
        (throw (handle-api-error wrapper#))))))

(defn eva-ex
  "an aspect for eva.error/with-api-error-handling"
  [fn-form]
  (d/alter-bodies
   fn-form
   `(with-api-error-handling ~@&body)))

(defn override-codes
  "Potentially retag error codes based on the type->code map passed in"
  [overrides ^Throwable t]
  (if-not (instance? EvaException t)
    t
    (let [error-type (.getErrorType ^EvaException t)]
      (when-let [error-code (get overrides error-type)]
        (.setErrorCode ^EvaException t error-code))
      t)))

(defmacro overriding-codes
  [overrides & body]
  `(let [overrides# ~overrides]
     (try ~@body
          (catch EvaException e#
            (throw (override-codes overrides# e#))))))

(defmacro is-thrown?
  "Utility macro that requires a literal map as its `test` argument.

   Executes body, and asserts that either (a) its result is an EvaException or (b) it throws one.
   Optional additional tests:
    * :msg-re is a regex code to match a substring of the exception message
    * :error-codes is a collection of error codes to assert hasErrorCode on
    * :http-code is the integer HTTP error mapped onto this exception
    * :eva-code is the integer Eva error code
    * :error-type matches the keyword type under :eva/error

  If the body throws a different type of exception or something other than an EvaException, the
  optional key :unwrapper may be used to pull the EvaException off of this other value before
  any test assertions are made."
  [{:keys [msg-re error-codes http-code eva-code error-type unwrapper] :as tests}
   & body]
  (insist (map? tests) "You must pass a literal map into is-thrown?")
  (let [e (with-meta (gensym 'e) {:tag `EvaException})
        msg-re-test (when msg-re `(test/is (re-find ~msg-re (.getMessage ~e))))
        error-type-test (when error-type
                          `(test/is (.hasErrorType ~e ~error-type)
                                    (format "hasErrorType - expected: %s, found: %s\n%s\n"
                                            ~error-type
                                            (.getErrorType ~e)
                                            ~e)))
        error-codes-test (when error-codes
                           `(doseq [code# ~error-codes]
                              (test/is (.hasErrorCode ~e code#)
                                       (format "hasErrorCode - expected: %s, found: %s\n%s\n"
                                               code#
                                               (.getErrorCode ~e)
                                               ~e))))
        http-code-test (when http-code
                         `(test/is (-> ~e .getErrorCode .getHttpErrorCode (= ~http-code))
                                   (format "getHttpErrorCode - expected: %s, found: %s\n%s\n"
                                           ~http-code
                                           (-> ~e .getErrorCode .getHttpErrorCode)
                                           ~e)))
        eva-code-test (when eva-code
                        `(test/is (-> ~e .getErrorCode .getCode (= ~eva-code))
                                  (format "getCode - expected: %s, found: %s\n%s\n"
                                          ~eva-code
                                          (-> ~e .getErrorCode .getCode)
                                          ~e)))]
    `(let [result# (try ~@body
                        (catch Throwable e# e#))
           ~e (~(or unwrapper identity) result#)]
       (test/is (instance? EvaException ~e))
       ~@(filter some?
                 [msg-re-test
                  error-type-test
                  error-codes-test
                  http-code-test
                  eva-code-test]))))
