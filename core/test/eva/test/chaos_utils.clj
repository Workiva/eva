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

(ns eva.test.chaos-utils
  (:require [eva.v2.storage.value-store]
            [clojure.test.check.generators :as gen]
            [clojure.spec.alpha :as s]))

(defn- frequency-mock-spec->gen
  "Converts mock spec such as `[[frequency-1 fn-1] [frequency-2 fn-2]]`
  into test check generator which would sample fn-1 ... fn-n according to
  their frequencies."
  [frequency-v]
  (gen/frequency (map (fn [[frequency f]]
                        [frequency (gen/return f)])
                      frequency-v)))

(defn- gen->chaos-fn
  "Converts generator into 'chaos' function, i.e.
  function which on every call would sample generator and
  apply its arguments to randomly selected function."
  [gen]
  (fn [& args]
    (let [sampled (-> gen (gen/sample 1) first)]
      (apply sampled args))))

(defn mock-spec->mocker
  "Generates what `with-redefs` macro defines as `temp-value-expr`
  from mock-spec. The latter is either a symbol and then it is
  being used as-is or vector `[[frequency-1 fn-1] [frequency-2 fn-2]]`
  where `frequency-n` is integer and `fn-n` is function name symbol."
  [mock-spec]
  (cond
    (vector? mock-spec) (-> mock-spec
                            frequency-mock-spec->gen
                            gen->chaos-fn)
    :else               mock-spec))


(defmacro with-induced-chaos [m & body]
  "Accept map of keyword->mock-spec where keyword is one of `allowed-keywords`
   and spec is either funciton name (symbol) or frequency vector:
   `[[frequency-1 fn-1] [frequency-2 fn-2]]`."
  (let [redefs-binding (->> m
                            (map (fn [[symbol spec]]
                                   [symbol `(mock-spec->mocker ~spec)]))
                            (apply concat)
                            vec)]
    `(with-redefs ~redefs-binding
       ~@body)))

;; examples:
(comment
  ;; mocks eva.v2.storage.value-store/put-value
  ;; with chaos function which in 70% applies inc to its arg
  ;; and in 30% calls applies dec to its arg
  (with-induced-chaos {eva.v2.storage.value-store/put-value [[7 inc] [3 dec]]}
    (eva.v2.storage.value-store/put-value 1))

  ;; mocks eva.v2.storage.value-store/put-value wit inc
  (with-induced-chaos {eva.v2.storage.value-store/put-value inc}
    (eva.v2.storage.value-store/put-value 1))
  )
