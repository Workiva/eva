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

(ns eva.contextual.core
  (:require
   [barometer.core]
   [clojure.string :as str]
   [ichnaie.core]
   [eva.error :refer [insist]]
   [recide.sanex.logging]
   [eva.contextual.tags :as tags]
   [eva.contextual.config :as config]
   [eva.contextual.context :as c]
   [eva.contextual.utils :as utils]
   [eva.contextual.metrics :as metrics]
   [eva.contextual.logging :as logging]
   [eva.contextual.tracing :as tracing]
   [morphe.core :as d]))

(def ^:dynamic *context* (c/->Context {} {}))

(defmacro with-context [context & body]
  `(binding [*context* ~context]
     ~@body))

;; aspects
(declare merge-runtime)

(defn mixin-runtime
  ([runtime override-config]
   (merge-runtime (:runtime *context*) runtime override-config))
  ([runtime]
   (mixin-runtime runtime ::config/as-per-config)))

(defrecord Lexical [namespace fn-name params])

(defn capture [runtime]
  (fn [fn-form]
    (d/alter-bodies fn-form
      (let [lexical (->Lexical &ns &name &params)]
        `(with-context
           (c/map->Context {:runtime (mixin-runtime ~runtime ::config/override)
                                        :lexical ~lexical})
           ~@&body)))))

(defn timed
  ([override-config runtime]
   (fn [fn-form]
     (d/alter-bodies fn-form
       (let [lexical (->Lexical &ns &name &params)]
         `(let [context# (c/map->Context {:runtime (mixin-runtime ~runtime ~override-config)
                                          :lexical ~lexical})
                timer#   (metrics/->timer context#)]
            (barometer.core/with-timer timer# ~@&body))))))
  ([runtime] (timed ::config/as-per-config runtime))
  ([] (timed [])))

(defn logged
  "Inserts a log/trace call as the first item in the fn body."
  ([override-config runtime]
   (fn [fn-form]
     (d/prefix-bodies fn-form
       (let [lexical (->Lexical &ns &name &params)]
         `(let [context#     (c/map->Context {:runtime (mixin-runtime ~runtime ~override-config)
                                              :lexical ~lexical})]
            (log context# :trace  "started..."))))))
  ([runtime] (logged ::config/as-per-config runtime))
  ([] (logged [])))


(defn traced
  ([runtime]
   (fn [fn-form]
     (d/alter-bodies fn-form
       (let [lexical (->Lexical &ns &name &params)]
         `(let [context#          (c/map->Context {:runtime (mixin-runtime ~runtime)
                                                   :lexical ~lexical})
                trace-str-prefix# (tracing/->prefix context#)
                trace-str#        (format "%s:%s" trace-str-prefix# (pr-str ~&params))]
            (ichnaie.core/tracing trace-str# ~@&body))))))
  ([] (traced [])))


;; contextual functions

(defn log
  "Contextual logging: messages prefixed with current context"
  ([context level message]
   (let [lexical-str (logging/lexical->str context {:params true})
         runtime-str (logging/runtime->str context "[" "]")
         context-str (str lexical-str runtime-str)
         message     (format "%s %s" context-str message)]
     (recide.sanex.logging/log level message)))
  ([level message]
   (log *context* level message)))

(defn inc [] (metrics/inc *context*))
(defn ->gauge [] (metrics/->gauge *context*))

(defn merge-runtime [runtime merge-with config-opt]
  (cond
    (vector? merge-with) (let [keepf    #(config/keep? %1 config-opt)
                               filtered (filter keepf merge-with)]
                           (select-keys runtime filtered))
    (map? merge-with)    (let [keepf    #(config/keep? (key %1) config-opt)
                               filtered (filter keepf merge-with)
                               filtered (into {} filtered)]
                           (merge runtime filtered))
    :else                (throw (IllegalArgumentException. "merge-runtime only allows merging with vector or map"))))


(comment
  (config/disable! :a)
  (config/enable! :a)
  (merge-runtime {} {:a "a"} ::config/as-per-config)
  (merge-runtime {:a "a" :b "b"} {:a "aa" :c "c"} ::config/as-per-config) ;; {:a "a" :b "bb" :c "c"}
  (merge-runtime {:a "a" :b "b"} [:a :c] ::config/as-per-config) ;; {:a "a"}
  )
