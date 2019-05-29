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

(ns eva.contextual.metrics
  (:require
   [eva.contextual.logging :as logging]
   [eva.contextual.utils :as utils]
   [clojure.string :as str]
   [recide.sanex.logging :as log]
   [barometer.core]
   [quartermaster.core :as qu]
   [quartermaster.alpha :as qua]))

(defmulti metric-constructor (fn [_ [metric-type context]] metric-type))

(qu/defmanager metrics
  :discriminator (fn [_ [metric-type metric-name]] [metric-type metric-name])
  :constructor metric-constructor)

(defn ->counter [context]
  "Builds a counter out of context using `metrics` resource and metric name"
  @(qu/acquire metrics ::user [:counter context]))

(defn ->gauge [context]
  "Builds a gauge out of context using `metrics` resource and metric name"
  @(qu/acquire metrics ::user [:gauge context]))

(defn ->timer [context]
  "Builds a timer out of context using `metrics` resource and metric name"
  @(qu/acquire metrics ::user [:timer context]))

(defn inc [context]
  "Contextual incremented: counter built out of current context get incremented"
  (let [counter (->counter context)]
    (barometer.core/increment counter)))

(defn- ->explanation
  [context metric-type]
  (let [lexical (logging/lexical->str context)]
    (format  "%s for the function: %s"
             (-> metric-type name str/capitalize)
             lexical)))

(defn- ->name
  [context metric-type]
  (let [lexical   (:lexical context)
        lexical (format "%s.%s.%s"
                          (:namespace lexical)
                          (:fn-name lexical)
                          (name metric-type))
        runtime (utils/params->query-string (:runtime context))
        runtime (some->> runtime (str "?"))]
    (str lexical runtime)))

(defn- register-metric [metric-name metric]
  (barometer.core/register barometer.core/DEFAULT metric-name metric))

;; Metrics Implemenentation
(defmethod metric-constructor :counter [_ [metric-type context]]
  (let [explanation (->explanation context metric-type)
        counter     (barometer.core/counter explanation)
        metric-name (->name context metric-type)]
    (register-metric metric-name counter)))

(defmethod metric-constructor :timer [_ [metric-type context]]
  (let [explanation (->explanation context metric-type)
        timer       (barometer.core/timer explanation)
        metric-name (->name context metric-type)]
    (register-metric metric-name timer)))

(defmethod metric-constructor :gauge [_ [metric-type context]]
(let [gauge-atom (atom 0)
      explanation (->explanation context metric-type)
      gauge       (barometer.core/gauge (fn [] @gauge-atom) explanation)
      metric-name (->name context metric-type)]
  (when (register-metric metric-name gauge)
    (with-meta gauge {:set! (fn [x] (reset! gauge-atom x))}))))


(defn release-metrics []
  (qua/release-user metrics ::user))
