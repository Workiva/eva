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

(ns eva.contextual.config
  (:require [clojure.spec.alpha :as s])
  (:refer-clojure :exclude [reset!]))

(s/def ::config-options #{::as-per-config ::override})

(def ^:private config (atom {}))

(defn keep?
  [tag option]
  (cond
    (= ::override option)      true ;; do not ignore tag
    (= ::as-per-config option) (get @config tag true) ;; not ignored if not configured
    :else (throw (IllegalArgumentException. (format "Invalid config option: %s" option)))))

(s/fdef keep? :args (s/cat :tag keyword? :option ::config-options))

(defn set-tag! [tag yes-no]
  (swap! config assoc tag yes-no))

(defn enable! [tag]
  (set-tag! tag true))

(defn disable! [tag]
  (set-tag! tag false))

(defn reset! []
  (clojure.core/reset! config {}))
