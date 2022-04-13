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

(ns eva.contextual.utils
  (:require [clojure.string :refer [join replace]])
  (:import [java.util.regex Pattern])
  (:refer-clojure :exclude [replace]))

;; helpers to sanitize names a bit, so code parsing them could use standard URI parsing tools
(def ^:private replacements
  {"?" "_QMARK_"
   "!" "_BANG_"
   "&" "_AND_"
   "=" "_EQ_"})

(def ^:private replacement-pattern
  (->> (keys replacements)
       (map #(Pattern/quote %))
       (interpose "|")
       (apply str)
       (re-pattern)))

(defn- clean-name
  [name]
  (replace name replacement-pattern replacements))

;; Until tags could be submitted to metrics library as hash, they are being
;; encoded into metric name. Some third-party tool could be used to parse it
;; and process properly.
(defn params->query-string [m]
  (if (not-empty m)
    (join "&" (for [[k v] m] (str (clean-name (name k)) "=" (clean-name (str v)))))
    nil))

(comment
  (params->query-string {::a "a" ::b "b"})
  (params->query-string {}))
