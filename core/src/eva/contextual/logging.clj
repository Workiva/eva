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

(ns eva.contextual.logging
  (:require [eva.contextual.utils :as utils]))

(defn- sorted
  [tags]
  (into (sorted-map) tags))

(defn runtime->str
  ([context left-bracket right-bracket]
   (let [runtime (:runtime context)]
     (if (not-empty runtime)
       (str
        left-bracket
        (utils/params->query-string (sorted runtime)) ;; for now
        right-bracket))))
  ([context]
   (runtime->str context "" "")))

(defn lexical->str
  ([context]
   (lexical->str context {}))
  ([context options]
   (let [fn-form (:lexical context)
         str (format "%s/%s"(:namespace fn-form) (:fn-name fn-form))]
     (if (:params options)
       (format "%s%s" str (apply list (:params fn-form)))
       str))))

(comment
  (runtime->str {:runtime {:b "b" :a "a"}})
  (runtime->str {:runtime {:b "b" :a "a"}} "[" "]")
  (runtime->str {} "[" "]"))
