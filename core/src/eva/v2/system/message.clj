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

(ns eva.v2.system.message
  (:require [eva.v2.system.indexing.core :as indexing]
            [eva.v2.database.log :as log]
            [eva.v2.system.transactor.core :as tx]
            [eva.v2.database.core :as database]
            [eva.v2.storage.value-store.core :as values]
            [clojure.spec.alpha :as s]))

(s/def ::scope
  (s/keys :req [::values/partition-id
                ::database/id]))

(defn system-scoped-spec [s]
  (s/merge s ::scope))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SYSTEM-SUPPORTED MESSAGES ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO: This may not be the correct place for these.

(s/def ::transaction-submission
  (system-scoped-spec (s/keys :req-un [::tx/transaction-data])))

(s/def ::transaction-accepted
  (system-scoped-spec (s/keys :req-un [::log/entry]))) ;; TODO: This currently has undocumented issues

;; the transactor has written a new log entry vs ...
(s/def ::log-advanced
  (system-scoped-spec (s/keys :req-un [::log/head]))) ;; TODO: This currently has undocumented issues

(s/def ::indexes-updated
  (system-scoped-spec (s/keys :req-un [::indexing/index-updates]))) ;; TODO: This currently has undocumented issues
