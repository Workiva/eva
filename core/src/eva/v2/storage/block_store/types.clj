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

(ns eva.v2.storage.block-store.types
  (:require [clojure.spec.alpha :as s]
            [eva.v2.utils.spec :refer [conform-spec]]))

(defmulti config-by-type ::storage-type)
(s/def ::config (s/multi-spec config-by-type ::storage-type))

(defn config-type [config]
  {:pre [(conform-spec ::config config)]}
  (::storage-type config))

(defmulti build-block-store
  "Constructs an object satisfying the blockstore contract given a configuration
   which must be multispec'd via ::config. The returned block store must also
   implement the component protocol for starting and stopping to control resource
   allocation."
  config-type)

(defmulti block-store-ident
  "Returns a 'ident' for the provided block store configuration, which will be
   used to determine uniqueness of the block store object. Since scopes for
   allocating connections / resources for the block stores are specific to each
   block store, this must be provided by the individual configurations."
  config-type)
