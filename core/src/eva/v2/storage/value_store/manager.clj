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

(ns eva.v2.storage.value-store.manager
  (:require [quartermaster.core :as qu]
            [eva.v2.storage.value-store.concurrent :as concurrent-value-store]
            [eva.v2.storage.value-store.gcached :as caching-value-store]))

;; TODO: for consistency, this file should probably be in storage/value-store/core.clj,
;;       but the presence of specs in that file would force a cyclic dependency.
;;       Once we have an external contract that allows us to perturb the config
;;       parameters more easily, this manager should probably be relocated to core.

;; TODO: right now, we're calling down to the definitions for these functions
;;       from their resource managers, but they should probably be
;;       consolidated: I don't think we *really* have a case for lots of
;;       polymorphism wrt value stores. The different managers should
;;       probably just be one.

(qu/defmanager value-store-manager
  :discriminator
  (fn [user config]
    (if (true? (::disable-caching? config))
      [:no-caching (concurrent-value-store/discriminator user config)]
      [:caching    (caching-value-store/discriminator user config)]))
  :constructor
  (fn [[caching-opt sub-ident] config]
    (case caching-opt
      :no-caching (concurrent-value-store/constructor sub-ident config)
      :caching (caching-value-store/constructor sub-ident config))))
