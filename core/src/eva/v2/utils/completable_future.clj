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

(ns eva.v2.utils.completable-future
  (:refer-clojure :exclude [promise deliver realized?])
  (:import (java.util.concurrent CompletableFuture)))

(defn promise [] (CompletableFuture.))
(defn promise? [x] (instance? CompletableFuture x))

(defn complete [^CompletableFuture p v] (.complete p v))
(defn reject [^CompletableFuture p ex] (.completeExceptionally p ex))
(defn deliver [p v] (if (instance? Throwable v)
                      (reject p v)
                      (complete p v)))

(defn done? [^CompletableFuture p] (or (.isDone p)))
(defn pending? [p] (not (done? p)))
(defn rejected? [^CompletableFuture p] (.isCompletedExceptionally p))
(defn cancelled? [^CompletableFuture p] (.isCancelled p))

(defn failed? [p] (or (rejected? p) (cancelled? p)))
(defn resolved? [p] (and (not (failed? p))
                         (done? p)))
