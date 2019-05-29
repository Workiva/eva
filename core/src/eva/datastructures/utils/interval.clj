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

(ns eva.datastructures.utils.interval
  (:require [eva.datastructures.protocols :as protocols]
            [eva.datastructures.utils.comparators :as data-comp]
            [utiliva.comparator :as comparison]))

(defrecord Interval
    [low_ high_ low-open_ high-open_]
  protocols/Interval
  (low [_] low_)
  (high [_] high_)
  (low-open? [_] low-open_)
  (high-open? [_] high-open_)
  (intersect [_ cmp other-range]
    (let [[low low-open] (cond (= (:low_ other-range) low_)
                               [low_ (or (:low-open_ other-range) low-open_)]
                               (comparison/> cmp (:low_ other-range) low_)
                               [(:low_ other-range) (:low-open_ other-range)]
                               :else
                               [low_ low-open_])
          [high high-open] (cond (= (:high_ other-range) high_)
                                 [high_ (or (:high-open_ other-range) high-open_)]
                                 (comparison/< cmp (:high_ other-range) high_)
                                 [(:high_ other-range) (:high-open_ other-range)]
                                 :else
                                 [high_ high-open_])]
      (Interval. low high low-open high-open)))
  (restrict [this cmp low-restriction high-restriction]
    (protocols/intersect this cmp (Interval. low-restriction high-restriction false false)))
  (interval-overlaps? [this cmp other-range]
    (let [low-cmp (if (or high-open_ (:low-open_ other-range))
                    comparison/<
                    comparison/<=)
          high-cmp (if (or low-open_ (:high-open_ other-range))
                     comparison/>
                     comparison/<=)]
      (and (low-cmp cmp (:low_ other-range) high_)
           (high-cmp cmp (:high_ other-range) low_))))
  (interval-overlaps? [this cmp low high]
    (and ((if high-open_ comparison/< comparison/<=) cmp low high_)
         ((if low-open_ comparison/> comparison/>=) cmp high low_)))
  (interval-contains? [this cmp point]
    (and ((if low-open_ comparison/< comparison/<=) cmp low_ point)
         ((if high-open_ comparison/> comparison/>=) cmp high_ point))))

(def ^:static infinite-interval (->Interval data-comp/LOWER data-comp/UPPER false false))
(defn closed-interval "Closed above and below: [low,high]" [low high] (->Interval low high false false))
(defn open-interval "Open above and below: (low,high)" [low high] (->Interval low high true true))
(defn interval
  "low-open? and high-open? indicate whether the interval is open/closed on below and above.
  This lets you construct all four of these intervals: (x,y) [x,y] (x,y] [x,y)"
  [low high low-open? high-open?]
  (->Interval low high low-open? high-open?))

;; FRESSIAN HANDLERS

(def interval-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (let [low (.readObject reader)
            high (.readObject reader)
            open-low (.readObject reader)
            open-high (.readObject reader)]
        (->Interval low high open-low open-high)))))

(def interval-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer obj]
      (.writeTag writer "eva/interval" 4)
      (.writeObject writer (.low_ ^Interval obj))
      (.writeObject writer (.high_ ^Interval obj))
      (.writeObject writer (.low-open_ ^Interval obj))
      (.writeObject writer (.high-open_ ^Interval obj)))))
