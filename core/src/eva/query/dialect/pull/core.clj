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

(ns eva.query.dialect.pull.core
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [eva.query.dialect.spec :as qp]
            [eva.query.dialect.pull-helpers :as ph]
            [eva.attribute :refer [resolve-attribute]]
            [eva.error :refer [raise]]
            [eva.core :as core]
            [utiliva.core :refer [partition-map partition-pmap]]
            [map-experiments.smart-maps.protocol :as smart-maps]
            [eva.attribute :as attr]
            [recide.sanex :as sanex]
            [eva.utils.logging :refer [logged]]
            [barometer.aspects :refer [timed]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:import (eva Datom Database)))

(def ^:private ^:const +default-limit+ 1000)

(defn reverse-attr? [kw]
  (-> kw name (.charAt 0) (= \_)))

(defn classify-spec [[attr-key spec]]
  [attr-key
   (assoc spec :spec-class
          (if (= attr-key :db/id)
            :db/id
            (if (reverse-attr? attr-key)
              :rev-attr
              :fwd-attr)))])

(defn kwify-a [db datom]
  (update datom :a (smart-maps/inverse (:idents db))))

(defn subkey
  ([parent-eid [attr-key spec]] (subkey parent-eid attr-key spec))
  ([parent-eid attr-key spec] {:parent-eid parent-eid :attr-key attr-key :spec spec}))

;; Summarized from a discussion on github / hangouts:
;;
;; We may want this vv implementation of multi-select-datoms-ordered to be the baseline impl.
;; There are pros and cons to this behavior, so it is a design decision.
;;
;; As is, msdo will throw an exception if it encounters an attribute which it
;; cannot resolved, the following subverts that behavior by eliding any results.
;;
;; In terms of 'idiomatic' clojure contracts, the following impl is probably
;; a bit more canonical, but I think it can lead to 'implicit' failures
;; from things like typos, so I'm not entirely convinced one way or the other.

(defn multi-select-datoms-ordered*
  "A variant of multi-select-datoms-ordered that will only execute the
   search on the criteria that have attributes which we can resolve."
  [db [index-name & comp-colls]]
  {:pre [(or (= :eavt index-name) (= :vaet index-name))]}
  (partition-map (fn [[_ a]] (contains? (:idents db) a))
                 {true #(core/multi-select-datoms-ordered db (cons index-name %))
                  false #(map (constantly #{}) %)}
                 comp-colls))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; State
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol PullCollapseState
  (collapse-complete? [state])
  (ascend [state db] "Ascend during the collapse of a pull frame tree, merging a child onto a parent.")
  (descend [state]   "Descend during the collapse of a pull frame tree, traversing from a parent to a child"))

(defprotocol PullFrame
  ;; basically setters and getters
  (specs [frame] "Return the sequence of specs from this frame's pattern")
  (set-data! [frame data] "Sets the pull-frames data as a whole.")
  (update-data! [frame eid attr-key data] "Adds 'data' to the frames internal data under eid and attr-key")
  (set-subframe-map! [frame subframes])
  (assoc-subframe! [frame k subframe] "Adds a subframe to this pull frame under the key 'k'")
  (subframe-map [frame])
  (merge-info [frame] "Retrieve the information for merging this frame onto its parent")
  (subframes [frame])
  (data->final-map [frame] "Take the data stored in the frame and finalize it for collapse")
  (frame->flattened-specs [frame])

  ;; 'actual' interesting operations on the pull frame
  (realize! [frame db data]
    "Given a map of data selected across all entity ids and specs, realize the frame")
  (realize-spec! [this db eid spec datoms]
    "The primary workhorse of the expansion process.  Given the eid, spec, and
     datoms realized therefor, 'evaluate' the spec and merge the results into
     the state of the frame.")
  (add-recursion! [this multi? rec-depths join-spec parent-eid rec-comp? child-eids]
    "Add a recursive frame per child eid.")
  (collapse!  [frame db]
    "Collapses a frame to its final pull-result state.")
  (seqify-subframes! [frame]
    "Replaces the subframe map with a seq of itself. Pretty gross.")
  (consume-child! [this db child-frame]
    "Merge the data from the child frame into this frame and remove the child from subframes."))

(declare subpattern-frame)

(defrecord PullCollapseStateImpl [cur-frame parents]
  PullCollapseState
  (collapse-complete? [state]
    (seqify-subframes! cur-frame)
    (and (empty? (subframe-map cur-frame))
         (empty? parents)))

  (descend [state]
    (let [sframes (subframes cur-frame)]
      (-> state
          (update :parents conj cur-frame)
          (assoc :cur-frame (first sframes)))))

  (ascend [state db]
    (let [new-parent (consume-child! (first parents) cur-frame db)]
      (assoc state :parents (rest parents), :cur-frame new-parent))))

;; A frame can be thought of as a point in the pull where we join together
;; the pull's pattern with a collection of entity-ids on which we're going to
;; evaluate said pattern.
;;
;; we are also using them to control the shape of any nested or recursive
;; expansions that result from the evaluation of the pull. Under _subframes
;; we are building a tree of subframes based on this.
;;
;; NOTE: The following constraints were assumed when designing and building
;; the functionality in this file, centered mostly in this class:
;;
;;  1) Pulls can have database-scale recursion.
;;
;;     As a result of this, we should build s.t. we do not recurse via
;;     the call stack.
;;
;;  2) Corollary: We have no way to know the structure or nesting of the
;;     pull a priori.
;;
;;     Because of this, we also want the 'structure' of the pull's data / result
;;     to be decoupled from the structure of the calculation--at each iteration
;;     we want to be able to know the next nodes we want to modify independently
;;     from the nodes we've already seen. I couldn't come up with an immediate
;;     way to couple these things in a purely-functional style using persistent
;;     datastructures, so I decided to crutch on mutation for handling the
;;     data and subframe tree of the pull.
;;
;;  3) We want to batch as much IO together as possible, minimizing the total
;;     number of index accesses
;;
;;     Since our index structures very amenably support batch-style reads,
;;     we want to evaluate as much of the pull as possible at each step.
;;     Because of this, we want the ability to keep a 'frontier' of frames
;;     available for expansion without affecting or needing to traverse their
;;     parents.  This also confounded my first attempts at a purely-functional
;;     approach, and motivated the switch to having a mutable basis for the
;;     subframes and data.

(deftype PullFrameImpl [_eids ;; [eid]
                        pattern
                        multi? ;; bool for how to merge us back in to a parent
                        ^:unsynchronized-mutable _subframes ;; {[parent-eid join-spec] --> subframe}
                        ^:unsynchronized-mutable _data ;; {eid --> eid-pull-res}
                        _merge-info
                        seen-eids ;; #{eids} for use
                        recursion-depths ;; (atom {attr->depth})
                        rec-component?]
  PullFrame
  (set-subframe-map! [frame subframes] (set! _subframes subframes))
  (assoc-subframe! [frame k subframe] (set-subframe-map! frame (assoc _subframes k subframe)))
  (seqify-subframes! [frame] (set! _subframes (seq _subframes)))
  (set-data! [frame data] (set! _data data))
  (update-data! [frame eid k v] (set-data! frame (update _data eid assoc k v)))
  (subframes [this] (vals _subframes))
  (subframe-map [this] _subframes)
  (specs [this] (-> pattern :attr-specs seq))
  (merge-info [_] _merge-info)
  (frame->flattened-specs [this]
    (cond->> (for [e _eids, s (specs this)] [e s])
      (:wildcard? pattern) (into (for [e _eids] [e :wildcard]))))
  (data->final-map [_]
    (into []
          (comp (map (juxt identity (partial get _data)))
                (remove (comp nil? second))
                (map (fn [[eid data]] (cond-> data
                                        rec-component? (assoc :db/id eid)))))
          _eids))

  (realize! [this db data]
    (when (:wildcard? pattern)
      (doseq [eid _eids]
        (let [non-wc-attrs (into #{}
                                 (map (comp :normalized-attr-name second))
                                 (specs this)) ;; explicit specs override wildcard.
              wc-data (->>
                       (sequence (comp (map (partial kwify-a db))
                                       (remove (comp non-wc-attrs :a)))
                                 (get data [eid :wildcard]))
                       (group-by :a))]
          ;; realize db-id
          (realize-spec! this db eid [:db/id {:normalized-attr-name :db/id}]
                         ;; if we're in the context of a merge, we know the eid is at least referred to,
                         ;; so we have to return the eid.  If we're in a root wildcard, this is not the case.
                         (if (some? _merge-info) [{:e eid}] (get data [eid :wildcard])))
          (doseq [[attr-key datoms] wc-data]
            ;; build a 'new' spec here, 'cause wildcard vv
            (realize-spec! this db eid [attr-key {:normalized-attr-name attr-key}] datoms)))))
    (doseq [eid _eids, spec (specs this)]
      (realize-spec! this db eid spec (get data [eid spec]))))

  (realize-spec! [this db eid full-spec datoms]
    (let [[attr-key spec] full-spec
          limit (get spec :limit +default-limit+)
          found (not-empty
                 (cond->> datoms
                   limit (into [] (take limit))))]
      (cond
        (not found)
        ;; special case: didn't find anything
        (when (contains? spec :default)
          (update-data! this eid attr-key (:default spec)))

        ;; special case: handling :db/id
        (= :db/id attr-key)
        (update-data! this eid :db/id (-> datoms first :e))

        ;; general case: we found something
        :else
        ;; build up a bunch of predicates.
        (let [attr       (resolve-attribute db (:normalized-attr-name spec))
              ref?       (attr/ref-attr? attr)
              component? (and ref? (attr/is-component? attr))
              forward?   (not (reverse-attr? attr-key))
              multi?     (if forward? (attr/card-many? attr) (not component?))
              datom-val  (if forward? :v :e)
              as-value   (cond->> datom-val
                           ref? (comp #(hash-map :db/id %)))
              rec-comp?  (true? (and component?
                                     forward?
                                     (not (contains? spec :recursion-limit))
                                     (not (contains? spec :pattern))))]
          (cond
            ;; case: this spec has a subpattern, we need to push a subframe for it.
            (contains? spec :pattern)
            (let [sub-eids (mapv datom-val found)
                  skey (subkey eid full-spec)]
              (->> (subpattern-frame sub-eids (:pattern spec) multi? skey seen-eids recursion-depths rec-comp?)
                   (assoc-subframe! this skey)))

            ;; case: this spec is defined as recursive *or* we're traversing an is-component attr.
            (or (contains? spec :recursion-limit) rec-comp?)
            (let [child-eids (mapv datom-val found)
                  rec-depths (update recursion-depths attr-key (fnil inc -1))]
              (doall (partition-map
                      (fn recursion-case [child-eid]
                        (cond
                          (= (get rec-depths attr-key) (:recursion-limit spec))
                          :at-depth  ;; we terminate recursion if we hit max depth
                          (contains? seen-eids child-eid)
                          :at-cycle  ;; we terminate recursion if we hit an eid on this path before
                          :else
                          :recurse)) ;; we add a recursion frame
                      ;; at max depth we just pass the eids through
                      {:at-depth (fn [child-eids] child-eids)
                       ;; when we hit a cycle we terminate with a {:db/id <eid>} to show where we're terminating
                       :at-cycle (fn [child-eids]
                                   (doseq [child-eid child-eids]
                                     (if multi?
                                       (set-data! this (update-in _data [eid attr-key] (fnil conj []) {:db/id child-eid}))
                                       (update-data! this eid attr-key {:db/id child-eid}))))
                       ;; push a recursion frame
                       :recurse (partial add-recursion! this multi? rec-depths full-spec eid rec-comp?)}
                      child-eids))
              this)

            ;; case: this spec is boring and we can just tape the results directly into the data.
            :else
            (->> (cond-> (into [] (map as-value) found)
                   (not multi?) first)
                 (update-data! this eid attr-key)))))))

  (add-recursion! [this multi? rec-depths join-spec parent-eid rec-comp? child-eids]
    (doseq [child-eid child-eids]
      (let [[attr-key spec] join-spec
            rec-frame-key (subkey parent-eid join-spec)
            rec-frame (subpattern-frame child-eids
                                        (cond-> pattern
                                          rec-comp? (assoc :wildcard? true
                                                           :attr-specs {}))
                                        multi? rec-frame-key
                                        (cond-> (conj seen-eids child-eid)
                                            rec-comp?
                                            (conj parent-eid))
                                        rec-depths
                                        rec-comp?)]
        (assoc-subframe! this rec-frame-key rec-frame))))

  (consume-child! [this child db]
    (let [minfo (merge-info child)
          subframes' (rest _subframes)
          attr       (resolve-attribute db (:normalized-attr-name (:spec minfo)))
          ref?       (attr/ref-attr? attr)
          component? (and ref? (attr/is-component? attr))
          forward?   (not (reverse-attr? (:attr-key minfo)))
          multi?     (if forward? (attr/card-many? attr) (not component?))
          data' (if multi?
                  (update-in _data [(:parent-eid minfo) (:attr-key minfo)] (fnil into []) (data->final-map child))
                  (update _data (:parent-eid minfo) assoc (:attr-key minfo) (first (data->final-map child))))]
      (set-data! this data')
      (set-subframe-map! this subframes')
      this))

  (collapse! [this db]
    (loop [state (map->PullCollapseStateImpl {:cur-frame this, :parents ()})]
      (if (collapse-complete? state)
        (data->final-map this)
        (cond
          ;; if we're not at a terminal node, descend
          (not-empty (subframe-map (:cur-frame state)))
          (recur (descend state))
          ;; if we're at a terminal node
          :else
          (recur (ascend state db)))))))

(defn subpattern-frame [eids pattern multi? merge-info seen-eids recursion-depths rec-comp?]
  (PullFrameImpl. eids
                  pattern
                  multi?
                  nil
                  (zipmap eids (repeat nil))
                  merge-info
                  seen-eids
                  recursion-depths
                  rec-comp?))

(defn initial-frame [eids pattern multi?]
  (PullFrameImpl. eids
                  pattern
                  multi?
                  nil
                  (zipmap eids (repeat nil))
                  nil
                  #{}
                  {}
                  false)) ;; seen entity-ids

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Selection
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn fetch-datoms
  "query is a sequence like [[entity-id-to-query full-spec]]
   where full-spec is a tuple [attr-key attr-spec]
   where attr-spec is a map containing at least the normalized attribute ident
   under the keyword :attr"
  [db index-name query]
  (let [flat-query (cons index-name (for [[e spec] query]
                                      [e (-> spec second :normalized-attr-name)]))
        q-res (multi-select-datoms-ordered* db flat-query)]
    (zipmap query q-res)))

(defn shape-db-id [db query]
  (zipmap query (map #(vector {:e (first %)}) query)))

(defn fetch-wildcard [db query]
  (zipmap query (core/multi-select-datoms-ordered
                 db
                 (cons :eavt (map (comp vector first) query)))))

(defn select
  "Given a query as [[eid spec]], yields a map of:
   {[eid spec] --> multi-select-result-on-eid+spec}"
  [db query]
  (->> query
       (partition-pmap (fn spec->class [[eid spec]]
                         ;; generally, eid+full-spec == [entid < full-spec | :wildcard >]
                         ;;
                         ;; three example eid+specs:
                         ;; [8796093023249 [:part {:attr :part, :recursion 1, :spec-class :fwd-attr}]]
                         ;; [19            [:db/ident {:attr :db/ident, :spec-class :fwd-attr}]]
                         ;; [19            :wildcard]
                         (if (= :wildcard spec) :wildcard
                             (-> spec second :attr-type)))
                       {:db/id    (partial shape-db-id db)
                        :fwd-attr (partial fetch-datoms db :eavt) ;; :fwd
                        :rev-attr (partial fetch-datoms db :vaet) ;; :_rev
                        :wildcard (partial fetch-wildcard db)})
       (into {})))

(defn realize-frontier
  "Gathers data for the requisite queries across the pull's frontier"
  [db frontier]
  (let [eids+specs (mapcat frame->flattened-specs frontier) ;; flat list of [eids specs]
        query-res (select db eids+specs)] ;; map of {[eid spec] --> selection}]
    (mapv #(realize! % db query-res) frontier)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn pull-pattern [db root frontier]
  (if (empty? frontier)
    (collapse! root db)
    (do (realize-frontier db frontier)
        (recur db root (mapcat subframes frontier)))))

(defn- ensure-edn [x] (if (string? x) (edn/read-string x) x))

(defn conform-pull-spec [pull-spec]
  (let [conformed (s/conform ::qp/pattern-data-literal (ensure-edn pull-spec))]
    (if (s/invalid? conformed)
      (raise :pull/invalid-spec
             (str "\n" (s/explain-str ::qp/pattern-data-literal pull-spec))
             {:pull-spec pull-spec,
              :explain-data (s/explain-data ::qp/pattern-data-literal pull-spec),
              ::sanex/sanitary? false})
      conformed)))

(defn pull-spec
  [^Database db selector eids multi?]
  (let [pattern (ph/normalize-pdl (conform-pull-spec selector))
        deduped-eids (distinct eids)
        res (partition-map nil?
                           {true  (fn [ns] (vec (for [_ ns] nil)))
                            false #(let [init (initial-frame (vec %) pattern true)]
                                     (pull-pattern db init [init]))}
                           (.entids db deduped-eids))]
    (map (zipmap deduped-eids res) eids)))

(d/defn ^{::d/aspects [(logged) traced timed]} pull [db selector eid]
  (first (pull-spec db selector [eid] false)))

(d/defn ^{::d/aspects [(logged) traced timed]} pull-many [db selector eids]
  (pull-spec db selector eids true))
