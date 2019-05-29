## Two Stories of Our Ancestors (Clause Ordering Pt. 2)

### Story Number One
Consider the following rule which turned out to be *incredibly* slow for the use case it was being used for:

```clojure
(def ancestor-rule
  "Rule that finds the ancestry of an entity
  returns the ancestors' eids"
 '[[(ancestor ?ancestor-eid ?descendant-eid)
    (parent ?ancestor-eid ?descendant-eid)]
   [(ancestor ?ancestor-eid ?descendant-eid)
    (parent ?ancestor-eid ?child-eid)
    (ancestor ?child-eid ?descendant-eid)]])
```

Why? Let's just step through it. The goal of this rule is to find all ancestors of a descendant, not all descendants of an ancestor. So let's invoke the rule: `(ancestor ?ancestor-eid ?descendant-eid)`, with `?descendant-eid` bound to an input.

The result will be the [logical disjunction (or set union)](https://en.wikipedia.org/wiki/Logical_disjunction) of the results of the two versions of the rule. The first rule fires:

```clojure
[(ancestor ?ancestor-eid ?descendant-eid)
 (parent ?ancestor-eid ?descendant-eid)]
 ```

This isn't bad. It will return the immediate parents of the descendant-eid that we passed in. The second rule fires:

```clojure
[(ancestor ?ancestor-eid ?descendant-eid)
 (parent ?ancestor-eid ?child-eid)
 (ancestor ?child-eid ?descendant-eid)]
  ```

This is terrible. We don't have bindings for `?ancestor-eid` or `?child-eid`, but the `parent` rule doesn't care -- it will merrily go look up _**every single parent-child relationship that exists in the database**_.

Even if we had bindings for `?ancestor-eid`, this is still not ideal, because the query engine will walk down the entire descendancy tree from the ancestor to all the leaves, only at the end eliminating leaves that are not equal to `?descendant-eid`.

In other words, the rule as written is perfectly *fine* if the goal is to find all descendants of an ancestor. If the goal is to verify that a particular entity is the descendant of an ancestor, it's *bad*. If the goal is to find all ancestors of a descendant, it's *very* __*very*__ bad.

Swapping two lines speeds it up:

```clojure
(def ancestor-rule
  "Rule that finds the ancestry of an entity
  returns the ancestors' eids"
 '[[(ancestor ?ancestor-eid ?descendant-eid)
    (parent ?ancestor-eid ?descendant-eid)]
   [(ancestor ?ancestor-eid ?descendant-eid)
    (ancestor ?child-eid ?descendant-eid)
    (parent ?ancestor-eid ?child-eid)]])
```

Again let us invoke the rule: `(ancestor ?ancestor-eid ?descendant-eid)`, with `?descendant-eid` bound to an input. Here the first rule fires:

```clojure
[(ancestor ?ancestor-eid ?descendant-eid)
 (parent ?ancestor-eid ?descendant-eid)]
 ```

This is fine. The query engine will now know (and cache) the parent(s) of `?descendant-eid`. Now the second rule fires:

```clojure
[(ancestor ?ancestor-eid ?descendant-eid)
 (ancestor ?child-eid ?descendant-eid)
 (parent ?ancestor-eid ?child-eid)]
  ```

The first clause is `(ancestor ?child-eid ?descendant-eid)`. This binds `?child-eid` to the now-known ancestors of `?descendant-eid`, its immediate parents.[1]

[1] Notice that this rule invocation is exactly the same as the first one, differing only in name: `(ancestor ?ancestor-eid ?descendant-eid)` vs. `(ancestor ?child-eid ?descendant-eid)`, where in both cases `?descendant-eid` have the same bindings and the ancestor binding is unknown. Invoking the rule again naively would result in an infinite loop. As an evaluation model for this kind of logic program, it is sufficient to bind `?child-eid` to the currently-known ancestors of `?descendant-eid` (which in this iteration happen to be its parents), and to move on. Once the rule has finished running, we'll run it again. As we run it in succession, we discover more layers in the ancestral hierarchy, until we can discover no more.

Then the second clause fires, `(parent ?ancestor-eid ?child-eid)`, but this time `?child-eid` has bindings, and we're not performing a full database scan.

Another way to speed this up would have been to give the `parent` rule a required binding. This would ensure that it never gets called with no bindings (a nice thing to ensure!), but it would also force a particular "direction" on the rule. If some queries wanted to use the rule to find children and other queries to find parents, you would have to use two rules. Consider the original "bad" `ancestor` definition with this change to `parent` (changed to `child` for the required bindings):

```clojure
(def ancestor-rule
  "Rule that finds the ancestry of an entity
  returns the ancestors' eids"
  '[[(ancestor ?ancestor-eid ?descendant-eid)
     (child ?descendant-eid ?ancestor-eid)]
    [(ancestor ?ancestor-eid ?descendant-eid)
     (child ?child-eid ?ancestor-eid)
     (ancestor ?child-eid ?descendant-eid)]])
```

Now the query engine would automatically re-order the clauses to respect the required bindings, and everything would move smoothly and efficiently.

### Story Number Two

Now consider this rule, which was being used in a similar fashion to our first example. Doubts were now doubted concerning its efficiency. 

```clojure
[[(ancestor [?parent] ?child)
  [?c :resource/parent ?parent]
  [?c :resource/id ?child]]
 [(ancestor [?parent] ?descendant)
  (ancestor ?child ?descendant)
  [?c :resource/parent ?parent]
  [?c :resource/id ?child]]]
```

But in this case, the goal *is* to pass in an ancestor and to find *all* its descendants. There is even a required binding which enforces this usage on all users of the rule. `?parent` always has a binding, so at no point is it possible to execute something so computationally dysfunctional as finding all parent-child relationships in the database.

So this is fine!