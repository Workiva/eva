# Clause Ordering: One Example

Consider the following query:

```clojure
(q '[:find [?target ...]
     :in [?pointer ...] $
     :where
     [?target :type :doc]
     [?pointer :target ?target]]
   pointers
   db)
```

Let's say that this is our database:

```clojure
[[3198 :target 114]
 [3199 :target 115]
 [3200 :target 116]
 [3201 :target 114]
 [3202 :target 119]
 [3203 :target 120]
 [3204 :target 117]
 [3205 :target 120]
 [3206 :target 121]
 [114 :type :doc]
 [115 :type :doc]
 [116 :type :doc]
 [117 :type :doc]
 [119 :type :folder]
 [120 :type :doc]
 [121 :type :doc]]
```

And let's say that these are the pointers we pass in:

```clojure
[3198 3199 3200 3201 3202 3203 3204 3205 3206]
```

You can run this example in eva to verify. When the query starts running, it hasn't assigned any value to any lvar except for ?pointer. ?pointer has 9 bindings. Then we encounter the first clause:

```clojure
[?target :type :doc]
```

Because ?target is unbound, we hit one of our indexes (AVET -- sorted by attribute, then value, then entity id) with a request for anything starting with `[:type :doc]` (under the AVET ordering). This returns the following datoms:

```clojure
[[114 :type :doc]
 [115 :type :doc]
 [116 :type :doc]
 [117 :type :doc]
 [120 :type :doc]
 [121 :type :doc]]
```

And now ?target is "bound" -- that is, we have six different values for it. Next clause:

```clojure
[?pointer :target ?target]
```

Remember that up until now you haven't told the query engine about *any* kind of relationship between ?pointer and ?target. There are 9 possible values for ?pointer and 6 possible values for ?target, and as far as the query engine knows, all 54 combinations are valid. Even if the query engine could take a break here, go inspect your schema, and discover that the pointer-to-target attribute is a many to one relationship, it would still have to check all 54 combinations.

So it asks an index (in this case, EAVT) to return all of the following which may be present in the database:

```clojure
[[3198 :target 114]
 [3198 :target 115]
 [3198 :target 116]
 [3198 :target 117]
 ...
 [3199 :target 114]
 [3199 :Target 115]
 ... ]
```

Now let's look at what happens when you reverse the ordering of the clauses:

```clojure
'[:find [?target ...]
  :in [?pointer ...] $
  :where
  [?pointer :target ?target]
  [?target :type :doc]]
```

Now we start off the query by telling the engine about the relationship between pointer and target:

```clojure
[?pointer :target ?target]
```

A request is sent to the EAVT index for all datoms beginning with `[3198 :target]` or `[3199 :target]` etc. These datoms are returned:

```clojure
[[3198 :target 114]
 [3199 :target 115]
 [3200 :target 116]
 [3201 :target 114]
 [3202 :target 119]
 [3203 :target 120]
 [3204 :target 117]
 [3205 :target 120]
 [3206 :target 121]]
```

Now ?target is bound to one of [114 115 116 117 119 120 121], but most importantly, the query engine no longer has to consider combinations such as `[3198 :target 120]` -- instead of 54 combinations of ?pointer and ?target, it is keeping track now only of 9. And because ?pointer and ?target now have some bindings, from this point on that number can only stay the same or get smaller.

Possible bindings:

```clojure
#{ {?pointer 3198, ?target 114}
   {?pointer 3199, ?target 115}
   {?pointer 3200, ?target 116}
   ... }
```

We run the final clause in this ordering:

```clojure
[?target :type :doc]
```

This generates 7 patterns for the index because there are 7 distinct bindings for ?target. This eliminates {?pointer 3202, ?target 119} from the set of possible results, leaving us with just 8. But there are only six distinct values for ?target, so the final query result is:

```clojure
[114 115 116 117 120 121]
```

There is some work in progress to mitigate the impact of some of the large cross products. However, even with this work in place, the relative efficiency of these two clause orderings will not change.
