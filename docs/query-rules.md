# Query Rules

Datalog allows you to define logical rules for use in any of your queries. These rules may be thought of as "logic subroutines," insofar as they encode complete queries and can be used within other queries. If you find that one of your queries is growing large and cumbersome, you should start decomposing it into rules.

Consider the following silly example:

```clojure
'[:find [?rival ...]
  :in $ ?parent
  :where
  [?parent :has-child ?child]
  [?child :attends ?uni-one]
  [?division :has-member ?uni-one]
  [?division :has-member ?uni-two]
  [?other-child :attends ?uni-two]
  [?rival :has-child ?other-child]]
```

Given a parent, return all parents whose children attend a rival school to any attended by one of that parent's children. Notice that there is a bit of repetition between some of the clauses, which encode this basic idea:

```clojure
'[:find [?rival ...]
  :in $ ?parent
  :where
  (has-child-in-uni ?parent ?uni-one)
  (sports-rivals ?uni-one ?uni-two)
  (has-child-in-uni ?rival ?uni-two)]
```

This is, in fact, *almost* exactly how you can write this query in Eva, provided that you have written the rules `has-child-in-uni` and `sports-rivals`. The first step in so doing is to write out the "head" of the rule, analogous to a subroutine signature. We have more or less already done this by writing the prototype query: `(has-child-in-uni ?parent ?university)` would be one, and `(sports-rivals ?university-1 ?university-2)` the other. The next step is to pull out the relevant clauses to be executed by the rule, being careful that you use the same variable names in the head of the rule as in the body. All of the clause types that you can write in the main body of your query you can write in your rule.

```clojure
;; has-child-in-uni head:
(has-child-in-uni ?parent ?university)

;;has-child-in-uni body:
[?parent :has-child ?child]
[?child :attends ?university]

;; sports-rivals head:
(sports-rivals ?uni-one ?uni-two)

;; sports-rivals body:
[?division :has-member ?uni-one]
[?division :has-member ?uni-two]
```

Note that `?child` is used in the body of `has-child-in-uni`, but is not used in the head. That is fine. What would *not* be legal is to write a rule that contains a variable in the head which does not appear in the body (there is an exception to this that we will get to later).

```clojure
;; AN ILLEGAL RULE:

;; head:
(bad-rule ?x ?y ?z)

;; body:
[?x :some-rel ?y]
```

In order to use rules in a query, you need to pass them in a special argument to the query. In your `:in` clause, the special symbol `%` is a placeholder for the rules argument; when the query parser sees that symbol, it knows to look for rules in the corresponding input to the query. The proper format for this argument is as a sequence of rule definitions, where each rule definition is a list whose head is the rule head and whose tail consists of the clauses in the body.

Here is our silly query above fully written out with rules:

```clojure
(eva.api/q '[:find [?rival ...]
             :in $ ?parent %
	     :where
	     (has-child-in-uni ?parent ?uni-one)
	     (sports-rivals ?uni-one ?uni-two)
	     (has-child-in-uni ?rival ?uni-two)]
	   my-db
	   "John Smith"
	   '[ ;; first rule definition in sequence
	      [(has-child-in-uni ?parent ?university) ; head
	       [?parent :has-child ?child]            ; & body 
	       [?chil :attends ?university]]

              ;; our second rule definition:
	      [(sports-rivals ?uni-one ?uni-two)      ; head
	       [?division :has-member ?uni-one]       ; & body
	       [?division :has-member ?uni-two]]
	    ])
```

In this way, you can store a library of logic rules written to your schema implementation, then use and re-use those throughout your query library. Managed well, you may even get by without even having to rewrite your queries if your schema changes -- just rewriting the relevant logic rules.

## Recursion

Suppose that our database stores genealogical data, and that one of the attributes in our database is `:has-child`. This is a cardinality-many attribute: each parent entity may have more than one value (child entity) under this attribute. If I want to ask for all the descendants of a particular entity, "John Smith," I can do this by writing a recursive query. That is, one of the definitions for descendant will contain a self-reference. One person (X) is descended from another (Y) if:

1. X is the child of Y.
2. X is the child of Z, someone else who is descended from Y.

Written out for Eva, this results in two distinct rule definitions for the same rule, just as we wrote them out separately above. This is safe recursion, because it is grounded with a non-recursive base case -- `:has-child` is an attribute we can just look up easily in the database.

```clojure
'[
  [(descends-from ?descendant ?ancestor)
   [?ancestor :has-child ?descendant]]

  [(descends-from ?descendant ?ancestor)
   [?intermediary :has-child ?descendant]
   (descends-from ?intermediary ?ancestor)]
]
```

Note that you *could* have combined these two definitions with an or-join into the following logically equivalent definition:

```clojure
'[
  [(descends-from ?descendant ?ancestor)
   (or-join [?descendant ?ancestor]
     [?ancestor :has-child ?descendant]
     (and [?intermediary :has-child ?descendant]
          (descends-from ?intermediary ?ancestor)))]
]
```

**But you shouldn't**. Besides suffering from reduced clarity, this relies on an `or-join` which, under the hood, is just compiled to an anonymous rule with the same two definitions we wrote above.

## Database sources (a.k.a. extensional sources)

In the rule definitions we wrote above, there are data patterns that do not specify an explicit database. This means that, within the context of that rule, there is a single database source that is implied. If your rule is called from a query form or another rule in which there is also an implicit db, then that db will implicitly be passed into the rule. However, you can always explicitly specify a database when calling a rule, as in the following example. Here we imagine that we have two databases, one storing the demographic information of patrons and the other storing information about the current locations of books. Given one book, we want a list of all other books currently checked out by the same household as that book (none if it is not checked out).

```clojure
'[:find [?other-book ...]
  :in $d $p ?book
  :where
  ($p checked-out ?patron ?book)
  (or-join [?patron ?other-book]
     (and ($d shares-household ?patron ?related)
          ($p checked-out ?related ?other-book))
     ($p checked-out ?patron ?other-book))]
```

As used here, this is compatible with the [grammar provided by Datomic](http://docs.datomic.com/query.html#rules). However, we have extended the grammar a little, allowing you to pass multiple database srcs into a rule -- but only if the rule has been explicitly defined to require multiple database sources.

```clojure
'[:find [?patron
  :in $d $p [?patron ...]
  :where
  ($d $p books-checked-out-by-same-household ?patron ?book)
```

Specifically, instead of the following (from the Datomic grammar):

```
rule-expr                  = [ src-var? rule-name (variable | constant | '_')+]
rule-head                  = [rule-name rule-vars]


```

Eva's grammar has:

```
rule-expr                  = [ src-var* rule-name (variable | constant | '_')+]
rule-head                  = [src-var* rule-name rule-vars]
```

Again, *rules which accept multiple src-vars __require__ that they be specified explicitly every time the rule is called.*

Aside from this difference in grammar, Eva's logic rules should be fully compatible with those of Datomic, and guides written with Datomic in mind should be relevant.