This document is being produced as an artifact of discussions surrounding how to acquire 
‘last updated’ information from Eva, and comes as a *very* rough guide for best practices.

# Problem

Users of Eva have wanted to be able to able to answer the specific question 
“Has this entity in this database been updated since the last time I’ve checked?” 

Suppose this entity is the top of a hierarchy of entities,
including some that can be arbitrarily nested. Given this, it’s important to have a proper definition 
for what ‘updated’ means in the above question.  Often our users have wanted
the most-covering definition, where if any attribute is added, retracted, or modified 
on the root entity or *any* descendant entity referenced underneath the root entity.

There are a variety of methods that can be used for acquiring and maintaining this information, 
this document seeks to capture as many of those as possible and discuss pros and cons. 

Ultimately, most of this is Eva-agnostic and comes down to proper data modeling and curation.
Because of the generality of data modeling, **this is not a problem where Eva 
can provide an out-of-the-box solution**. That being said, Eva does facilitate *specific*
potential solutions based on *your* requirements.

# Possible Solutions

The following are all potential solutions for tracking or discovering this updated
information, sorted very roughly from what we least recommend to most recommend. 

## Select all relevant information from a non-historic db-snapshot

To do this, you can define a query that selects the *full transitive closure*
of the relevant entities underneath the root entity and find the max tx-id 
among them. However, **this is not a correct solution for the problem as defined**. 
Since retracted datoms are not captured in the non-historic indexes getting the 
max tx-id of the datoms will not, in general, yield a correct answer to the
definition of ‘updated’ we’re going with. That being said, *this method may still
suffice for some applications* provided you only need to know monotonic additions to state.

	Advantages:
		Doesn’t require explicit modeling 
		Faster than scanning the history of the database 
	Disadvantages:
		**Not correct**, given the definition of ‘updated’ we’re considering
		Possibly slower than re-fetching the data and doing an equality check

## Select the relevant information from a history db-snapshot

This is probably the worst (correct) solution but carries the marginal advantage 
of not requiring any explicit modeling in the source db.  As with the above, you 
can define a query that selects the full transitive closure of the relevant entities
underneath the root entity and find the max tx-id. In a database at scale, this could 
result in having to touch **hundreds of millions 
of datoms**. *This will also scale with the full size of the history of data in the database.*
Obviously not ideal, but if the underlying model has not been built to support capturing 
this information it is still an option. 

	Advantages:
		Doesn’t require explicit modeling
	Disadvantages:
		Almost certainly slower than fetching and doing an equality check

## Scan the transaction log for changes

If you can initialize local state with a transitive closure query to know which entities 
are relevant, you can use the exposed Eva log api to filter each log entry to capture 
relevant updates as they occur.  This does require that a local copy of the entity 
hierarchy be instantiated s.t. updates captured can be reflected correctly in the
in-memory copy of the hierarchy.

	Advantages:
		Doesn’t require explicit modeling
		Can be relatively performant compared to the previous methods
		→ Once state is established, should be around O(tx-log-entries)
	Disadvantages:
		Requires a fair bit of explicit state construction outside the systems
		
## Maintain a local ‘last updated’ attribute on all entities in the hierarchy

Whenever an individual entity in the hierarchy is updated, you can contract that a
last-modified attribute is updated as well. Upon deletion of a nested entity, the 
parent’s entity would have to be updated as well.  Given this information, a query can 
be written to perform the transitive closure of the entities under the root and the max 
of the last-updated will accurately reflect the global last updated.

	Advantages:
		Relatively fast if the hierarchies stay small
		Relatively simple extension to a model
	Disadvantages:
		Scales with the number of entities in the hierarchy
		→ linearly with depth of hierarchy, logarithmically with width
		
## Use basisT as a coarse proxy for updated scopes

Eva database snapshots provide a basisT function that allows you to acquire a number 
that corresponds to the current ‘state’ of the database. This number is incremented every 
time a new transaction is added to the transaction log. Observing the basisT incrementing 
indicates that an update may have occurred on the entity of interest. This is an **approximate** 
solution, given the scoped use case, but is a very fast, data agnostic, and low-cost way to 
know if database state has advanced. If the database contains a large amount of data that is 
not contained in the hierarchy of interest, this could lead to a lot of false-positives on updates.

**Note**: this method *can* and *should* be used in concert with the other methods listed in this 
doc. An equal basisT on two databases from the same connection implies completely equivalent state.

	Advantages:
		Doesn’t require explicit modeling
		*Entirely* agnostic to underlying data model
		Fast: O(1) cost off of an instantiated database snapshot
	Disadvantages
		Only provides an indication that something has changed in the database
		→ May be too coarse an approximation for your application
		
## Maintain a single ‘last updated’ attribute per hierarchy root

There are two different ways this may implemented: either the root entities have an 
attribute that signifies last updated, or the transaction entity has an attribute that lists the 
entities ‘touched’ in this update. There is an onus of maintenance that this method induces, where
the hierarchy data must either be looked up or always kept in scope on the transactor for updates.

	Advantages:
		Fast: O(1) Space, Lookup per entity
	Disadvantages:
		Requires explicit modeling and upkeep
		→ Maintaining the attribute falls on the owners of the database
		→ Requires traversal to the root entity if that information is not kept in scope.	

