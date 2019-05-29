This document enumerates some differences between Eva and Datomic

* Eva has no implementation of the top-level functions:
  * `sync`
  * `filter`
  * `since`
  * `tx-report-queue`
* The `transact` function / core data model in Eva does not currently support:
  * Byte-type attributes
    * note: Eva has a partial implementation that has been tested that has issues
  * Excision
  * Schema alterations (adding new schema is supported, modifying extant schema is not)
  * Inferring of installation attributes on transaction data
  * Inferring implicit `:db/id`s at the root level of a map (implicit `:db/ids` on nested entity maps is supported)
  * String-type tempids
  * Attributes with `:db/noHistory`, `:db/fulltext`, `:db/index`
    * note: all non-byte attributes are indexed in AVET by default
  * Database functions written in Java
* Eva's `q` api does not support:
   * A subset of the built-in expression functions and predicates
* Eva's `pull` api does not support:
   * The "Attributes with Options" sub-grammar
* Eva has a few specific features that are supported that Datomic does not:
  * There are a few minor API functions we have added or renamed
  * Eva's query engine also has more general support for cross-database queries
  * Eva's `connect` call is passed a baroque configuration map, whereas datomic uses a uri
  * Eva has a more typeful exception set
  * Eva's Client model is quite distinct from  Datomic Cloud's Client
