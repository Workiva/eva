# Using a fully local persistent h2 connection with the v2 API

1. Create the file you want to use as the backing db
```bash
$> touch example/path/to/my.db
```

2. Start up eva, build (And save!) a config similar to the following,
   generating your own random UUIDs:

```clojure
(def config {:local true,
             :eva.v2.database.core/id #uuid "8b9b56bc-25a9-4ecc-8edf-ee5fb4b93509",
             :eva.v2.storage.value-store.core/partition-id #uuid "8230ebe8-84c0-45b8-b0d9-a5752c9fa031",
             :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
             :eva.v2.storage.block-store.impl.sql/db-spec
             {:classname "org.h2.Driver",
              :subprotocol "h2",
              :subname "example/path/to/my.db",
              :user "sa"}})
```

3. Connect! If you use the same config and the file is accessible,
   you should have a persistent fully-local connection ready to go

```clojure
(eva.api/connect config)
```
