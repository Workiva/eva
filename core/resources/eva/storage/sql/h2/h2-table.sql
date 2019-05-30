CREATE TABLE IF NOT EXISTS
  eva_kv(
       namespace varchar(128),
       id varchar(128),
       attrs varchar(600),
       val blob,
       primary key (namespace, id)
);
