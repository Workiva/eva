-- DROP TABLE eva_kv;

CREATE TABLE eva_kv (
  namespace varchar(640) NOT NULL,
  id varchar(640) NOT NULL,
  attrs text,
  val longblob,
  CONSTRAINT pk_ns_id PRIMARY KEY (namespace, id)
)
  ENGINE=INNODB
