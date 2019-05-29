-- Table: eva_kv

-- DROP TABLE eva_kv;

CREATE TABLE eva_kv
(
  namespace TEXT NOT NULL,
  id TEXT NOT NULL,
  attrs TEXT,
  val BYTEA,
  CONSTRAINT pk_ns_id PRIMARY KEY (namespace, id)
)
  WITH (
  OIDS=FALSE
);
