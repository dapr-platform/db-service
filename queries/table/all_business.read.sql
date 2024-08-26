SELECT
    n.nspname as "schema",
    c.relname as "name",
    cast( obj_description ( c.relfilenode, 'pg_class' ) AS VARCHAR ) AS COMMENT,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 's' THEN 'special'
        WHEN 'f' THEN 'foreign_table'
        END as "type",
    pg_catalog.pg_get_userbyid(c.relowner) as "owner"
FROM
    pg_catalog.pg_class c,
    pg_catalog.pg_namespace n
where n.oid = c.relnamespace
  and n.nspname='public'
  and c.relkind in ('r','v','m','f')