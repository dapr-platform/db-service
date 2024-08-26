SELECT
    col_description ( a.attrelid, a.attnum ) AS COMMENT,
    format_type ( a.atttypid, a.atttypmod ) AS type,
    a.attname AS NAME,
    a.attnotnull AS notnull
FROM
    pg_class AS c,
    pg_attribute AS a
WHERE
        c.relname ='{{.table_name}}'
  AND a.attrelid = c.oid
  AND a.attnum >0