select upsert.rcnt,
       upsert.has_error from upsert({{.schema}}, {{.table}}, {{.keys}}, {{.values}});
