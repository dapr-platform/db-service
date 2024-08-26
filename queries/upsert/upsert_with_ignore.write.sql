select upsert_with_ignore.rcnt,
       upsert_with_ignore.has_error from upsert_with_ignore({{.schema}}, {{.table}}, {{.keys}},{{.ignore_keys}}, {{.values}});
