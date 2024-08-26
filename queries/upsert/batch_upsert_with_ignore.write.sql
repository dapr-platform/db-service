select batch_upsert_with_ignore.ins,batch_upsert_with_ignore.upd,batch_upsert_with_ignore.has_error from batch_upsert_with_ignore({{.schema}}, {{.table}}, {{.keys}}, {{.ignore_keys}},{{.values}});
