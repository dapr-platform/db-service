select batch_upsert.ins,batch_upsert.upd,batch_upsert.has_error from batch_upsert({{.schema}}, {{.table}}, {{.keys}}, {{.values}});
