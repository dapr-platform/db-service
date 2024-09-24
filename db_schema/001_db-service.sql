-- +goose Up
-- +goose StatementBegin
CREATE VIEW dual AS
SELECT 'X'::varchar AS dummy;
create table f_sql_upsert_err ( err varchar(10240), time timestamp);

CREATE OR REPLACE FUNCTION public.upsert(
    nsp name,
    tbl name,
    keys text[],
    icontent json,
    OUT rcnt integer, OUT has_error integer)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS
$BODY$

declare
    k       text;
    v       text;
    sql1    text := 'update ' || quote_ident(nsp) || '.' || quote_ident(tbl) || ' set ';
    sql2    text := 'insert into ' || quote_ident(nsp) || '.' || quote_ident(tbl) || ' (';
    sql3    text := 'values (';
    sql4    text := ' where ';
    sqlp    text := '';
    v_text1 text;
    v_text2 text;
    key_str text;
begin
    rcnt := 0;
    has_error := 0;
    key_str :=array_to_string(keys,',');
    for k,v in select * from json_each_text(icontent)
        loop
            sql1 := sql1 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ',';
            if (position(k in key_str)>0) then
                sql4 := sql4 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ' and ';
            end if;
            --if (not array [k] && keys) then
            --    sql1 := sql1 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ',';
            --else
            --    sql4 := sql4 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ' and ';
            --end if;
        end loop;
    sqlp := rtrim(sql1, ',') || rtrim(sql4, 'and ');

    execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    --RAISE NOTICE 'update sql value %',rtrim(sql1, ',') || rtrim(sql4, 'and ');

    --RAISE NOTICE 'rcnt value %',rcnt;
    if rcnt = 0 then

        for k,v in select * from json_each_text(icontent)
            loop
                sql2 := sql2 || quote_ident(k) || ',';
                sql3 := sql3 || coalesce(quote_literal(v), 'NULL') || ',';
            end loop;
        RAISE NOTICE 'insert sql value %',rtrim(sql2, ',') || ') ' || rtrim(sql3, ',') || ') ';

        execute rtrim(sql2, ',') || ') ' || rtrim(sql3, ',') || ') ';

    end if;

    return;

exception
    when others then get stacked diagnostics v_text1= MESSAGE_TEXT,
        v_text2= PG_EXCEPTION_CONTEXT;
    --execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
    --RAISE NOTICE 'excep sql1 value %',sql1;
    --RAISE NOTICE 'excep sql4 value %',sql4;
    has_error := 1;
    insert into f_sql_upsert_err values(sqlerrm || v_text1||' '||v_text2,now());
    RAISE EXCEPTION '异常:%,%,%,%',sqlstate ,sqlerrm,v_text1,v_text2;
    return;
end;
$BODY$;



CREATE OR REPLACE FUNCTION public.batch_upsert(
    nsp name,
    tbl name,
    keys text[],
    js json[],
    OUT ins integer,
    OUT upd integer,
    OUT has_error integer)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS
$BODY$

declare
    icontent json;
    res      record;

begin
    ins := 0;
    upd := 0;
    has_error := 0;
    for icontent in select * from unnest(js)
        loop
            select upsert.rcnt, upsert.has_error
            from upsert(nsp, tbl, keys, icontent)
            into res; -- 调用单次请求的函数

            ins := ins + case res.rcnt when 0 then 1 else 0 end;
            upd := upd + case res.rcnt when 0 then 0 else 1 end;
            has_error := has_error + res.has_error;
        end loop;
    return;
end;
$BODY$;

CREATE OR REPLACE FUNCTION public.upsert_with_ignore(
    nsp name,
    tbl name,
    keys text[],
    ignore_keys text[],
    icontent json,
    OUT rcnt integer, OUT has_error integer)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS
$BODY$

declare
    k       text;
    v       text;
    sql1    text := 'update ' || quote_ident(nsp) || '.' || quote_ident(tbl) || ' set ';
    sql2    text := 'insert into ' || quote_ident(nsp) || '.' || quote_ident(tbl) || ' (';
    sql3    text := 'values (';
    sql4    text := ' where ';
    sqlp    text := '';
    v_text1 text;
    v_text2 text;

begin
    rcnt := 0;
    has_error := 0;

    for k,v in select * from json_each_text(icontent)
        loop
            if not k = any(ignore_keys) then
                sql1 := sql1 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ',';
            end if;
            if k = any(keys) then
                sql4 := sql4 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ' and ';
            end if;
            --if (not array [k] && keys) then
            --    sql1 := sql1 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ',';
            --else
            --    sql4 := sql4 || quote_ident(k) || '=' || coalesce(quote_literal(v), 'NULL') || ' and ';
            --end if;
        end loop;
    sqlp := rtrim(sql1, ',') || rtrim(sql4, 'and ');

    execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
    GET DIAGNOSTICS rcnt = ROW_COUNT;
    --RAISE NOTICE 'update sql value %',rtrim(sql1, ',') || rtrim(sql4, 'and ');

    --RAISE NOTICE 'rcnt value %',rcnt;
    if rcnt = 0 then

        for k,v in select * from json_each_text(icontent)
            loop
                sql2 := sql2 || quote_ident(k) || ',';
                sql3 := sql3 || coalesce(quote_literal(v), 'NULL') || ',';
            end loop;
        RAISE NOTICE 'insert sql value %',rtrim(sql2, ',') || ') ' || rtrim(sql3, ',') || ') ';

        execute rtrim(sql2, ',') || ') ' || rtrim(sql3, ',') || ') ';

    end if;

    return;

exception
    when others then get stacked diagnostics v_text1= MESSAGE_TEXT,
        v_text2= PG_EXCEPTION_CONTEXT;
    --execute rtrim(sql1, ',') || rtrim(sql4, 'and ');
    --RAISE NOTICE 'excep sql1 value %',sql1;
    --RAISE NOTICE 'excep sql4 value %',sql4;
    has_error := 1;
    insert into f_sql_upsert_err values(sqlerrm || v_text1||' '||v_text2,now());
    RAISE EXCEPTION '异常:%,%,%,%',sqlstate ,sqlerrm,v_text1,v_text2;
    return;
end;
$BODY$;


CREATE OR REPLACE FUNCTION public.batch_upsert_with_ignore(
    nsp name,
    tbl name,
    keys text[],
    ignore_keys text[],
    js json[],
    OUT ins integer,
    OUT upd integer,
    OUT has_error integer)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS
$BODY$

declare
    icontent json;
    res      record;

begin
    ins := 0;
    upd := 0;
    has_error := 0;
    for icontent in select * from unnest(js)
        loop
            select upsert_with_ignore.rcnt, upsert_with_ignore.has_error
            from upsert_with_ignore(nsp, tbl, keys, ignore_keys, icontent)
            into res; -- 调用单次请求的函数

            ins := ins + case res.rcnt when 0 then 1 else 0 end;
            upd := upd + case res.rcnt when 0 then 0 else 1 end;
            has_error := has_error + res.has_error;
        end loop;
    return;
end;
$BODY$;


CREATE OR REPLACE FUNCTION public.backup_one_table(
    tbl name,
    OUT ret integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT PARALLEL UNSAFE
AS
$BODY$

declare
    timestr text;
    dest_tbl_name text;
    sql text;
begin
    timestr:=to_char(now(),'YYYYMMDD');
    dest_tbl_name:=tbl||'_'||timestr;
    drop table if exists  dest_tbl_name;
    sql:='create table '||dest_tbl_name||' as select * from '||tbl;
    execute sql;
    ret:=0
        return;

end;
$BODY$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop view if exists dual;
drop table if exists f_sql_upsert_err ;
drop function public.batch_upsert_with_ignore;
drop function public.upsert_with_ignore;
drop function public.upsert;
drop function public .batch_upsert;
-- +goose StatementEnd
