create table PK.TRACK_TAB_LZ_ACTIONS
(
    TABLE_NAME   text,
    ACTION       text,
    TS_INSERTION timestamp default current_timestamp
);

-- Baseline
create function PK.track_lz_journal_info_insert()
    returns trigger
as
$$
begin
    insert into PK.TRACK_TAB_LZ_ACTIONS
    select 'lz_journal_info', 'insert', now();

    return null;
end;
$$
    language plpgsql;


create trigger TRIG_TRACK_LZ_JOURNAL_INFO_INSERT
    after insert
    on PK.LZ_JOURNAL_INFO
    for each statement
execute function PK.track_lz_journal_info_insert();


-- COPIED CODE!
create function PK.track_lz_aref_inbox_insert()
    returns trigger
as
$$
begin
    insert into PK.TRACK_TAB_LZ_ACTIONS
    select 'lz_aref_inbox', 'insert', now();

    return null;
end;
$$
    language plpgsql;


create trigger TRIG_TRACK_LZ_AREF_INBOX_INSERT
    after insert
    on PK.LZ_AREF_INBOX
    for each statement
execute function PK.track_lz_aref_inbox_insert();


-- Delete Example
create function PK.track_lz_aref_inbox_delete()
    returns trigger
as
$$
begin
    insert into PK.TRACK_TAB_LZ_ACTIONS
    select 'lz_aref_inbox', 'delete', now();

    return null;
end;
$$
    language plpgsql;


create trigger TRIG_TRACK_LZ_AREF_INBOX_DELETE
    after delete
    on PK.LZ_AREF_INBOX
    for each statement
execute function PK.track_lz_aref_inbox_delete();
