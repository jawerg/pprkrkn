create table PK.IDX_JID
(
    JOURNAL  text,
    JOUR     char(6) primary key,
    JID      uuid not null unique default uuid_generate_v4(),
    TS_ENTRY timestamp            default current_timestamp
);

create view PK.VIEW_IDX_INSERT_JID as
select
    JOURNAL,
    right(KUERZEL, 6) as JOUR
    from PK.LZ_JOURNAL_INFO
    where not right(KUERZEL, 6) in (
        select
            JOUR
            from PK.IDX_JID );

create function PK.JID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_JID( JOURNAL, JOUR )
    select *
        from PK.VIEW_IDX_INSERT_JID;
    return null;
end;
$$
    language plpgsql;

create trigger TRIG_0_JID_IDX_SHIFT
    after insert
    on PK.LZ_JOURNAL_INFO
    for each statement
execute procedure PK.JID_INDEXING();