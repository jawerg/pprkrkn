create table PK.IDX_JID
(
    PUB       char(3),
    JOUR      char(6),
    PUBLISHER text,
    JOURNAL   text,
    JID       uuid not null unique default uuid_generate_v4(),
    TS_ENTRY  timestamp            default current_timestamp,
    primary key (PUB, JOUR)
);

create view PK.VIEW_IDX_INSERT_JID as
with
    SUBSTRINGS as (
        select
            left(KUERZEL, 3)  as PUB,
            right(KUERZEL, 6) as JOUR,
            PUBLISHER,
            JOURNAL
            from PK.LZ_JOURNAL_INFO
    )
select distinct *
    from SUBSTRINGS
    where not (PUB, JOUR) in (
        select PUB, JOUR
            from PK.IDX_JID );

create function PK.JID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_JID( PUB, JOUR, Publisher, JOURNAL )
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