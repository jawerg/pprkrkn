create table PK.IDX_AID
(
    PUB      char(3),
    JOUR     char(6),
    ART      text not null,
    AREF     text not null unique,
    AID      uuid primary key,
    TS_ENTRY timestamp,
    foreign key (PUB, JOUR) references PK.IDX_JID (PUB, JOUR)
);

call PK.gen_tracking_functions('IDX_AID');

create view PK.VIEW_IDX_INSERT_AID as
with
    SUBSTRINGS as (
        select distinct
            substr(AREF, 4, 3) as PUB,
            substr(AREF, 8, 6) as JOUR,
            substr(AREF, 15)   as ART,
            AREF,
            md5(''
                    || substr(AREF, 4, 3)
                    || substr(AREF, 8, 6)
                || substr(AREF, 15)
                )::uuid        as AID,
            TS_ENTRY
            from PK.LZ_AREF_INBOX
            where left(AREF, 3) = '/a/'
    )
select *
    from SUBSTRINGS
    where not AREF in (
        select
            AREF
            from PK.IDX_AID );

create function PK.AID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_AID
    select *
        from PK.VIEW_IDX_INSERT_AID;

    -- delete transferred articles.
    delete
        from PK.LZ_AREF_INBOX
        where AREF in (
            select
                AREF
                from PK.IDX_AID );

    -- theoretically, there could by an accumulation of non-transmitted RePEc IDs, thus
    -- delete duplicates by keeping only the oldest entry.
    delete
        from PK.LZ_AREF_INBOX as BASE
            using PK.LZ_AREF_INBOX as DUP
        where BASE.AREF = DUP.AREF
          and DUP.TS_ENTRY > BASE.TS_ENTRY;

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_AID_IDX_SHIFT
    after insert
    on PK.LZ_AREF_INBOX
    for each statement
execute procedure PK.AID_INDEXING();