create table PK.IDX_VID
(
    PUBLISHER text,
    PUB       char(3) primary key,
    VID       uuid not null unique default uuid_generate_v4(), -- P reserved for paper, thus Verlag for puplisher.
    TS_ENTRY  timestamp            default current_timestamp
);

create view PK.VIEW_IDX_INSERT_VID as
select
    PUBLISHER,
    left(KUERZEL, 3) as PUB
    from PK.LZ_JOURNAL_INFO
    where not left(KUERZEL, 3) in (
        select
            PUB
            from PK.IDX_VID );

create function PK.VID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_VID( PUBLISHER, PUB )
    select *
        from PK.VIEW_IDX_INSERT_VID;
    return null;
end;
$$
    language plpgsql;

create trigger TRIG_0_VID_IDX_SHIFT
    after insert
    on PK.LZ_JOURNAL_INFO
    for each statement
execute procedure PK.VID_INDEXING();