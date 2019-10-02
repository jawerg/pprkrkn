
create table PK.IDX_VID
(
    VID       serial primary key, -- P reserved for paper, thus Verlag for puplisher.
    PUBLISHER text not null,
    TS_ENTRY  timestamp default current_timestamp
);

create view PK.VIEW_IDX_INSERT_VID as
select
    PUBLISHER
    from PK.LZ_JOURNAL_RANKING
    where not PUBLISHER in (
        select
            PUBLISHER
            from PK.IDX_VID );

create function PK.VID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_VID( PUBLISHER )
    select *
        from PK.VIEW_IDX_INSERT_VID;
    return null;
end;
$$
    language plpgsql;

create trigger TRIG_VID_IDX_SHIFT
    after insert
    on PK.LZ_JOURNAL_RANKING
    for each statement
execute procedure PK.VID_INDEXING();
