create table PK.IDX_JID
(
    JID      serial primary key,
    JOURNAL  text not null,
    TS_ENTRY timestamp default current_timestamp
);

create view PK.VIEW_IDX_INSERT_JID as
select
    JOURNAL
    from PK.LZ_JOURNAL_RANKING
    where not JOURNAL in (
        select
            JOURNAL
            from PK.IDX_JID );

create function PK.JID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_JID( JOURNAL )
    select *
        from PK.VIEW_IDX_INSERT_JID;
    return null;
end;
$$
    language plpgsql;

create trigger TRIG_JID_IDX_SHIFT
    after insert
    on PK.LZ_JOURNAL_RANKING
    for each statement
execute procedure PK.JID_INDEXING();

