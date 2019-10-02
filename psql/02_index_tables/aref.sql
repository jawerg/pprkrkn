create table PK.IDX_AID
(
    AID      serial primary key,
    AREF     text not null,
    TS_ENTRY timestamp default current_timestamp
);

create view PK.VIEW_IDX_INSERT_AID as
select
    AREF
    from PK.LZ_AREF_INBOX
    where not AREF in (
        select
            AREF
            from PK.IDX_AID );

create function PK.AID_INDEXING()
    returns trigger
as
$$
begin
    insert into PK.IDX_AID( AREF )
    select *
        from PK.VIEW_IDX_INSERT_AID;

    delete
        from PK.LZ_AREF_INBOX
        where 1 = 1;

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_AID_IDX_SHIFT
    after insert
    on PK.LZ_AREF_INBOX
    for each statement
execute procedure PK.AID_INDEXING();

