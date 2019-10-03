-- TYPE: TIME-SERIES

create table PK.CT_JOURNAL_RANK
(
    PUB      char(3),
    JOUR     char(6),
    RANK     integer,
    TS_ENTRY timestamp default current_timestamp,
    foreign key (PUB, JOUR) references PK.IDX_JID (PUB, JOUR)
);

create view PK.VIEW_SHUTTLE_JOURNAL_RANK as
select
    left(KUERZEL, 3)  as PUB,
    right(KUERZEL, 6) as JOUR,
    RANK,
    now()             as TS_ENTRY
    from PK.LZ_JOURNAL_INFO;

create view PK.VIEW_NEWBIES_JOURNAL_RANK as
select *
    from PK.VIEW_SHUTTLE_JOURNAL_RANK
    where not (PUB, JOUR, RANK) in (
        select PUB, JOUR, RANK
            from PK.CT_JOURNAL_RANK
    );
create view PK.VIEW_OLDIES_JOURNAL_RANK as
select
    CT.*,
    NEWB.TS_ENTRY as TS_ARCH
    from PK.CT_JOURNAL_RANK                     as CT
        inner join PK.VIEW_NEWBIES_JOURNAL_RANK as NEWB
                       on (CT.PUB, CT.JOUR) = (NEWB.PUB, NEWB.JOUR)
union all
select *, now()
    from PK.CT_JOURNAL_RANK
    where not (PUB, JOUR) in (
        select PUB, JOUR
            from PK.VIEW_SHUTTLE_JOURNAL_RANK )
;

create table PK.ARCH_JOURNAL_RANK as table PK.VIEW_OLDIES_JOURNAL_RANK with no data;

create function PK.ETL_JOURNAL_RANK()
    returns trigger
as
$$
begin
    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_JOURNAL_RANK
    select *
        from PK.VIEW_OLDIES_JOURNAL_RANK;

    -- delete newbies.
    delete
        from PK.CT_JOURNAL_RANK
        where (PUB, JOUR) in (
            select PUB, JOUR
                from PK.VIEW_OLDIES_JOURNAL_RANK
        );

    -- insert newbies.
    insert into PK.CT_JOURNAL_RANK
    select *
        from PK.VIEW_NEWBIES_JOURNAL_RANK;

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_1_ETL_JOURNAL_RANK
    after insert
    on PK.LZ_JOURNAL_INFO
    for each statement
execute procedure PK.ETL_JOURNAL_RANK();
