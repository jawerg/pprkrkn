-- Droplet
drop trigger TRIG_ETL_JOURNAL_RANKING on PK.LZ_JOURNAL_RANKING;
drop function PK.ETL_JOURNAL_RANKING;
drop view PK.VIEW_OLDIES_JOURNAL_RANKING;
drop view PK.VIEW_NEWBIES_JOURNAL_RANKING;
drop table PK.ARCH_JOURNAL_RANKING;
drop table PK.CT_JOURNAL_RANKING;
drop view PK.VIEW_HASH_JOURNAL_RANKING;

create view PK.VIEW_HASH_JOURNAL_RANKING as
select
    KUERZEL,
    md5(cast(TAB.* as text))::uuid as HASHVAL,
    JOURNAL,
    PUBLISHER,
    FACTOR,
    ADJ_CITATIONS,
    N_ARTICLES,
    N_CITATIONS,
    now()                          as TS_ENTRY
    from PK.LZ_JOURNAL_RANKING as TAB;

create table PK.CT_JOURNAL_RANKING as table PK.VIEW_HASH_JOURNAL_RANKING with no data;

create view PK.VIEW_NEWBIES_JOURNAL_RANKING as
select *
    from PK.VIEW_HASH_JOURNAL_RANKING
    where not (KUERZEL, HASHVAL) in (
        select
            KUERZEL,
            HASHVAL
            from PK.CT_JOURNAL_RANKING
    );

create view PK.VIEW_OLDIES_JOURNAL_RANKING as
select
    CT.*,
    NEWB.TS_ENTRY as TS_ARCH
    from PK.CT_JOURNAL_RANKING                     as CT
        inner join PK.VIEW_NEWBIES_JOURNAL_RANKING as NEWB
                       on CT.KUERZEL = NEWB.KUERZEL
;

create table PK.ARCH_JOURNAL_RANKING as table PK.VIEW_OLDIES_JOURNAL_RANKING with no data;

create function PK.ETL_JOURNAL_RANKING()
    returns trigger
as
$$
begin
    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_JOURNAL_RANKING
    select *
        from PK.VIEW_OLDIES_JOURNAL_RANKING;

    -- delete newbies.
    delete
        from PK.CT_JOURNAL_RANKING
        where KUERZEL in (
            select
                KUERZEL
                from PK.VIEW_OLDIES_JOURNAL_RANKING
        );

    -- insert newbies.
    insert into PK.CT_JOURNAL_RANKING
    select *
        from PK.VIEW_NEWBIES_JOURNAL_RANKING;

    -- everything must have been processed.
    delete
        from PK.LZ_JOURNAL_RANKING
        where 1 = 1;

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_ETL_JOURNAL_RANKING
    after insert
    on PK.LZ_JOURNAL_RANKING
    for each statement
execute procedure PK.ETL_JOURNAL_RANKING();
