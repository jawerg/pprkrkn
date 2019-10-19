create view PK.VIEW_HASH_PAP_INFO_CORE as
select
    IDX.AID,
    md5(''
            || P.DATE || P.HANDLE || P.TITLE
            || P.ABSTRACT || P.YEAR || P.VOLUME
            || P.ISSUE || P.FIRSTPAGE || P.LASTPAGE
        )::uuid                                                         as HASHVAL,
    DATE,
    P.HANDLE,
    P.TITLE,
    nullif(trim(P.ABSTRACT), 'No abstract is available for this item.') as ABSTRACT,
    P.YEAR,
    P.VOLUME,
    P.ISSUE,
    P.FIRSTPAGE,
    P.LASTPAGE,
    NOW()                                                               as TS_ENTRY
    from PK.LZ_PAP_INFO       as P
        inner join PK.IDX_AID as IDX
                       on (substr(P.AREF, 4, 3), substr(P.AREF, 8, 6), substr(P.AREF, 15))
                           = (IDX.PUB, IDX.JOUR, IDX.ART);

create table PK.CT_PAP_INFO_CORE as table PK.VIEW_HASH_PAP_INFO_CORE with no data;
alter table PK.CT_PAP_INFO_CORE
    add foreign key (AID) references PK.IDX_AID (AID);

call PK.gen_tracking_functions('CT_PAP_INFO_CORE');

create view PK.VIEW_NEWBIES_PAP_INFO_CORE as
select *
    from PK.VIEW_HASH_PAP_INFO_CORE
    where not (AID, HASHVAL) in (
        select
            AID,
            HASHVAL
            from PK.CT_PAP_INFO_CORE
    );

create view PK.VIEW_OLDIES_PAP_INFO_CORE as
select
    CT.*,
    NEWB.TS_ENTRY as TS_ARCH
    from PK.CT_PAP_INFO_CORE                     as CT
        inner join PK.VIEW_NEWBIES_PAP_INFO_CORE as NEWB
                       on CT.AID = NEWB.AID
;

create table PK.ARCH_PAP_INFO_CORE as table PK.VIEW_OLDIES_PAP_INFO_CORE with no data;
alter table PK.ARCH_PAP_INFO_CORE
    add foreign key (AID) references PK.IDX_AID (AID);

call PK.gen_tracking_functions('ARCH_PAP_INFO_CORE');

create function PK.ETL_PAP_INFO_CORE()
    returns trigger
as
$$
begin
    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_PAP_INFO_CORE
    select *
        from PK.VIEW_OLDIES_PAP_INFO_CORE;

    -- delete newbies.
    delete
        from PK.CT_PAP_INFO_CORE
        where AID in (
            select
                AID
                from PK.VIEW_OLDIES_PAP_INFO_CORE
        );

    -- insert newbies.
    insert into PK.CT_PAP_INFO_CORE
    select *
        from PK.VIEW_NEWBIES_PAP_INFO_CORE;

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_1_ETL_PAP_INFO_CORE
    after insert
    on PK.LZ_PAP_INFO
    for each statement
execute procedure PK.ETL_PAP_INFO_CORE();