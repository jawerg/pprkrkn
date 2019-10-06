create view PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS as
select distinct
    IDX.AID,
    trim(unnest(string_to_array(JINF.AUTHOR_SHORTIDS, ';'))) as AUT_SID,
    now()                                                    as TS_ENTRY
    from PK.LZ_PAP_INFO       as JINF
        inner join PK.IDX_AID as IDX
                       on JINF.AREF = IDX.AREF;

create table PK.CT_PAP_INFO_AUTHOR_SHORTIDS as table PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS with no data;
alter table PK.CT_PAP_INFO_AUTHOR_SHORTIDS
    add foreign key (AID) references PK.IDX_AID (AID);

call pk.gen_tracking_functions('CT_PAP_INFO_AUTHOR_SHORTIDS');

create view PK.VIEW_NEWBIES_PAP_INFO_AUTHOR_SHORTIDS as
select *
    from PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS
    where (AID, AUT_SID) in (
        select AID, AUT_SID
            from PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS
                except
        select AID, AUT_SID
            from PK.CT_PAP_INFO_AUTHOR_SHORTIDS );

create view PK.VIEW_OLDIES_PAP_INFO_AUTHOR_SHORTIDS as
select CT.*, now() as TS_ARCH
    from PK.CT_PAP_INFO_AUTHOR_SHORTIDS as CT
    where (AID, AUT_SID) in (
        select AID, AUT_SID
            from PK.CT_PAP_INFO_AUTHOR_SHORTIDS
            where AID in (
                select AID
                    from PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS )
                except
        select AID, AUT_SID
            from PK.VIEW_SHUTTLE_PAP_INFO_AUTHOR_SHORTIDS );

create table PK.ARCH_PAP_INFO_AUTHOR_SHORTIDS as table PK.VIEW_OLDIES_PAP_INFO_AUTHOR_SHORTIDS with no data;
alter table PK.CT_PAP_INFO_AUTHOR_SHORTIDS
    add foreign key (AID) references PK.IDX_AID (AID);

call PK.gen_tracking_functions('ARCH_PAP_INFO_AUTHOR_SHORTIDS');

create function PK.ETL_PAP_INFO_AUTHOR_SHORTIDS()
    returns trigger
as
$$
begin
    -- Insert newly gained information.
    insert into PK.CT_PAP_INFO_AUTHOR_SHORTIDS
    select *
        from PK.VIEW_NEWBIES_PAP_INFO_AUTHOR_SHORTIDS;

    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_PAP_INFO_AUTHOR_SHORTIDS
    select *
        from PK.VIEW_OLDIES_PAP_INFO_AUTHOR_SHORTIDS;

    -- delete newbies.
    delete
        from PK.CT_PAP_INFO_AUTHOR_SHORTIDS
        where (AID, AUT_SID) in (
            select AID, AUT_SID
                from PK.VIEW_OLDIES_PAP_INFO_AUTHOR_SHORTIDS
        );

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_1_ETL_PAP_INFO_AUTHOR_SHORTIDS
    after insert
    on PK.LZ_PAP_INFO
    for each statement
execute procedure PK.ETL_PAP_INFO_AUTHOR_SHORTIDS();
