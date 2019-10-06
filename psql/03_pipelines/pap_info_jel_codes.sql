create view PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES as
select distinct
    IDX.AID,
    trim(unnest(string_to_array(JINF.JEL_CODES, ';'))) as JEL_CODE,
    now()                                              as TS_ENTRY
    from PK.LZ_PAP_INFO       as JINF
        inner join PK.IDX_AID as IDX
                       on JINF.AREF = IDX.AREF;

create table PK.CT_PAP_INFO_JEL_CODES as table PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES with no data;
alter table PK.CT_PAP_INFO_JEL_CODES
    add foreign key (AID) references PK.IDX_AID (AID);

call PK.gen_tracking_functions('CT_PAP_INFO_JEL_CODES');

create view PK.VIEW_NEWBIES_PAP_INFO_JEL_CODES as
select *
    from PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES
    where (AID, JEL_CODE) in (
        select AID, JEL_CODE
            from PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES
                except
        select AID, JEL_CODE
            from PK.CT_PAP_INFO_JEL_CODES );

create view PK.VIEW_OLDIES_PAP_INFO_JEL_CODES as
select CT.*, now() as TS_ARCH
    from PK.CT_PAP_INFO_JEL_CODES as CT
    where (AID, JEL_CODE) in (
        select AID, JEL_CODE
            from PK.CT_PAP_INFO_JEL_CODES
            where AID in (
                select AID
                    from PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES )
                except
        select AID, JEL_CODE
            from PK.VIEW_SHUTTLE_PAP_INFO_JEL_CODES );

create table PK.ARCH_PAP_INFO_JEL_CODES as table PK.VIEW_OLDIES_PAP_INFO_JEL_CODES with no data;
alter table PK.CT_PAP_INFO_JEL_CODES
    add foreign key (AID) references PK.IDX_AID (AID);

call PK.gen_tracking_functions('ARCH_PAP_INFO_JEL_CODES');

create function PK.ETL_PAP_INFO_JEL_CODES()
    returns trigger
as
$$
begin
    -- Insert newly gained information.
    insert into PK.CT_PAP_INFO_JEL_CODES
    select *
        from PK.VIEW_NEWBIES_PAP_INFO_JEL_CODES;

    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_PAP_INFO_JEL_CODES
    select *
        from PK.VIEW_OLDIES_PAP_INFO_JEL_CODES;

    -- delete newbies.
    delete
        from PK.CT_PAP_INFO_JEL_CODES
        where (AID, JEL_CODE) in (
            select AID, JEL_CODE
                from PK.VIEW_OLDIES_PAP_INFO_JEL_CODES
        );

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_1_ETL_PAP_INFO_JEL_CODES
    after insert
    on PK.LZ_PAP_INFO
    for each statement
execute procedure PK.ETL_PAP_INFO_JEL_CODES();
