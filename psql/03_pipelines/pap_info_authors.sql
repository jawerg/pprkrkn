create view PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS as
select distinct
    IDX.AID,
    trim(unnest(string_to_array(JINF.AUTHORS, ';'))) as AUTHOR,
    now()                                            as TS_ENTRY
    from PK.LZ_PAP_INFO       as JINF
        inner join PK.IDX_AID as IDX
                       on JINF.AREF = IDX.AREF;

create table PK.CT_PAP_INFO_AUTHORS as table PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS with no data;
alter table PK.CT_PAP_INFO_AUTHORS
    add foreign key (AID) references PK.IDX_AID (AID);

call pk.gen_tracking_functions('CT_PAP_INFO_AUTHORS');

create view PK.VIEW_NEWBIES_PAP_INFO_AUTHORS as
select *
    from PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS
    where (AID, AUTHOR) in (
        select AID, AUTHOR
            from PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS
                except
        select AID, AUTHOR
            from PK.CT_PAP_INFO_AUTHORS );

create view PK.VIEW_OLDIES_PAP_INFO_AUTHORS as
select CT.*, now() as TS_ARCH
    from PK.CT_PAP_INFO_AUTHORS as CT
    where (AID, AUTHOR) in (
        select AID, AUTHOR
            from PK.CT_PAP_INFO_AUTHORS
            where AID in (
                select AID
                    from PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS )
                except
        select AID, AUTHOR
            from PK.VIEW_SHUTTLE_PAP_INFO_AUTHORS );

create table PK.ARCH_PAP_INFO_AUTHORS as table PK.VIEW_OLDIES_PAP_INFO_AUTHORS with no data;
alter table PK.CT_PAP_INFO_AUTHORS
    add foreign key (AID) references PK.IDX_AID (AID);

call pk.gen_tracking_functions('ARCH_PAP_INFO_AUTHORS');

create function PK.ETL_PAP_INFO_AUTHORS()
    returns trigger
as
$$
begin
    -- Insert newly gained information.
    insert into PK.CT_PAP_INFO_AUTHORS
    select *
        from PK.VIEW_NEWBIES_PAP_INFO_AUTHORS;

    -- Send entries that will be updated (newbies) to the archive.
    insert into PK.ARCH_PAP_INFO_AUTHORS
    select *
        from PK.VIEW_OLDIES_PAP_INFO_AUTHORS;

    -- delete newbies.
    delete
        from PK.CT_PAP_INFO_AUTHORS
        where (AID, AUTHOR) in (
            select AID, AUTHOR
                from PK.VIEW_OLDIES_PAP_INFO_AUTHORS
        );

    return null;
end;
$$
    language plpgsql;

create trigger TRIG_1_ETL_PAP_INFO_AUTHORS
    after insert
    on PK.LZ_PAP_INFO
    for each statement
execute procedure PK.ETL_PAP_INFO_AUTHORS();
