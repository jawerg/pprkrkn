create view PK.VIEW_QUEUE_AREF as
select
    AREF
    from PK.IDX_AID;


select *
    from INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'pg_catalog';

select pg_relation_filepath('pk.ct_journal_rank');