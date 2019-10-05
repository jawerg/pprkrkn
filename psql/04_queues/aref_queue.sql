create or replace view PK.VIEW_QUEUE_AREF as
select
    AREF
    from PK.IDX_AID
    order by AID
    limit 10;

