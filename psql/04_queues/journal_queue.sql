create or replace view PK.VIEW_QUEUE_JOURNALS as
select
    PUB || '/' || JOUR
    from PK.CT_JOURNAL_RANK
    where RANK < 71
    order by RANK;