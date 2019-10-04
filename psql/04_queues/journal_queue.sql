create or replace view PK.VIEW_QUEUE_JOURNALS as
select
    PUB || '/' || JOUR
    from PK.CT_JOURNAL_RANK
    where RANK < 6
    order by RANK;