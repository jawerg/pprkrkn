create or replace view PK.VIEW_QUEUE_JOURNALS as
select
    KUERZEL
    from PK.CT_JOURNAL_RANKING
    where RANK < 6
    order by RANK;