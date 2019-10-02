create or replace view PK.VIEW_QUEUE_AREF as
select
    AID.AREF
    from PK.IDX_AID                      as AID
        inner join PK.CT_JOURNAL_RANKING as JOUR
                       on substr(AID.AREF, 4, 10) = JOUR.KUERZEL
    order by JOUR.RANK,
             AID.AREF desc;