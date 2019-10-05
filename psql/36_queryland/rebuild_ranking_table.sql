select distinct IDX.PUB, IDX.JOUR, IDX.JOURNAL, RANK.RANK, ART.N_ARTICLES, FAC.FACTOR
    from PK.IDX_JID                      as IDX
        left join PK.CT_JOURNAL_RANK     as RANK
                      on (IDX.PUB, IDX.JOUR) = (RANK.PUB, RANK.JOUR)
        left join PK.CT_JOURNAL_ARTICLES as ART
                      on (IDX.PUB, IDX.JOUR) = (ART.PUB, ART.JOUR)
        left join PK.CT_JOURNAL_FACTOR   as FAC
                      on (IDX.PUB, IDX.JOUR) = (FAC.PUB, FAC.JOUR)
    order by RANK.RANK;
