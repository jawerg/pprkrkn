create table PK.LZ_JOURNAL_RANKING
(
    KUERZEL       char(10) primary key,
    RANK          integer,
    JOURNAL       text,
    PUBLISHER     text,
    FACTOR        float,
    ADJ_CITATIONS integer,
    N_ARTICLES    integer,
    N_CITATIONS   integer
);