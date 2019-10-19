-- analog notation to LZ_REFERENCES as those are closely connected.
-- However, citations are the inverse of a reference.
create table PK.LZ_CITATIONS
(
    REFERENT text,
    REFERRER text
);