drop table PK.LZ_AREF_INBOX;
create table PK.LZ_AREF_INBOX
(
    AREF      text primary key,
    TIMESTAMP timestamp default current_timestamp
);

