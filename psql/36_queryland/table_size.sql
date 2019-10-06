with
    TABLES as (
        select TABLE_SCHEMA || '.' || TABLE_NAME as TABNAME
            from INFORMATION_SCHEMA.TABLES
            where TABLE_SCHEMA = 'pk'
              and TABLE_TYPE = 'BASE TABLE' -- views do not have a reasonable size...
    )
select
    TABNAME,
    pg_size_pretty(pg_table_size(TABNAME))          as TABLE_SIZE,
    pg_size_pretty(pg_indexes_size(TABNAME))        as INDEX_SIZE,
    pg_size_pretty(pg_total_relation_size(TABNAME)) as TOTAL_SIZE
    from TABLES
    order by pg_total_relation_size(TABNAME) desc;