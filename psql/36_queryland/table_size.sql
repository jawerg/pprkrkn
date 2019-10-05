select
    TABLE_SCHEMA || '.' || TABLE_NAME                                                         as TABLE_FULL_NAME,
    pg_size_pretty(pg_total_relation_size('"' || TABLE_SCHEMA || '"."' || TABLE_NAME || '"')) as SIZE
    from INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = 'pk'
      and pg_total_relation_size('"' || TABLE_SCHEMA || '"."' || TABLE_NAME || '"') > 0
    order by pg_total_relation_size('"' || TABLE_SCHEMA || '"."' || TABLE_NAME || '"') desc;
