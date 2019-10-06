create table PK.TRIGGERED_TAB_MONITOR
(
    TABLE_NAME text,
    OP         text,
    NROWS      integer,
    TS_OP      timestamp default current_timestamp
);

create or replace procedure PK.gen_tracking_functions(TABNAME text)
    language plpgsql as
$$
declare
    T            text := trim(lower(TABNAME));
    FUN_CREATOR  text := '' ||
                         'create or replace function PK.track_' || T || '() '
                             || 'returns trigger language plpgsql as $fun$ '
                             || 'begin '
                             || 'insert into PK.TRIGGERED_TAB_MONITOR '
                             || 'select TG_TABLE_NAME, lower(TG_OP), count(*), now() from pk.' || T || '; '
        || 'return null; end; $fun$ ';
    TRIG_CREATOR text := '' ||
                         'create trigger trig_track_' || T || ' '
                             || 'after insert or delete or update or truncate on PK.' || T || ' '
                             || 'for each statement execute function PK.track_' || T || '();';
begin
    execute FUN_CREATOR;
    execute TRIG_CREATOR;
end ;
$$;

