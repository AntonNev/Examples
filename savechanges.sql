create or replace procedure cdc.read_wal() as $$
declare 
	end_lsn pg_lsn = pg_current_wal_lsn();
	slot_name name = (select value from cdc.params where name = 'slot_name');
begin 
	with read_wal as
	(
		select  
			data::jsonb->>'table' as table,
			case 
				when data::jsonb->>'action' = 'I'
					then (
						select hstore(array_agg(j->>'name'), array_agg(j->>'value'))
						from jsonb_array_elements(data::jsonb->'columns') j 
						where j->>'name' = any(ti.p_keys)
					) 
				else (
					select hstore(array_agg(j->>'name'), array_agg(j->>'value'))
					from jsonb_array_elements(data::jsonb->'identity') j 
				) 
			end as p_keys
		from pg_logical_slot_peek_changes(
			slot_name, 
			end_lsn, 
			NULL, 
			'format-version', 
			'2',
			'add-tables',
			(select string_agg(schemaname || '.' || tablename, ', ') from cdc.table_info))
		inner join cdc.table_info ti on ti.tablename = data::jsonb->>'table'
		where data::jsonb->>'action' in ('I', 'D', 'U') 
	)
	insert into cdc.change_tracking(
		tablename, 
		p_keys)
	select 
		read_wal.table, 
		array_agg(distinct read_wal.p_keys) 
	from read_wal
	group by read_wal.table;
	
	perform pg_replication_slot_advance(slot_name, end_lsn);
end;
$$ language plpgsql;