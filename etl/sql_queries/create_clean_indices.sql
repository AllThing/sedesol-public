select concat('CREATE INDEX CONCURRENTLY IF NOT EXISTS ',table_name,'_',column_name,'_ind ON clean.',table_name,'(',column_name,');') as indexes from information_schema.columns where table_name in 
( SELECT table_name FROM information_schema.tables WHERE table_schema='clean' AND table_type='BASE TABLE' AND table_name <> 'pub') and table_schema = 'clean';
