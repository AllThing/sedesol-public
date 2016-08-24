CREATE INDEX CONCURRENTLY table_analytics_table_name_index ON raw.table_analytics(table_name);
CREATE INDEX CONCURRENTLY table_analytics_column_name_index ON raw.table_analytics(column_name);
