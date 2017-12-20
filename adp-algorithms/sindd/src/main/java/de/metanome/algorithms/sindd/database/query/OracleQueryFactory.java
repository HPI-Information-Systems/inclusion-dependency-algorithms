package de.metanome.algorithms.sindd.database.query;

public class OracleQueryFactory implements QueryFactory {

	private static String FIND_TABLES_BY_SCHEMA = "select table_name from user_tables";

	private static String FIND_TABLES_AND_COLUMNS_By_SCHEMA = "select table_name, column_name from user_tab_columns";

	private static String FIND_COLUMNS_BY_TABLE_AND_SCHEMA = "select column_name, data_type from user_tab_columns where table_name = ':table'";

	@Override
	public String createQueryToFindTablesBySchema(String schema) {
		return FIND_TABLES_BY_SCHEMA;
	}

	@Override
	public String createQueryToFindColumnsByTableAndSchema(String table, String schema) {
		return FIND_COLUMNS_BY_TABLE_AND_SCHEMA.replace(":table", table);
	}

	@Override
	public String createQueryToFindTablesAndColumnsBySchema(String schema) {
		return FIND_TABLES_AND_COLUMNS_By_SCHEMA;
	}

	@Override
	public String createQueryToExportColumnValues(String column, String table) {
		StringBuffer sql = new StringBuffer();
		sql.append("select distinct t." + column + " from " + table + " t where t." + column
				+ " is not null order by nlssort(t." + column + " , 'nls_sort=binary')");
		return sql.toString();
	}

}
