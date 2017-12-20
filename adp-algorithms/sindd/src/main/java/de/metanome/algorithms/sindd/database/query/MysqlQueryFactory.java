package de.metanome.algorithms.sindd.database.query;
/**
 * 
 * @author Nuhad.Shaabani
 *
 */
public class MysqlQueryFactory implements QueryFactory {

	private static String FIND_TABLES_BY_SCHEMA = "select distinct table_name from information_schema.columns where table_schema = ':schema' ";

	private static String FIND_TABLES_AND_COLUMNS_By_SCHEMA = "select table_name, column_name from information_schema.columns where table_schema = ':schema' ";

	private static String FIND_COLUMNS_BY_TABLE_AND_SCHEMA = "select column_name, column_type from information_schema.columns where table_name = ':table' and table_schema = ':schema' ";

	@Override
	public String createQueryToFindTablesBySchema(String schema) {
		return FIND_TABLES_BY_SCHEMA.replace(":schema", schema);
	}

	@Override
	public String createQueryToFindColumnsByTableAndSchema(String table, String schema) {
		return FIND_COLUMNS_BY_TABLE_AND_SCHEMA.replace(":table", table).replace(":schema", schema);
	}

	public String createQueryToFindTablesAndColumnsBySchema(String schema) {
		return FIND_TABLES_AND_COLUMNS_By_SCHEMA.replace(":schema", schema);
	}

	@Override
	public String createQueryToExportColumnValues(String column, String table) {
		StringBuffer sql = new StringBuffer();
		sql.append("select distinct t." + column + " from " + table + " t where t." + column
				+ " is not null order by cast(t." + column + " as binary)");
		return sql.toString();
	}
}
