package de.metanome.algorithms.sindd.database.query;

/**
 * 
 * @author Nuhad.Shaabani
 *
 */
public interface QueryFactory {

	String createQueryToFindTablesBySchema(String schema);

	String createQueryToFindColumnsByTableAndSchema(String table, String schema);

	String createQueryToFindTablesAndColumnsBySchema(String schema);

	String createQueryToExportColumnValues(String column, String table);

}
