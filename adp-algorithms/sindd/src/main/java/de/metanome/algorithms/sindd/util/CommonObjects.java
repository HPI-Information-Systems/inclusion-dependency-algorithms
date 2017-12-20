package de.metanome.algorithms.sindd.util;

import de.metanome.algorithms.sindd.database.DBConn;
import de.metanome.algorithms.sindd.database.metadata.Attribute;
import de.metanome.algorithms.sindd.database.metadata.Schema;
import de.metanome.algorithms.sindd.database.metadata.Table;
import de.metanome.algorithms.sindd.database.query.QueryFactory;
import de.metanome.algorithms.sindd.sindd.Partition;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Nuhad.Shaabani
 *
 */
public class CommonObjects {

	private static DBConn dbConn;
	private static QueryFactory queryFactory;
	private static List<Schema> schemas;
	private static List<Table> tables;
	private static List<Attribute> attributes;
	private static Map<String, Attribute> id2attributeMap;
	private static List<Partition> partitions;
	private static Performance performance;

	public static void setDBConnection(DBConn dbConn) {
		CommonObjects.dbConn = dbConn;
	}

	public static DBConn getDBConnection() {
		return dbConn;
	}

	public static void setQueryFactory(QueryFactory queryFactory) {
		CommonObjects.queryFactory = queryFactory;
	}

	public static QueryFactory getQueryFactory() {
		return queryFactory;
	}

	public static void setSchemas(List<Schema> schemas) {
		CommonObjects.schemas = schemas;
	}

	public static List<Schema> getSchemas() {
		return schemas;
	}

	public static void setTables(List<Table> tables) {
		CommonObjects.tables = tables;
	}

	public static List<Table> getTables() {
		return tables;
	}

	public static void setAttributes(List<Attribute> attributes) {
		CommonObjects.attributes = attributes;
	}

	public static List<Attribute> getAttributes() {
		return attributes;
	}

	public static void setId2attributeMap(Map<String, Attribute> id2attributeMap) {
		CommonObjects.id2attributeMap = id2attributeMap;
	}

	public static Map<String, Attribute> getId2attributeMap() {
		return id2attributeMap;
	}

	public static void setPartitions(List<Partition> partitions) {
		CommonObjects.partitions = partitions;
	}

	public static List<Partition> getPartitions() {
		return partitions;
	}

	public static void setPerformance(Performance performance) {
		CommonObjects.performance = performance;
	}

	public static Performance getPerformnce() {
		return performance;
	}
}
