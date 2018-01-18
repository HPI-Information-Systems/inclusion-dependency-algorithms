package de.metanome.algorithms.sindd.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import de.metanome.algorithms.sindd.database.metadata.Attribute;
import de.metanome.algorithms.sindd.sindd.Partition;
import de.metanome.util.TableInfo;
import java.util.List;
import java.util.Map;

public class CommonObjects {

  private static List<TableInfo> tables;
  private static ListMultimap<TableInfo, Attribute> tableToAttribute;
  private static List<Attribute> attributes;
  private static Map<String, Attribute> id2attributeMap;
  private static List<Partition> partitions;
  private static Performance performance;

  public static List<TableInfo> getTables() {
    return tables;
  }

  public static void setTables(final List<TableInfo> tables) {
    CommonObjects.tables = tables;
  }


  public static ListMultimap<TableInfo, Attribute> getTableToAttribute() {
    return tableToAttribute;
  }

  public static void addTableToAttribute(final TableInfo tableInfo,
      final List<Attribute> attributes) {
    if (tableToAttribute == null) {
      tableToAttribute = ArrayListMultimap.create();
    }
    tableToAttribute.putAll(tableInfo, attributes);
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

  public static Performance getPerformance() {
    return performance;
  }
}
