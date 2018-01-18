package de.metanome.algorithms.sindd.database.metadata;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithms.sindd.Configuration;
import de.metanome.algorithms.sindd.util.CommonObjects;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataBuilder {

  public static void build(final Configuration configuration) {

    try {
      final List<TableInfo> tables = createTables(configuration);
      CommonObjects.setTables(tables);

      final List<Attribute> attributes = createAttributes(tables);
      CommonObjects.setAttributes(attributes);

      final Map<String, Attribute> id2attMap = createId2attributeMap(attributes);
      CommonObjects.setId2attributeMap(id2attMap);
    } catch (final AlgorithmExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, Attribute> createId2attributeMap(final List<Attribute> attributes) {
    final Map<String, Attribute> id2attMap = new HashMap<String, Attribute>();
    for (final Attribute att : attributes) {
      id2attMap.put(att.getId(), att);
    }
    return id2attMap;
  }

  private static List<TableInfo> createTables(final Configuration configuration)
      throws AlgorithmExecutionException {

    final TableInfoFactory tableInfoFactory = new TableInfoFactory();
    return tableInfoFactory
        .create(configuration.getRelationalInputGenerators(),
            configuration.getTableInputGenerators());
  }

  private static List<Attribute> createAttributes(final List<TableInfo> tables) {
    final List<Attribute> attributes = new ArrayList<>();
    for (final TableInfo table : tables) {
      attributes.addAll(createAttributes(table));
    }
    return attributes;
  }

  private static List<Attribute> createAttributes(final TableInfo table) {
    final List<Attribute> attributes = new ArrayList<>(table.getColumnCount());
    for (final String column : table.getColumnNames()) {
      attributes.add(new Attribute(column, table));
    }
    CommonObjects.addTableToAttribute(table, attributes);
    return attributes;
  }
}
