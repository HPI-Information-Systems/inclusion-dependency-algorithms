package de.metanome.algorithms.sindd.database.metadata;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
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

      List<TableInfo> tables = createTables(configuration);

      List<Attribute> attributes = createAttributes(tables);
      CommonObjects.setAttributes(attributes);

      Map<String, Attribute> id2attMap = createId2attributeMpa(attributes);
      CommonObjects.setId2attributeMap(id2attMap);
    } catch (InputGenerationException e) {
      e.printStackTrace();
    } catch (AlgorithmConfigurationException e) {
      e.printStackTrace();
    }

  }

  private static Map<String, Attribute> createId2attributeMpa(List<Attribute> attributes) {
    Map<String, Attribute> id2attMap = new HashMap<String, Attribute>();
    for (Attribute att : attributes) {
      String id = att.getId();

      id2attMap.put(id, att);
    }

    return id2attMap;
  }

  private static List<TableInfo> createTables(final Configuration configuration)
      throws InputGenerationException, AlgorithmConfigurationException {
    final TableInfoFactory tableInfoFactory = new TableInfoFactory();
    List<TableInfo> tableInfos = tableInfoFactory
        .create(configuration.getRelationalInputGenerators(),
            configuration.getTableInputGenerators());
    List<TableInfo> tables = new ArrayList<TableInfo>();
    for (final TableInfo tableInfo : tableInfos) {
      tables.add(tableInfo);
    }
    return tables;
  }

  private static List<Attribute> createAttributes(List<TableInfo> tables) {
    List<Attribute> attributes = new ArrayList<Attribute>();

    for (final TableInfo table : tables) {
      for (String column : table.getColumnNames()) {
        Attribute att = new Attribute(column, table);
        attributes.add(att);
      }
    }
    return attributes;
  }
}
