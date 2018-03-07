package de.metanome.util;

import static java.util.Arrays.asList;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class TableInfoFactory {

  private static final String STRING_COLUMN_TYE = "String";

  public List<TableInfo> create(
      final Collection<RelationalInputGenerator> relationalInputGenerators,
      final Collection<TableInputGenerator> tableInputGenerators)
      throws InputGenerationException, AlgorithmConfigurationException {

    // FIXME Defensive programming: given that an algorithm only implements RelationalInput, TableInput may come in disguise
    final List<RelationalInputGenerator> relational = new ArrayList<>();
    final List<TableInputGenerator> table = new ArrayList<>(tableInputGenerators);

    for (final RelationalInputGenerator generator : relationalInputGenerators) {
      if (generator instanceof TableInputGenerator) {
        table.add(((TableInputGenerator) generator));
      } else {
        relational.add(generator);
      }
    }

    final List<TableInfo> tables = new ArrayList<>();
    tables.addAll(createFromRelationalInputs(relational));
    tables.addAll(createFromTableInputs(table));
    return tables;
  }

  public List<TableInfo> createFromRelationalInputs(
      final Collection<RelationalInputGenerator> generators)
      throws InputGenerationException {

    final List<TableInfo> result = new ArrayList<>();
    if (generators != null) {
      for (final RelationalInputGenerator generator : generators) {
        try (RelationalInput input = generator.generateNewCopy()) {
          result.add(createFrom(generator, input));
        } catch (final Exception e) {
          throw new InputGenerationException("relational input", e);
        }
      }
    }
    return result;
  }

  private TableInfo createFrom(final RelationalInputGenerator generator,
      final RelationalInput input) {

    return TableInfo.builder()
        .relationalInputGenerator(generator)
        .tableName(input.relationName())
        .columnNames(input.columnNames())
        .columnTypes(getFixedColumnTypes(input))
        .build();
  }

  private List<String> getFixedColumnTypes(final RelationalInput input) {
    final String[] types = new String[input.numberOfColumns()];
    Arrays.setAll(types, index -> STRING_COLUMN_TYE);
    return asList(types);
  }

  public List<TableInfo> createFromTableInputs(
      final Collection<TableInputGenerator> generators)
      throws InputGenerationException, AlgorithmConfigurationException {

    final List<TableInfo> result = new ArrayList<>();
    if (generators != null) {
      for (final TableInputGenerator generator : generators) {
        result.add(createFrom(generator));
      }
    }
    return result;
  }

  private TableInfo createFrom(final TableInputGenerator generator)
      throws AlgorithmConfigurationException, InputGenerationException {

    try (ResultSet set = generator.select()) {
      final ResultSetMetaData metadata = set.getMetaData();
      final List<String> columnNames = new ArrayList<>();
      final List<String> columnTypes = new ArrayList<>();
      for (int index = 1; index <= metadata.getColumnCount(); ++index) {
        columnNames.add(metadata.getColumnName(index));
        columnTypes.add(metadata.getColumnTypeName(index));
      }

      return TableInfo.builder()
          .tableInputGenerator(generator)
          .tableName(metadata.getTableName(1))
          .columnNames(columnNames)
          .columnTypes(columnTypes)
          .build();
    } catch (final SQLException e) {
      throw new InputGenerationException("database error while reading metadata", e);
    } finally {
      try {
        // FIXME iterating over all input tables, creating one connection per table exceeds connection limits
        // hopefully the underlying database generator re-establishes the connection
        generator.close();
      } catch (final Exception e) {
        throw new InputGenerationException("terrible", e);
      }
    }
  }
}