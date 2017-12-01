package de.metanome.algorithms.demarchi;

import com.google.common.annotations.VisibleForTesting;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

class DeMarchi {

  private Configuration configuration;
  private final TableInfoFactory tableInfoFactory;
  private Attribute[] attributeIndex;

  DeMarchi() {
    tableInfoFactory = new TableInfoFactory();
  }

  @VisibleForTesting
  DeMarchi(final TableInfoFactory tableInfoFactory) {
    this.tableInfoFactory = tableInfoFactory;
  }

  void execute(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;
    final List<TableInfo> tables = tableInfoFactory
        .createFromTableInputs(configuration.getTableInputGenerators());
    attributeIndex = new Attribute[getTotalColumnCount(tables)];

    final Map<String, IntSet> attributesByType = groupAttributesByType(tables);
    for (final IntSet attributes : attributesByType.values()) {
      handleDomain(attributes);
    }
  }

  private Map<String, IntSet> groupAttributesByType(final Collection<TableInfo> tables) {
    final Map<String, IntSet> attributesByType = new HashMap<>();
    int attributeId = 0;
    for (final TableInfo table : tables) {
      for (int index = 0; index < table.getColumnCount(); ++index) {
        final String name = table.getColumnNames().get(index);
        final String type = table.getColumnTypes().get(index);
        final Attribute attribute = new Attribute(attributeId, table.getTableName(), name,
            table.getTableInputGenerator());
        attributeIndex[attributeId] = attribute;
        attributesByType.computeIfAbsent(type, k -> new IntOpenHashSet()).add(attributeId);
        ++attributeId;
      }
    }
    return attributesByType;
  }

  private int getTotalColumnCount(final Collection<TableInfo> info) {
    return info.stream().mapToInt(TableInfo::getColumnCount).sum();
  }

  private void handleDomain(final IntSet attributes) throws AlgorithmExecutionException {
    final Map<String, IntSet> attributesByValue = groupAttributesByValue(attributes);
    final IntSet[] closures = computeClosures(attributesByValue);
    computeInclusionDependencies(closures);
  }

  private Map<String, IntSet> groupAttributesByValue(final IntSet attributes)
      throws AlgorithmExecutionException {

    final Map<String, IntSet> attributesByValue = new HashMap<>();
    for (final int attribute : attributes) {
      for (final String value : getValues(attribute)) {
        attributesByValue.computeIfAbsent(value, k -> new IntOpenHashSet()).add(attribute);
      }
    }
    return attributesByValue;
  }

  private IntSet[] computeClosures(final Map<String, IntSet> attributesByValue) {
    final IntSet[] closures = new IntSet[attributeIndex.length];
    for (Map.Entry<String, IntSet> entry : attributesByValue.entrySet()) {
      for (int index = 0; index < attributeIndex.length; ++index) {
        if (entry.getValue().contains(index)) {
          if (closures[index] == null) {
            closures[index] = entry.getValue();
          } else {
            closures[index].retainAll(entry.getValue());
          }
        }
      }
    }
    return closures;
  }

  private void computeInclusionDependencies(final IntSet[] closures)
      throws AlgorithmExecutionException {

    for (final Attribute attribute : attributeIndex) {
      final IntSet closure = closures[attribute.getId()];
      if (closure == null) {
        continue;
      }
      for (final int rhs : closure) {
        if (attribute.getId() != rhs) {
          receiveIND(attribute, attributeIndex[rhs]);
        }
      }
    }
  }

  private void receiveIND(final Attribute lhs, final Attribute rhs)
      throws AlgorithmExecutionException {

    final ColumnIdentifier leftColumn = new ColumnIdentifier(lhs.getTableName(), lhs.getName());
    final ColumnIdentifier rightColumn = new ColumnIdentifier(rhs.getTableName(), rhs.getName());
    final InclusionDependency ind = new InclusionDependency(new ColumnPermutation(leftColumn),
        new ColumnPermutation(rightColumn));
    configuration.getResultReceiver().receiveResult(ind);
  }

  private Collection<String> getValues(final int attributeId)
      throws AlgorithmExecutionException {

    final Attribute attribute = attributeIndex[attributeId];
    final TableInputGenerator generator = attribute.getGenerator();
    try (ResultSet set = generator.sortBy(attribute.getName(), false)) {
      final SortedSet<String> values = new TreeSet<>();
      while (set.next()) {
        final String value = set.getString(attribute.getName());
        if (value != null) {
          values.add(value);
        }
      }
      return values;
    } catch (final SQLException e) {
      throw new InputGenerationException("reading attribute values", e);
    }
  }

}
