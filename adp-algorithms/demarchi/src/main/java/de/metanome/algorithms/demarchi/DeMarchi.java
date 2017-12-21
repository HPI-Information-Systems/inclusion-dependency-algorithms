package de.metanome.algorithms.demarchi;

import com.google.common.annotations.VisibleForTesting;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

class DeMarchi {

  private final TableInfoFactory tableInfoFactory;
  private Configuration configuration;
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
        .create(configuration.getRelationalInputGenerators(),
            configuration.getTableInputGenerators());
    attributeIndex = new Attribute[getTotalColumnCount(tables)];

    fillAttributeIndex(tables);
    final Map<String, IntSet> attributesByType = groupAttributesByType();
    for (final IntSet attributes : attributesByType.values()) {
      handleDomain(attributes);
    }
  }

  private void fillAttributeIndex(final Collection<TableInfo> tables) {
    int attributeId = 0;
    for (final TableInfo table : tables) {
      for (int index = 0; index < table.getColumnCount(); ++index) {
        attributeIndex[attributeId] = Attribute.builder()
            .id(attributeId)
            .tableName(table.getTableName())
            .columnOffset(index)
            .name(table.getColumnNames().get(index))
            .type(table.getColumnTypes().get(index))
            .generator(table.selectInputGenerator())
            .build();
        ++attributeId;
      }
    }
  }

  private Map<String, IntSet> groupAttributesByType() {
    final Map<String, IntSet> attributesByType = new HashMap<>();
    for (final Attribute attribute : attributeIndex) {
      attributesByType
          .computeIfAbsent(attribute.getType(), k -> new IntOpenHashSet())
          .add(attribute.getId());
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
      final Collection<String> values = getValues(attribute);

      if (values.isEmpty()) {
        handleEmptyAttribute(attribute, attributes);
      }

      for (final String value : values) {
        attributesByValue.computeIfAbsent(value, k -> new IntOpenHashSet()).add(attribute);
      }
    }
    return attributesByValue;
  }

  private void handleEmptyAttribute(final int attribute, final IntSet attributes)
      throws AlgorithmExecutionException {

    for (int other : attributes) {
      if (attribute != other) {
        receiveIND(attributeIndex[attribute], attributeIndex[other]);
      }
    }
  }

  private IntSet[] computeClosures(final Map<String, IntSet> attributesByValue) {
    final IntSet[] closures = new IntSet[attributeIndex.length];
    for (Map.Entry<String, IntSet> entry : attributesByValue.entrySet()) {
      for (int attribute : entry.getValue()) {
        if (closures[attribute] == null) {
          closures[attribute] = new IntOpenHashSet(entry.getValue());
        } else {
          closures[attribute].retainAll(entry.getValue());
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

    final InclusionDependency ind = InclusionDependencyBuilder
        .dependent().column(lhs.getTableName(), lhs.getName())
        .referenced().column(rhs.getTableName(), rhs.getName())
        .build();

    configuration.getResultReceiver().receiveResult(ind);
  }

  private Collection<String> getValues(final int attributeId)
      throws AlgorithmExecutionException {

    final Attribute attribute = attributeIndex[attributeId];
    final int offset = attribute.getColumnOffset();
    final SortedSet<String> values = new TreeSet<>();

    try (RelationalInput input = attribute.getGenerator().generateNewCopy()) {
      while (input.hasNext()) {
        final List<String> read = input.next();
        if (offset < read.size()) {
          final String v = read.get(offset);
          if (v != null) {
            values.add(v);
          }
        }
      }
      return values;
    } catch (final Exception e) {
      throw new InputGenerationException("reading attribute values", e);
    }
  }
}
