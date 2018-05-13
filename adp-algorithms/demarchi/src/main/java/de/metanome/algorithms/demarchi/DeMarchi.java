package de.metanome.algorithms.demarchi;

import com.google.common.annotations.VisibleForTesting;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.AttributeHelper;
import de.metanome.util.BitSetIterator;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeMarchi {

  private final TableInfoFactory tableInfoFactory;
  private final AttributeHelper attributeHelper;

  private Configuration configuration;
  private Attribute[] attributeIndex;

  public DeMarchi() {
    tableInfoFactory = new TableInfoFactory();
    attributeHelper = new AttributeHelper();
  }

  @VisibleForTesting
  DeMarchi(final TableInfoFactory tableInfoFactory) {
    this.tableInfoFactory = tableInfoFactory;
    attributeHelper = new AttributeHelper();
  }

  public void execute(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;
    final List<TableInfo> tables = tableInfoFactory
        .create(configuration.getRelationalInputGenerators(),
            configuration.getTableInputGenerators());
    attributeIndex = new Attribute[getTotalColumnCount(tables)];

    fillAttributeIndex(tables);
    final Map<String, BitSet> attributesByType = groupAttributesByType();
    for (final BitSet attributes : attributesByType.values()) {
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
            .name(table.getColumnNames().get(index))
            .type(table.getColumnTypes().get(index))
            .relationalInputGenerator(table.getRelationalInputGenerator())
            .tableInputGenerator(table.getTableInputGenerator())
            .build();
        ++attributeId;
      }
    }
  }

  private Map<String, BitSet> groupAttributesByType() {
    final Map<String, BitSet> attributesByType = new HashMap<>();
    for (final Attribute attribute : attributeIndex) {
      attributesByType
          .computeIfAbsent(attribute.getType(), k -> new BitSet(attributeIndex.length))
          .set(attribute.getId());
    }
    return attributesByType;
  }

  private int getTotalColumnCount(final Collection<TableInfo> info) {
    return info.stream().mapToInt(TableInfo::getColumnCount).sum();
  }

  private void handleDomain(final BitSet attributes) throws AlgorithmExecutionException {
    final Map<String, BitSet> attributesByValue = groupAttributesByValue(attributes);
    final BitSet[] closures = computeClosures(attributesByValue);
    computeInclusionDependencies(closures);
  }

  private Map<String, BitSet> groupAttributesByValue(final BitSet attributes)
      throws AlgorithmExecutionException {

    final Map<String, BitSet> attributesByValue = new HashMap<>();
    final BitSet emptyAttributes = new BitSet(attributeIndex.length);

    final BitSetIterator iterator = BitSetIterator.of(attributes);
    while (iterator.hasNext()) {
      addValues(iterator.next(), attributesByValue, emptyAttributes);
    }

    if (configuration.isProcessEmptyColumns()) {
      final BitSetIterator empty = BitSetIterator.of(emptyAttributes);
      while (empty.hasNext()) {
        handleEmptyAttribute(empty.next(), attributes);
      }
    }

    return attributesByValue;
  }

  private void handleEmptyAttribute(final int attribute, final BitSet attributes)
      throws AlgorithmExecutionException {

    final BitSetIterator iterator = BitSetIterator.of(attributes);
    while (iterator.hasNext()) {
      final int other = iterator.next();
      if (attribute != other) {
        receiveIND(attributeIndex[attribute], attributeIndex[other]);
      }
    }
  }

  private BitSet[] computeClosures(final Map<String, BitSet> attributesByValue) {
    final BitSet[] closures = new BitSet[attributeIndex.length];

    for (Map.Entry<String, BitSet> entry : attributesByValue.entrySet()) {
      final BitSetIterator iterator = BitSetIterator.of(entry.getValue());
      while (iterator.hasNext()) {
        final int attribute = iterator.next();
        if (closures[attribute] == null) {
          final BitSet set = new BitSet(attributeIndex.length);
          set.or(entry.getValue());
          closures[attribute] = set;
        } else {
          closures[attribute].and(entry.getValue());
        }
      }
    }
    return closures;
  }

  private void computeInclusionDependencies(final BitSet[] closures)
      throws AlgorithmExecutionException {

    for (final Attribute attribute : attributeIndex) {
      final BitSet closure = closures[attribute.getId()];
      if (closure == null) {
        continue;
      }
      final BitSetIterator iterator = BitSetIterator.of(closure);
      while (iterator.hasNext()) {
        final int rhs = iterator.next();
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

  private void addValues(final int attributeId,
      final Map<String, BitSet> attributesByValue,
      final BitSet emptyAttributes) throws AlgorithmExecutionException {

    final Attribute attribute = attributeIndex[attributeId];
    final boolean hasValue = attributeHelper.getValues(attribute.selectInputGenerator(),
        attribute.getTableName(), attribute.getName(), configuration.getInputRowLimit(), value -> {
          attributesByValue
              .computeIfAbsent(value, v -> new BitSet(attributeIndex.length))
              .set(attributeId);
        });

    if (!hasValue) {
      emptyAttributes.set(attributeId);
    }
  }
}
