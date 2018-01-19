package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Spider {


  private final TableInfoFactory tableInfoFactory;
  private final ExternalRepository externalRepository;

  private SpiderConfiguration configuration;
  private Attribute[] attributeIndex;
  private PriorityQueue<Attribute> priorityQueue;


  public Spider() {
    tableInfoFactory = new TableInfoFactory();
    externalRepository = new ExternalRepository();
  }


  public void execute(final SpiderConfiguration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;
    final List<TableInfo> table = tableInfoFactory
        .create(configuration.getRelationalInputGenerators(),
            configuration.getTableInputGenerators());
    initializeAttributes(table);
    calculateInclusionDependencies();
    collectResults();
    shutdown();
  }

  private void initializeAttributes(final List<TableInfo> tables)
      throws AlgorithmExecutionException {

    final int columnCount = getTotalColumnCount(tables);
    attributeIndex = new Attribute[columnCount];
    priorityQueue = new ObjectHeapPriorityQueue<>(columnCount, this::compareAttributes);
    createAndEnqueueAttributes(tables);
    initializeRoles();
  }

  private void createAndEnqueueAttributes(final List<TableInfo> tables)
      throws AlgorithmExecutionException {

    int attributeId = 0;
    for (final TableInfo table : tables) {
      final Attribute[] attributes = getAttributes(table, attributeId);
      attributeId += attributes.length;

      for (final Attribute attribute : attributes) {
        attributeIndex[attribute.getId()] = attribute;
        if (attribute.getReadPointer().hasNext()) {
          // Has next value: always process.
          priorityQueue.enqueue(attribute);
        } else if (!configuration.isProcessEmptyColumns()) {
          // When ignoring empty columns, insert empty columns into queue.
          // Only during normal processing the dependent and referenced set of empty attributes
          // will be cleared.
          priorityQueue.enqueue(attribute);
        }
      }
    }
  }

  private Attribute[] getAttributes(final TableInfo table, int startIndex)
      throws AlgorithmExecutionException {

    final ReadPointer[] readPointers = externalRepository.uniqueAndSort(configuration, table);
    final Attribute[] attributes = new Attribute[table.getColumnCount()];
    for (int index = 0; index < readPointers.length; ++index) {
      attributes[index] =
          new Attribute(startIndex++, table.getTableName(), table.getColumnNames().get(index),
              readPointers[index]);
    }
    return attributes;
  }

  private void initializeRoles() {
    final IntSet allIds = allIds();
    for (final Attribute attribute : attributeIndex) {
      attribute.addDependent(allIds);
      attribute.removeDependent(attribute.getId());
      attribute.addReferenced(allIds);
      attribute.removeReferenced(attribute.getId());
    }
  }

  private IntSet allIds() {
    final IntSet ids = new IntOpenHashSet(attributeIndex.length);
    for (int index = 0; index < attributeIndex.length; ++index) {
      ids.add(index);
    }
    return ids;
  }

  private int getTotalColumnCount(final List<TableInfo> tables) {
    return tables.stream().mapToInt(TableInfo::getColumnCount).sum();
  }

  private int compareAttributes(final Attribute a1, final Attribute a2) {
    if (a1.getCurrentValue() == null && a2.getCurrentValue() == null) {
      return Integer.compare(a1.getId(), a2.getId());
    }

    if (a1.getCurrentValue() == null) {
      return 1;
    }

    if (a2.getCurrentValue() == null) {
      return -1;
    }

    final int order = a1.getCurrentValue().compareTo(a2.getCurrentValue());
    if (order == 0) {
      return Integer.compare(a1.getId(), a2.getId());
    }
    return order;
  }

  private void calculateInclusionDependencies() {
    final IntSet topAttributes = new IntOpenHashSet();
    while (!priorityQueue.isEmpty()) {

      final Attribute firstAttribute = priorityQueue.dequeue();
      topAttributes.add(firstAttribute.getId());
      while (!priorityQueue.isEmpty() && sameValue(priorityQueue.first(), firstAttribute)) {
        topAttributes.add(priorityQueue.dequeue().getId());
      }

      for (final int topAttribute : topAttributes) {
        attributeIndex[topAttribute].intersectReferenced(topAttributes, attributeIndex);
      }

      for (final int topAttribute : topAttributes) {
        final Attribute attribute = attributeIndex[topAttribute];
        attribute.nextValue();
        if (!attribute.isFinished()) {
          priorityQueue.enqueue(attribute);
        }
      }

      topAttributes.clear();
    }
  }

  private boolean sameValue(final Attribute a1, final Attribute a2) {
    return Objects.equals(a1.getCurrentValue(), a2.getCurrentValue());
  }

  private void collectResults() throws CouldNotReceiveResultException, ColumnNameMismatchException {
    for (final Attribute dep : attributeIndex) {

      if (dep.getReferenced().isEmpty()) {
        continue;
      }

      for (final int refId : dep.getReferenced()) {
        final Attribute ref = attributeIndex[refId];

        final InclusionDependency ind = InclusionDependencyBuilder
            .dependent().column(dep.getTableName(), dep.getColumnName())
            .referenced().column(ref.getTableName(), ref.getColumnName()).build();

        configuration.getResultReceiver().receiveResult(ind);
      }
    }
  }

  private void shutdown() throws AlgorithmExecutionException {
    try {
      for (final Attribute attribute : attributeIndex) {
        attribute.close();
      }
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("failed to close attribute", e);
    }
  }
}
