package de.metanome.algorithms.spiderbruteforce;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.TPMMS;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class SpiderBruteForce {

  private final TableInfoFactory tableInfoFactory;

  private int attributeCount;
  private Attribute[] attributes;
  private Path[] attributeValues;
  private Configuration configuration;

  SpiderBruteForce() {
    tableInfoFactory = new TableInfoFactory();
  }

  void execute(final Configuration configuration) throws AlgorithmExecutionException {
    this.configuration = configuration;

    final Collection<TableInfo> tables = tableInfoFactory
        .create(configuration.getRelationalInputGenerators(), Collections.emptyList());

    attributeCount = tables.stream().mapToInt(TableInfo::getColumnCount).sum();
    initializeAttributes(tables);

    try (FileGenerator ignored = configuration.getTempFileGenerator()) {
      export(tables);
      computeInclusionDependencies();
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("I/O error", e);
    }
  }

  private void initializeAttributes(final Collection<TableInfo> tables) {
    attributes = new Attribute[attributeCount];
    int attributeId = 0;
    for (final TableInfo table : tables) {
      for (final String column : table.getColumnNames()) {
        attributes[attributeId] = Attribute.builder()
            .id(attributeId)
            .table(table.getTableName())
            .column(column)
            .input(table.selectInputGenerator())
            .build();
        ++attributeId;
      }
    }
  }

  private void export(final Collection<TableInfo> tables)
      throws AlgorithmExecutionException, IOException {

    attributeValues = new Path[attributeCount];
    int startIndex = 0;
    for (final TableInfo table : tables) {
      export(table, startIndex);
      startIndex += table.getColumnCount();
    }
    sortAndDistinct();
  }

  private void export(final TableInfo table, final int startIndex)
      throws AlgorithmExecutionException {
    try {
      final BufferedWriter[] writers = new BufferedWriter[table.getColumnCount()];
      for (int index = 0; index < table.getColumnCount(); ++index) {
        final Path path = configuration.getTempFileGenerator().getTemporaryFile().toPath();
        attributeValues[startIndex + index] = path;
        writers[index] = Files.newBufferedWriter(path);
      }

      try (RelationalInputGenerator generator = table.selectInputGenerator();
          RelationalInput input = generator.generateNewCopy()) {

        while (input.hasNext()) {
          final List<String> values = input.next();
          for (int index = 0; index < values.size(); ++index) {
            final String value = values.get(index);
            if (value != null) {
              writers[index].write(value.replace('\n', '\0'));
              writers[index].newLine();
            }
          }
        }
      }

      for (final BufferedWriter writer : writers) {
        writer.close();
      }
    } catch (final Exception e) {
      throw new AlgorithmExecutionException("export", e);
    }
  }

  private void sortAndDistinct() throws IOException {
    final TPMMS tpmms = new TPMMS(configuration.getTpmmsConfiguration());
    for (final Path path : attributeValues) {
      tpmms.uniqueAndSort(path);
    }
  }

  private void computeInclusionDependencies() throws AlgorithmExecutionException {
    for (final Attribute dep : attributes) {
      for (final Attribute ref : attributes) {

        if (dep.getId() == ref.getId()) {
          continue;
        }

        if (isIncluded(dep, ref)) {
          receiveInd(dep, ref);
        }
      }
    }
  }

  private boolean isIncluded(final Attribute dependent, final Attribute referenced)
      throws AlgorithmExecutionException {

    try (BufferedReader depReader = Files.newBufferedReader(attributeValues[dependent.getId()]);
        BufferedReader refReader = Files.newBufferedReader(attributeValues[referenced.getId()])) {

      String dep = depReader.readLine();
      String ref = refReader.readLine();

      if (dep == null) {
        return configuration.isProcessEmptyColumns();
      }

      while (true) {

        if (dep == null) {
          // DEP is exhausted
          return true;
        }

        if (ref == null) {
          // REF is exhausted, but DEP still has a value
          return false;
        }

        final int result = dep.compareTo(ref);

        if (result < 0) {
          // DEP is smaller - REF cannot contain DEP's current value
          return false;
        } else if (result > 0) {
          // DEP is greater - advance REF until the value is (possibly) read
          ref = refReader.readLine();
        } else {
          // DEP == REF: advance both
          dep = depReader.readLine();
          ref = refReader.readLine();
        }
      }
    } catch (final Exception e) {
      throw new AlgorithmExecutionException("attribute comparison", e);
    }
  }

  private void receiveInd(final Attribute dep, final Attribute ref)
      throws AlgorithmExecutionException {

    final InclusionDependency ind = InclusionDependencyBuilder
        .dependent().column(dep.getTable(), dep.getColumn())
        .referenced().column(ref.getTable(), ref.getColumn())
        .build();

    configuration.getResultReceiver().receiveResult(ind);
  }
}
