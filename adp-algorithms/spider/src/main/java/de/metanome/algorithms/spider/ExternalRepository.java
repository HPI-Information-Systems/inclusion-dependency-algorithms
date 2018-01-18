package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileCreationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.util.TPMMS;
import de.metanome.util.TPMMSConfiguration;
import de.metanome.util.TableInfo;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class ExternalRepository {

  ReadPointer[] uniqueAndSort(final SpiderConfiguration configuration, final TableInfo table)
      throws AlgorithmExecutionException {

    final Path[] paths = store(configuration, table);
    try {
      uniqueAndSort(paths, configuration.getTpmmsConfiguration());
      return open(paths);
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("TPMMS failure", e);
    }
  }

  private void uniqueAndSort(final Path[] paths, final TPMMSConfiguration configuration)
      throws IOException {

    final TPMMS tpmms = new TPMMS(configuration);
    for (final Path path : paths) {
      tpmms.uniqueAndSort(path);
    }
  }

  private Path[] store(final SpiderConfiguration configuration, final TableInfo table)
      throws AlgorithmExecutionException {

    final Path[] paths = new Path[table.getColumnCount()];
    final BufferedWriter[] writers = new BufferedWriter[table.getColumnCount()];
    openForWriting(configuration, paths, writers);
    write(table.selectInputGenerator(), writers);
    close(writers);
    return paths;
  }

  private void openForWriting(final SpiderConfiguration configuration, final Path[] paths,
      final BufferedWriter[] writers) throws AlgorithmExecutionException {

    try {
      for (int index = 0; index < paths.length; ++index) {
        final Path path = getPath(configuration);
        paths[index] = path;
        writers[index] = Files.newBufferedWriter(path);
      }
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("cannot open file for writing", e);
    }
  }

  private void write(final RelationalInputGenerator generator, final BufferedWriter[] writers)
      throws AlgorithmExecutionException {

    try (RelationalInput input = generator.generateNewCopy()) {
      while (input.hasNext()) {
        final List<String> next = input.next();
        for (int index = 0; index < writers.length; ++index) {
          final String value = index >= next.size() ? null : next.get(index);
          if (value != null) {
            writers[index].write(escape(value));
            writers[index].newLine();
          }
        }
      }
    } catch (final Exception e) {
      throw new AlgorithmExecutionException("error while storing attributes to disk", e);
    }
  }

  private void close(final Closeable[] toClose) throws AlgorithmExecutionException {
    try {
      for (final Closeable item : toClose) {
        item.close();
      }
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("cannot close", e);
    }
  }

  private ReadPointer[] open(final Path[] paths)
      throws AlgorithmExecutionException {

    try {
      final ReadPointer[] result = new ReadPointer[paths.length];
      for (int index = 0; index < paths.length; ++index) {
        result[index] = ReadPointer.of(paths[index]);
      }
      return result;
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("cannot open file for reading", e);
    }
  }

  private String escape(final String value) {
    return value.replace('\n', '\0');
  }

  private Path getPath(final SpiderConfiguration configuration)
      throws FileCreationException {

    return configuration.getTempFileGenerator().getTemporaryFile().toPath();
  }
}
