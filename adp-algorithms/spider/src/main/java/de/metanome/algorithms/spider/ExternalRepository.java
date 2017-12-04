package de.metanome.algorithms.spider;

import static java.util.stream.Collectors.toList;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithms.spider.tpmms.TPMMS;
import de.metanome.util.TableInfo;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

class ExternalRepository {

  private static final String PREFIX = "spider";
  private static final Pattern NEWLINE = Pattern.compile(Pattern.quote("\n"));
  private static final String NULL = "⟂";

  ReadPointer[] uniqueAndSort(final SpiderConfiguration configuration, final TableInfo table)
      throws AlgorithmExecutionException {

    final Path[] paths = store(configuration, table);
    final TPMMS tpmms = getTpmms(configuration);
    tpmms.uniqueAndSort(paths);
    return open(paths);
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
          writers[index].write(escape(value));
          writers[index].newLine();
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

  private TPMMS getTpmms(final SpiderConfiguration configuration) {
    return TPMMS.builder()
        .inputRowLimit(configuration.getInputRowLimit())
        .maxMemoryUsage(configuration.getMaxMemoryUsage())
        .memoryCheckInterval(configuration.getMemoryCheckInterval())
        .build();
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
    return value == null ? NULL : NEWLINE.matcher(value).replaceAll("\0");
  }

  private Path getPath(final SpiderConfiguration configuration)
      throws IOException {

    if (configuration.getTemporaryFolderPath() == null ||
        configuration.getTemporaryFolderPath().isEmpty()) {
      return Files.createTempFile(PREFIX, null);
    }

    final Path path = Paths.get(configuration.getTemporaryFolderPath());
    return Files.createTempFile(path, PREFIX, null);
  }

  void close(final SpiderConfiguration configuration) throws IOException {
    if (!configuration.isClearTemporaryFolder()) {
      return;
    }

    final String toScan = configuration.getTemporaryFolderPath() == null ?
        System.getProperty("java.io.tmpdir") :
        configuration.getTemporaryFolderPath();

    final List<Path> paths = Files.walk(Paths.get(toScan))
        .filter(path -> path.toFile().getName().startsWith(PREFIX))
        .collect(toList());

    for (final Path path : paths) {
      Files.delete(path);
    }
  }
}
