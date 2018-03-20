package de.metanome.util;

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.RequiredArgsConstructor;

public class TPMMS {

  public interface Output extends Closeable {

    void open(Path to) throws IOException;

    void write(String value) throws IOException;
  }

  private final TPMMSConfiguration configuration;

  public TPMMS(final TPMMSConfiguration configuration) {
    this.configuration = configuration;
  }

  public void uniqueAndSort(final Path path) throws IOException {
    uniqueAndSort(path, new DefaultOutput());
  }

  public void uniqueAndSort(final Path path, final Output output) throws IOException {
    new Execution(configuration, path, output).uniqueAndSort();
  }

  private static class Execution {

    private final TPMMSConfiguration configuration;
    private final Output output;
    private final Path origin;
    private final long maxMemoryUsage;

    private final SortedSet<String> values = new TreeSet<>();
    private final List<Path> spilledFiles = new ArrayList<>();
    private int totalValues = 0;
    private int valuesSinceLastMemoryCheck = 0;

    private Execution(final TPMMSConfiguration configuration, final Path origin,
        final Output output) {

      this.configuration = configuration;
      this.output = output;
      this.origin = origin;
      this.maxMemoryUsage = getMaxMemoryUsage(configuration);
    }

    private static long getMaxMemoryUsage(final TPMMSConfiguration configuration) {
      final long available = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
      return (long) (available * (configuration.getMaxMemoryUsagePercentage() / 100.0d));
    }

    private void uniqueAndSort() throws IOException {
      writeSpillFiles();

      if (spilledFiles.isEmpty()) {
        writeOutput();
      } else {
        if (!values.isEmpty()) {
          writeSpillFile();
        }

        new Merger(output).merge(spilledFiles, origin);
      }

      removeSpillFiles();
    }

    private void writeSpillFiles() throws IOException {
      try (BufferedReader reader = Files.newBufferedReader(origin)) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (isInputLimitExceeded()) {
            break;
          }
          values.add(line);
          maybeWriteSpillFile();
        }
      }
    }

    private boolean isInputLimitExceeded() {
      ++totalValues;
      return configuration.getInputRowLimit() > 0 && totalValues > configuration.getInputRowLimit();
    }

    private void maybeWriteSpillFile() throws IOException {
      ++valuesSinceLastMemoryCheck;
      if (valuesSinceLastMemoryCheck > configuration.getMemoryCheckInterval()
          && shouldWriteSpillFile()) {
        valuesSinceLastMemoryCheck = 0;
        writeSpillFile();
      }
    }

    private boolean shouldWriteSpillFile() {
      return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > maxMemoryUsage;
    }

    private void writeSpillFile() throws IOException {
      final Path target = Paths.get(origin + "#" + spilledFiles.size());
      write(target, values);
      spilledFiles.add(target);
      values.clear();
      System.gc();
    }

    private void write(final Path path, final Set<String> values) throws IOException {
      try (BufferedWriter writer = Files
          .newBufferedWriter(path, StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING)) {
        for (final String value : values) {
          writer.write(value);
          writer.newLine();
        }
        writer.flush();
      }
    }

    private void writeOutput() throws IOException {
      output.open(origin);
      try {
        for (final String value : values) {
          output.write(value);
        }
      } finally {
        output.close();
      }
    }

    private void removeSpillFiles() throws IOException {
      for (final Path spill : spilledFiles) {
        Files.delete(spill);
      }
      spilledFiles.clear();
    }
  }

  @RequiredArgsConstructor
  private static class Merger {

    private final Output output;

    private ObjectHeapPriorityQueue<TPMMSTuple> values;
    private BufferedReader[] readers;

    private void init(final List<Path> files) throws IOException {
      values = new ObjectHeapPriorityQueue<>(files.size());
      readers = new BufferedReader[files.size()];

      for (int index = 0; index < files.size(); ++index) {
        final BufferedReader reader = Files.newBufferedReader(files.get(index));
        readers[index] = reader;
        final String firstLine = reader.readLine();
        if (firstLine != null) {
          values.enqueue(new TPMMSTuple(firstLine, index));
        }
      }
    }

    private void merge(final List<Path> files, final Path to) throws IOException {
      init(files);

      output.open(to);
      try {
        String previousValue = null;
        while (!values.isEmpty()) {
          final TPMMSTuple current = values.dequeue();
          if (previousValue == null || !previousValue.equals(current.getValue())) {
            output.write(current.getValue());
          }

          previousValue = current.getValue();
          final String nextValue = readers[current.getReaderNumber()].readLine();
          if (nextValue != null) {
            current.setValue(nextValue);
            values.enqueue(current);
          }
        }
      } finally {
        output.close();
        closeReaders();
      }
    }

    private void closeReaders() throws IOException {
      for (BufferedReader reader : readers) {
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  private static class DefaultOutput implements Output {

    private BufferedWriter writer;

    @Override
    public void open(Path to) throws IOException {
      writer = Files.newBufferedWriter(to, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void write(String value) throws IOException {
      writer.write(value);
      writer.newLine();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }
}
