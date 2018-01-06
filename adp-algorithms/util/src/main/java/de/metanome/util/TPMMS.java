package de.metanome.util;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Builder;

public class TPMMS {

  private final int inputRowLimit;
  private final long maxMemoryUsage;
  private final int memoryCheckInterval;

  @Builder
  TPMMS(final int inputRowLimit, final long maxMemoryUsage, final int memoryCheckInterval) {
    this.inputRowLimit = inputRowLimit;
    this.maxMemoryUsage = maxMemoryUsage;
    this.memoryCheckInterval = memoryCheckInterval;
  }

  public void uniqueAndSort(final Path[] paths) throws AlgorithmExecutionException {
    try {
      for (final Path path : paths) {
        uniqueAndSort(path);
      }
    } catch (final IOException e) {
      throw new AlgorithmExecutionException("Error during uniqueAndSort", e);
    }
  }

  public void uniqueAndSort(final Path path) throws IOException {
    int totalValues = 0;
    int valuesSinceLastMemoryCheck = 0;
    final List<String> spilledFiles = new ArrayList<>();
    final SortedSet<String> values = new TreeSet<>();

    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String line;
      while ((line = reader.readLine()) != null) {
        ++totalValues;
        if (inputRowLimit > 0 && totalValues > inputRowLimit) {
          break;
        }

        values.add(line);
        ++valuesSinceLastMemoryCheck;

        if (valuesSinceLastMemoryCheck >= memoryCheckInterval) {
          valuesSinceLastMemoryCheck = 0;
          if (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > maxMemoryUsage) {
            final String spillFilePath = path + "#" + spilledFiles.size();
            write(spillFilePath, values);
            spilledFiles.add(spillFilePath);
            values.clear();
            System.gc();
          }
        }
      }
    }

    if (spilledFiles.isEmpty()) {
      write(path.toString(), values);
    } else {
      // Write last file
      if (!values.isEmpty()) {
        final String spillFilePath = path + "#" + spilledFiles.size();
        write(spillFilePath, values);
        spilledFiles.add(spillFilePath);
        values.clear();

        System.gc();
      }

      // Read, merge and write
      merge(path.toString(), spilledFiles);
    }
  }

  private void write(final String filePath, final Set<String> values) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
      final Iterator<String> valueIterator = values.iterator();
      while (valueIterator.hasNext()) {
        writer.write(valueIterator.next());
        writer.newLine();
      }
      writer.flush();
    }
  }

  private void merge(final String filePath, final List<String> spilledFiles) throws IOException {
    final BufferedReader[] readers = new BufferedReader[spilledFiles.size()];
    final PriorityQueue<TPMMSTuple> values = new ObjectHeapPriorityQueue<>(spilledFiles.size());

    try {
      for (int readerNumber = 0; readerNumber < spilledFiles.size(); ++readerNumber) {
        final BufferedReader reader = new BufferedReader(
            new FileReader(new File(spilledFiles.get(readerNumber))));
        readers[readerNumber] = reader;
        final String value = reader.readLine();

        if (value != null) {
          values.enqueue(new TPMMSTuple(value, readerNumber));
        }
      }
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
        String previousValue = null;
        while (!values.isEmpty()) {
          final TPMMSTuple tuple = values.dequeue();
          if (previousValue == null || !previousValue.equals(tuple.getValue())) {
            writer.write(tuple.getValue());
            writer.newLine();
          }

          previousValue = tuple.getValue();
          tuple.setValue(readers[tuple.getReaderNumber()].readLine());
          if (tuple.getValue() != null) {
            values.enqueue(tuple);
          }
        }
        writer.flush();
      }
    } finally {
      for (BufferedReader reader : readers) {
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

}
