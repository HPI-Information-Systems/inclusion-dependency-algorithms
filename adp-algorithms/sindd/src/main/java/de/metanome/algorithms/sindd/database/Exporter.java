package de.metanome.algorithms.sindd.database;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithms.sindd.Configuration;
import de.metanome.algorithms.sindd.database.metadata.Attribute;
import de.metanome.algorithms.sindd.sindd.Partition;
import de.metanome.algorithms.sindd.util.CommonObjects;
import de.metanome.algorithms.sindd.util.FileUtil;
import de.metanome.algorithms.sindd.util.Performance;
import de.metanome.algorithms.sindd.util.TimeUtil;
import de.metanome.util.TPMMS;
import de.metanome.util.TableInfo;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.log4j.Logger;

public class Exporter {

  private final static Logger LOGGER = Logger.getLogger("Sindd");

  public static void export(final Configuration configuration) throws IOException {

    LOGGER.info("exporting ....");
    long st = System.currentTimeMillis();

    for (final TableInfo info : CommonObjects.getTables()) {
      export(configuration, info);
    }

    long et = System.currentTimeMillis();
    Performance performance = CommonObjects.getPerformance();
    performance.addExportTime(et - st);
    LOGGER.info(" needed time " + TimeUtil.toString(et - st) + "\n");
  }

  private static void export(final Configuration configuration,
      final TableInfo table) throws IOException {

    warnOnMultipleScans(configuration, table);
    final List<Attribute> attributes = CommonObjects.getTableToAttribute().get(table);
    final List<List<Attribute>> groups = Lists.partition(attributes, configuration.getOpenFileNr());

    int startIndex = 0;
    for (final List<Attribute> group : groups) {
      final List<Writer> writers = writeToDisk(configuration, table, group, startIndex);
      uniqueAndSort(configuration, writers);
      startIndex += group.size();
    }
  }

  private static void warnOnMultipleScans(final Configuration configuration,
      final TableInfo table) {
    if (configuration.getOpenFileNr() < table.getColumnCount()) {
      LOGGER.error("For table " + table.getTableName()
          + " multiple scans are required since the column count (" + table.getColumnCount()
          + ") exceeds the number of open files (" + configuration.getOpenFileNr()
          + "). This may impact I/O performance greatly.");
    }
  }

  private static List<Writer> writeToDisk(final Configuration configuration, final TableInfo table,
      final List<Attribute> group, final int startIndex) throws IOException {

    final RelationalInputGenerator generator = table.selectInputGenerator();
    try (RelationalInput in = generator.generateNewCopy()) {
      return writeToDisk(configuration, in, group, startIndex);
    } catch (final Exception e) {
      throw new IOException(e);
    } finally {
      try {
        generator.close();
      } catch (final Exception e) {
        throw new IOException("terrible", e);
      }
    }
  }

  private static List<Writer> writeToDisk(final Configuration configuration,
      final RelationalInput input,
      final List<Attribute> attributes, final int startIndex)
      throws IOException, AlgorithmExecutionException {

    final List<Writer> writers = createWriters(configuration, attributes);
    while (input.hasNext()) {
      final List<String> read = input.next().subList(startIndex, startIndex + attributes.size());
      for (int index = 0; index < read.size(); ++index) {
        final String value = read.get(index);
        if (value != null) {
          writers.get(index).write(value.replace('\n', '\0'));
        }
      }
    }
    closeWriters(writers);
    return writers;
  }

  private static List<Writer> createWriters(final Configuration configuration,
      final List<Attribute> attributes) throws IOException {

    final List<Writer> writers = new ArrayList<>(attributes.size());
    for (final Attribute attribute : attributes) {
      writers.add(createWriter(configuration, attribute));
    }
    return writers;
  }

  private static Writer createWriter(final Configuration configuration, final Attribute attribute)
      throws IOException {

    if (configuration.getPartitionNr() == 1) {
      return createSinglePartitionWriter(attribute);
    } else {
      return createPartitionedWriter(attribute);
    }
  }

  private static Writer createSinglePartitionWriter(final Attribute attribute) throws IOException {
    final Partition partition = CommonObjects.getPartitions().get(0);
    final Path destination = partition.getFirstDir().toPath().resolve(attribute.getQName());
    return new SinglePartitionWriter(destination, attribute.getId());
  }

  private static Writer createPartitionedWriter(final Attribute attribute) throws IOException {

    final List<Partition> partitions = CommonObjects.getPartitions();
    final List<Path> files = new ArrayList<>(partitions.size());

    for (final Partition partition : partitions) {
      final Path destination = partition.getFirstDir().toPath().resolve(attribute.getQName());
      files.add(destination);
    }

    return new PartitionedWriter(files, attribute.getId());
  }

  private static void closeWriters(final List<Writer> writers) throws IOException {
    for (final Writer writer : writers) {
      writer.close();
    }
  }

  private static void uniqueAndSort(final Configuration configuration, final List<Writer> writers)
      throws IOException {

    final TPMMS tpmms = new TPMMS(configuration.getTpmmsConfiguration());
    for (final Writer writer : writers) {
      for (final Path written : writer.getWritten()) {
        tpmms.uniqueAndSort(written, new CsvTpmmsOutput(writer.getAttributeId()));
      }
    }
  }

  private interface Writer extends Closeable {

    void write(String value) throws IOException;

    List<Path> getWritten();

    String getAttributeId();
  }

  private static class SinglePartitionWriter implements Writer {

    private final Path path;
    private final BufferedWriter writer;
    @Getter
    private final String attributeId;

    private SinglePartitionWriter(final Path path, final String attributeId) throws IOException {
      this.path = path;
      this.writer = Files.newBufferedWriter(path);
      this.attributeId = attributeId;
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

    @Override
    public List<Path> getWritten() {
      return Collections.singletonList(path);
    }
  }

  private static class PartitionedWriter implements Writer {

    private final List<Path> paths;
    private final List<BufferedWriter> writers;
    @Getter
    private final String attributeId;

    private PartitionedWriter(final List<Path> paths, final String attributeId) throws IOException {
      this.paths = paths;
      this.writers = new ArrayList<>(paths.size());
      for (final Path path : paths) {
        this.writers.add(Files.newBufferedWriter(path));
      }
      this.attributeId = attributeId;
    }

    @Override
    public void write(String value) throws IOException {
      final BufferedWriter writer = selectWriter(value);
      writer.write(value);
      writer.newLine();
    }

    private BufferedWriter selectWriter(final String value) {
      final int hc = Math.abs(value.hashCode());
      return writers.get(hc % writers.size());
    }

    @Override
    public void close() throws IOException {
      for (BufferedWriter writer : writers) {
        writer.close();
      }
    }

    @Override
    public List<Path> getWritten() {
      return paths;
    }
  }


  @RequiredArgsConstructor
  private static class CsvTpmmsOutput implements TPMMS.Output {

    private final String attributeId;
    private CSVWriter writer;

    @Override
    public void open(Path to) throws IOException {
      writer = FileUtil.createWriter(to.toFile());
    }

    @Override
    public void write(String value) {
      writer.writeNext(new String[]{value, attributeId});
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }
}