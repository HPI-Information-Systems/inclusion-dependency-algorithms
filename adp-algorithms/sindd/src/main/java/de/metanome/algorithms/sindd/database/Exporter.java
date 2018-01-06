package de.metanome.algorithms.sindd.database;

import com.opencsv.CSVWriter;
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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class Exporter {

  private final static Logger LOGGER = Logger.getLogger("Sindd");

  public static void export(final Configuration configuration) throws IOException {

    LOGGER.info("exporting ....");
    long st = System.currentTimeMillis();

    List<Attribute> attributes = CommonObjects.getAttributes();

    for (Attribute attribute : attributes) {

      LOGGER.info("column: " + attribute.getQName());
      final BufferedReader values = getValues(attribute, configuration);
      exportAttValues(attribute, values);
      values.close();
    }

    long et = System.currentTimeMillis();
    Performance performance = CommonObjects.getPerformance();
    performance.addExportTime(et - st);
    LOGGER.info(" needed time " + TimeUtil.toString(et - st) + "\n");
  }

  private static BufferedReader getValues(Attribute attribute, Configuration configuration)
      throws IOException {

    final RelationalInputGenerator generator = attribute.getTable().selectInputGenerator();
    final int index = attribute.getTable().getColumnNames().indexOf(attribute.getName());
    final File file = new File("tmp" + File.separator + "swap");

    try (RelationalInput input = generator.generateNewCopy()) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
        while (input.hasNext()) {
          final String read = input.next().get(index);
          if (read != null) {
            writer.write(read.replace('\n', '\0'));
            writer.newLine();
          }
        }
      }
    } catch (final Exception e) {
      throw new IOException(e);
    }

    final TPMMS tpmms = new TPMMS(configuration.getTpmmsConfiguration());
    tpmms.uniqueAndSort(file.toPath());
    return new BufferedReader(new FileReader(file));
  }

  private static void exportAttValues(Attribute attribute, BufferedReader values)
      throws IOException {

    List<CSVWriter> writers = createWriters(attribute);
    int partitionNr = writers.size();
    try {
      if (partitionNr == 1) {
        exportWithoutPartitioning(attribute, values, writers.get(0));
      } else {
        exportWithPartitioning(attribute, values, writers);
      }
    } finally {
      for (CSVWriter writer : writers) {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  private static void exportWithPartitioning(Attribute attribute, BufferedReader values,
      List<CSVWriter> writers) throws IOException {

    int partitionNr = writers.size();
    String value;
    while ((value = values.readLine()) != null) {
      int writerIndex = getWriterIndex(value, partitionNr);
      CSVWriter writer = writers.get(writerIndex);
      writer.writeNext(new String[]{value, attribute.getId()});
    }
  }

  private static int getWriterIndex(String value, int partitionNr) {
    int hc = Math.abs(value.hashCode());
    return hc % partitionNr;
  }

  private static void exportWithoutPartitioning(Attribute attribute, BufferedReader values,
      CSVWriter writer) throws IOException {

    String value;
    while ((value = values.readLine()) != null) {
      writer.writeNext(new String[]{value, attribute.getId()});
    }
  }

  private static List<CSVWriter> createWriters(Attribute attribute) throws IOException {
    List<Partition> partitions = CommonObjects.getPartitions();
    List<CSVWriter> writers = new ArrayList<CSVWriter>(partitions.size());
    String attName = attribute.getQName();
    for (Partition partition : partitions) {
      File firstDir = partition.getFirstDir();
      File outFile = new File(firstDir + File.separator + attName);
      writers.add(FileUtil.createWriter(outFile));
    }
    return writers;
  }
}
