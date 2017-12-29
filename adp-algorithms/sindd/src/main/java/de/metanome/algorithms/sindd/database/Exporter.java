package de.metanome.algorithms.sindd.database;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithms.sindd.Configuration;
import de.metanome.algorithms.sindd.sindd.Partition;
import de.metanome.algorithms.sindd.util.CommonObjects;
import de.metanome.algorithms.sindd.util.FileUtil;
import de.metanome.algorithms.sindd.util.Performance;
import de.metanome.algorithms.sindd.util.TimeUtil;
import org.apache.log4j.Logger;

import com.opencsv.CSVWriter;

import de.metanome.algorithms.sindd.database.metadata.Attribute;

public class Exporter {

  private final static Logger LOGGER = Logger.getLogger("Sindd");

  public static void export(Configuration configuration) throws SQLException, IOException {

    LOGGER.info("exporting ....");
    long st = System.currentTimeMillis();

    ResultSet resultSet = null;

    List<Attribute> attributes = CommonObjects.getAttributes();

    try {
      for (Attribute attribute : attributes) {

        LOGGER.info("column: " + attribute.getQName());
        //sqlQuery = createSqlQuery(attribute);
        resultSet = getValues(attribute, configuration);
        exportAttValues(attribute, resultSet);

      }

      long et = System.currentTimeMillis();
      Performance performance = CommonObjects.getPerformance();
      performance.addExportTime(et - st);
      LOGGER.info(" needed time " + TimeUtil.toString(et - st) + "\n");

    } catch (AlgorithmExecutionException e) {
      e.printStackTrace();
    }
  }

  private static ResultSet getValues(Attribute attribute, Configuration configuration)
      throws AlgorithmExecutionException {

    StringBuffer sql = new StringBuffer();
    sql.append("select distinct t." + attribute.getName() + " from " + attribute.getTableName() + " t where t." + attribute.getName()
        + " is not null order by cast(t." + attribute.getName() + " as binary)");
    DatabaseConnectionGenerator gen = configuration.getDatabaseConnectionGenerator();
    ResultSet result = gen.generateResultSetFromSql(sql.toString());
    //final TableInputGenerator generator = attribute.getTable().getTableInputGenerator();

    return result; //generator.sortBy(attribute.getName(), true);
  }

  private static void exportAttValues(Attribute attribute, ResultSet resultSet) throws IOException, SQLException {
    List<CSVWriter> writers = createWriters(attribute);
    int partitionNr = writers.size();
    try {
      if (partitionNr == 1) {
        exportWithoutPartitioning(attribute, resultSet, writers.get(0));
      } else {
        exportWithPartitioning(attribute, resultSet, writers);
      }
    } finally {
      for (CSVWriter writer : writers) {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  private static void exportWithPartitioning(Attribute attribute, ResultSet resultSet, List<CSVWriter> writers)
      throws SQLException {

    int partitionNr = writers.size();

    while (resultSet.next()) {
      String value = resultSet.getString(1);
      int writerIndex = getWriterIndex(value, partitionNr);
      CSVWriter writer = writers.get(writerIndex);
      writer.writeNext(new String[]{value, attribute.getId()});
    }
  }

  private static int getWriterIndex(String value, int partitionNr) {
    int hc = value.hashCode();
    return hc % partitionNr;
  }

  private static void exportWithoutPartitioning(Attribute attribute, ResultSet resultSet, CSVWriter writer)
      throws SQLException {

    while (resultSet.next()) {
      String value = resultSet.getString(1);
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
