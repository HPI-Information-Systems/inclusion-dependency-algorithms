package de.metanome.algorithms.sindd;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.sindd.database.Exporter;
import de.metanome.algorithms.sindd.database.metadata.Attribute;
import de.metanome.algorithms.sindd.database.metadata.MetadataBuilder;
import de.metanome.algorithms.sindd.sindd.Merger;
import de.metanome.algorithms.sindd.sindd.Partition;
import de.metanome.algorithms.sindd.sindd.UnaryINDsGenerator;
import de.metanome.algorithms.sindd.util.CommonObjects;
import de.metanome.algorithms.sindd.util.PartitionPerformance;
import de.metanome.algorithms.sindd.util.Performance;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;


class Sindd {

  private Configuration configuration;
  private final static Logger LOGGER = Logger.getLogger("Sindd");


  void execute(final Configuration configuration) throws AlgorithmExecutionException {

    this.configuration = configuration;
    try {
      createPerformanceObject();
      createPartitions();
      createPartitionDirectories();
      createMetadata();

      exportData();
      discoverUnaryINDs();

      printUnaryINDs();

    } catch (Exception e){
      throw new AlgorithmExecutionException("Failed to execute Sindd", e);

    }
  }


  private void receiveIND(final Attribute lhs, final Attribute rhs)
      throws AlgorithmExecutionException {

    final ColumnIdentifier leftColumn = new ColumnIdentifier(lhs.getTableName(), lhs.getName());
    final ColumnIdentifier rightColumn = new ColumnIdentifier(rhs.getTableName(), rhs.getName());
    final InclusionDependency ind = new InclusionDependency(new ColumnPermutation(leftColumn),
        new ColumnPermutation(rightColumn));
    configuration.getResultReceiver().receiveResult(ind);
  }

  private void printUnaryINDs() throws AlgorithmExecutionException {
    List<Attribute> attributes = CommonObjects.getAttributes();
    for (Attribute att : attributes) {
      StringBuffer sb = new StringBuffer();
      sb.append(att.getQName());
      sb.append("--> [");
      Set<Attribute> refAtts = att.getRefAttributes();
      if (refAtts.size() > 1) {
        for (Attribute refAtt : refAtts) {
          if(refAtt.equals(att)){
            continue;
          }
          receiveIND(att, refAtt);
          sb.append(refAtt.getQName());
          sb.append(", ");
        }
        sb.replace(sb.length() - 2, sb.length(), "]");
        System.out.println(sb);
      }
    }
  }

  private void discoverUnaryINDs() throws IOException, InterruptedException {
    LOGGER.info("......discoverying ... ");

    int openFileNumber = configuration.getOpenFileNr();
    Merger merger = new Merger(openFileNumber);

    Map<String, Attribute> id2attMap = CommonObjects.getId2attributeMap();
    UnaryINDsGenerator uindsGenerator = new UnaryINDsGenerator(id2attMap);

    Performance performance = de.metanome.algorithms.sindd.util.CommonObjects.getPerformnce();
    List<Partition> partitions = CommonObjects.getPartitions();
    for (Partition partition : partitions) {
      LOGGER.info("partition " + partition.getId() + ": ");

      PartitionPerformance partitionPerformance = new PartitionPerformance(partition.getId());
      merge(merger, partition, partitionPerformance);
      computeUinds(uindsGenerator, partition, partitionPerformance);
      performance.addPartitionPerformance(partitionPerformance);

      LOGGER.info( "needed time: " + partitionPerformance);
    }
    LOGGER.info("total needed time: " + performance.toStringWithoutExport() + "\n");
  }

  private void merge(Merger merger, Partition partition, PartitionPerformance partitionPerformance)
          throws IOException, InterruptedException {
    long st = System.currentTimeMillis();

    merger.merge(partition);

    long et = System.currentTimeMillis();
    partitionPerformance.setMerginTime(st, et);
  }

  private void computeUinds(UnaryINDsGenerator generator, Partition partition,
                                   PartitionPerformance partitionPerformance) throws IOException {
    long st = System.currentTimeMillis();

    generator.generateFrom(partition);

    long et = System.currentTimeMillis();
    partitionPerformance.setUindsGenTime(st, et);
  }

  private static void exportData() throws SQLException, IOException {
    Exporter.export();
  }

  private void createMetadata() throws SQLException {
    MetadataBuilder.build(configuration);
  }

  private void createPartitions() {
    int partitionNr = configuration.getPartitionNr();
    File workingDir = new File("tmp");

    List<Partition> partitions = new ArrayList<Partition>(partitionNr);
    for (int pId = 1; pId <= partitionNr; pId++) {
      partitions.add(new Partition(pId, workingDir));
    }
    CommonObjects.setPartitions(partitions);
  }

  private static void createPartitionDirectories() {
    List<Partition> partitions = CommonObjects.getPartitions();
    for (Partition partition : partitions) {
      createDirectory(partition.getFirstDir());
      createDirectory(partition.getSecondDir());
    }
  }

  private static void createDirectory(File dirFile) {
    dirFile.mkdir();
  }

  private static void createPerformanceObject() {
    Performance performance = new Performance();
    CommonObjects.setPerformance(performance);
  }
}
