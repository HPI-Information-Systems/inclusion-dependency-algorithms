package de.metanome.algorithms.sindd.sindd;

import de.metanome.algorithms.sindd.database.metadata.Attribute;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnaryINDsGenerator {

  private Map<String, Attribute> id2attMap;
  private Set<Set<String>> maxClusters;

  public UnaryINDsGenerator(Map<String, Attribute> id2attMap) {
    this.id2attMap = id2attMap;
    maxClusters = new HashSet<>();
  }

  public void generateFrom(Partition partition) throws IOException {
    List<File> partitionFiles = partition.getPartitionFiles();
    computeUnaryINDs(partitionFiles);
  }

  private void computeUnaryINDs(List<File> partitionFiles) throws IOException {
    AttributeSetCollector collector = new AttributeSetCollector(partitionFiles);
    while (collector.hasNext()) {
      Set<String> nextAttSet = collector.nextCluster();
      maxClusters.add(nextAttSet);
    }

    collector.close();

    for (Set<String> cluster : maxClusters) {
      updateReferencedAttributes(cluster);
    }
  }

  private void updateReferencedAttributes(Set<String> attSet) {
    Set<Attribute> attObjects = getAttObjects(attSet);
    for (Attribute attObj : attObjects) {
      if (attObj.isRefAttsInitialized()) {
        attObj.updateRefAttributes(attObjects);
      } else {
        attObj.initRefAttributes(attObjects);
      }
    }
  }

  private Set<Attribute> getAttObjects(Set<String> attSet) {
    Set<Attribute> attObjects = new HashSet<Attribute>(attSet.size());
    for (String att : attSet) {
      Attribute attObj = id2attMap.get(att);
      attObjects.add(attObj);
    }
    return attObjects;
  }
}
