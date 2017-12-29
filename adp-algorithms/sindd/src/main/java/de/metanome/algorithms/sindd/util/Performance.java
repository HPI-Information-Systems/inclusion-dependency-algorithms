package de.metanome.algorithms.sindd.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Performance implements Iterable<PartitionPerformance> {

  private List<PartitionPerformance> pPerformances = new ArrayList<PartitionPerformance>();

  private long exportTime;
  private long totalMergingTime;
  private long totalUindsGenTime;

  public void addExportTime(long exportTime) {
    this.exportTime = exportTime;
  }

  public void addPartitionPerformance(PartitionPerformance partitionPerformance) {
    pPerformances.add(partitionPerformance);
    totalMergingTime += partitionPerformance.getMergingTime();
    totalUindsGenTime += partitionPerformance.getUindsGenTime();
  }

  public long getTotalMergingTime() {
    return totalMergingTime;
  }

  public long getTotalUindsGenTime() {
    return totalUindsGenTime;
  }

  public long getExportTime() {
    return exportTime;
  }

  public long getTotalTimeWithoutExport() {
    return getTotalMergingTime() + getTotalUindsGenTime();
  }

  public long getTotalTime() {
    return getExportTime() + getTotalTimeWithoutExport();
  }

  public String toStringWithoutExport() {
    StringBuffer sb = new StringBuffer();

    sb.append("merging=[");
    sb.append(TimeUtil.toString(getTotalMergingTime()));
    sb.append("], ");
    sb.append("uindsGen=[ ");
    sb.append(TimeUtil.toString(getTotalUindsGenTime()));
    sb.append("], ");
    sb.append("total=[");
    sb.append(TimeUtil.toString(getTotalTimeWithoutExport()));
    sb.append(" (");
    sb.append(getTotalTimeWithoutExport());
    sb.append(" ms)]");

    return sb.toString();
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();

    sb.append("export =[");
    sb.append(TimeUtil.toString(getExportTime()));
    sb.append("], ");
    sb.append("totalWithoutExport=[");
    sb.append(TimeUtil.toString(getTotalTimeWithoutExport()));
    sb.append(" (");
    sb.append(getTotalTimeWithoutExport());
    sb.append(" ms)]");
    sb.append(", ");
    sb.append("total=[");
    sb.append(TimeUtil.toString(getTotalTime()));
    sb.append(" (");
    sb.append(getTotalTime());
    sb.append(" ms)]");

    return sb.toString();

  }

  @Override
  public Iterator<PartitionPerformance> iterator() {
    return pPerformances.iterator();
  }
}
