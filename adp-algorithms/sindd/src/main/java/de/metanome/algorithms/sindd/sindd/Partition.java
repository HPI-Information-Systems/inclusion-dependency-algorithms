package de.metanome.algorithms.sindd.sindd;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Partition {

  private int id;
  private File dir1;
  private File dir2;

  public Partition(int id, File parentDir) {
    this.id = id;
    this.dir1 = new File(parentDir + File.separator + "f-" + id);
    this.dir2 = new File(parentDir + File.separator + "s-" + id);
  }

  public int getId() {
    return id;
  }

  public File getFirstDir() {
    return dir1;
  }

  public File getSecondDir() {
    return dir2;
  }

  public List<File> getPartitionFiles() {
    List<File> partitionFiles = new ArrayList<File>();
    partitionFiles.addAll(getFilesInFirstDir());
    partitionFiles.addAll(getFilesInSecondDir());
    return partitionFiles;
  }

  public List<File> getFilesInFirstDir() {
    List<File> files = new ArrayList<File>();
    files.addAll(Arrays.asList(dir1.listFiles()));
    return files;
  }

  public List<File> getFilesInSecondDir() {
    List<File> files = new ArrayList<File>();
    files.addAll(Arrays.asList(dir2.listFiles()));
    return files;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    sb.append(id);
    sb.append(": ");
    sb.append("(");
    sb.append(dir1.getAbsolutePath());
    sb.append(", ");
    sb.append(dir2.getAbsolutePath());
    sb.append(")]");
    return sb.toString();
  }
}
