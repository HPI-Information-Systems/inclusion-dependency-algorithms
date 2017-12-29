package de.metanome.algorithms.sindd.sindd;

import com.opencsv.CSVReader;
import de.metanome.algorithms.sindd.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MergedListReader implements Comparable<MergedListReader> {

  private CSVReader reader;
  private String[] nextList;

  public MergedListReader(File inputFile) throws IOException {
    reader = FileUtil.createReader(inputFile);
  }

  public boolean hasNext() throws IOException {
    nextList = reader.readNext();
    return nextList != null;
  }

  public Set<String> nextAttSet() {
    Set<String> nextAttSet = new HashSet<String>();
    for (int i = 1; i < nextList.length; ++i) {
      nextAttSet.add(nextList[i]);
    }
    return nextAttSet;
  }

  public void addNextAttributesTo(Set<String> attSet) {
    for (int i = 1; i < nextList.length; ++i) {
      attSet.add(nextList[i]);
    }
  }

  public String getNextValue() {
    String nextValue = nextList[0];
    return nextValue;
  }

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public int compareTo(MergedListReader other) {
    String nextVal = getNextValue();
    String otherNextVal = other.getNextValue();
    return nextVal.compareTo(otherNextVal);
  }
}
