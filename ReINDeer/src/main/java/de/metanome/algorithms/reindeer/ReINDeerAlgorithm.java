package de.metanome.algorithms.reindeer;

import java.io.File;
import java.util.List;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;

public class ReINDeerAlgorithm {

  protected RelationalInputGenerator[] relationalInputGenerators;
  protected TableInputGenerator[] tableInputGenerators;

  protected InclusionDependencyResultReceiver resultReceiver;

  protected int maxNaryLevel;
  protected String tempFolderPath;
  protected boolean cleanTemp;
  protected boolean detectNary;
  protected boolean filterKeyForeignkeys;

  private String relationName;
  private List<String> columnNames;

  public String toString() {
    return "ReINDeerAlgorithm: \r\n\t" +
        "maxNaryLevel: " + this.maxNaryLevel + "\r\n\t" +
        "tempFolderPath: " + this.tempFolderPath + "\r\n\t" +
        "cleanTemp: " + this.cleanTemp + "\r\n\t" +
        "detectNary: " + this.detectNary + "\r\n\t" +
        "filterKeyForeignkeys: " + this.filterKeyForeignkeys + "\r\n\t";
  }

  public void execute() throws AlgorithmExecutionException {
    try {
      // To test if the algorithm gets data
      this.print();

      // To test if the algorithm outputs results
      this.outputSomething();

    } finally {
      if (this.cleanTemp) {
        this.delete(new File(this.tempFolderPath));
      }
    }
  }

  private void delete(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          if (files[i].isDirectory()) {
            this.delete(files[i]);
          } else {
            files[i].delete();
          }
        }
      }
    }
    directory.delete();
  }

  protected void print()
      throws InputGenerationException, InputIterationException, AlgorithmConfigurationException {
    RelationalInput input = this.relationalInputGenerators[0].generateNewCopy();
    this.relationName = input.relationName();
    this.columnNames = input.columnNames();

    System.out.print(this.relationName + "( ");
    for (String columnName : this.columnNames) {
      System.out.print(columnName + " ");
    }
    System.out.println(")");

    while (input.hasNext()) {
      System.out.print("| ");

      List<String> record = input.next();
      for (String value : record) {
        System.out.print(value + " | ");
      }

      System.out.println();
    }
  }

  protected void outputSomething()
      throws CouldNotReceiveResultException, ColumnNameMismatchException {
    ColumnPermutation lhs = new ColumnPermutation(
        new ColumnIdentifier(this.relationName, this.columnNames.get(0)),
        new ColumnIdentifier(this.relationName, this.columnNames.get(1)),
        new ColumnIdentifier(this.relationName, this.columnNames.get(2)));
    ColumnPermutation rhs = new ColumnPermutation(
        new ColumnIdentifier(this.relationName, this.columnNames.get(3)),
        new ColumnIdentifier(this.relationName, this.columnNames.get(4)),
        new ColumnIdentifier(this.relationName, this.columnNames.get(5)));
    InclusionDependency ind = new InclusionDependency(lhs, rhs);

    this.resultReceiver.receiveResult(ind);
  }
}
