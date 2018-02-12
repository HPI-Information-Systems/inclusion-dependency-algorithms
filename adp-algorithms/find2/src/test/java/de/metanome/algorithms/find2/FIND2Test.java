package de.metanome.algorithms.find2;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.input.ind.AlgorithmType;
import de.metanome.input.ind.InclusionDependencyParameters;
import de.metanome.util.InclusionDependencyResultReceiverStub;
import de.metanome.util.TestDatabase;
import de.metanome.validation.ValidationParameters;
import de.metanome.validation.database.QueryType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FIND2Test {

  private TestDatabase testDatabase;

  @AfterEach
  void tearDown() {
    if (testDatabase != null) {
      testDatabase.tearDown();
    }
  }

  @Test
  void testPaperExample() throws Exception {
    // GIVEN
    String relationName = "TEST";
    List<String> columnNames =
        asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N");

    List<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) {
      ci.add(new ColumnIdentifier(relationName, c));
    }

    List<ExIND> maximumINDs = new ArrayList<>();
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(0), ci.get(1), ci.get(2), ci.get(3), ci.get(4)),
            new ColumnPermutation(ci.get(7), ci.get(8), ci.get(9), ci.get(10), ci.get(11))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(3), ci.get(5), ci.get(6)),
            new ColumnPermutation(ci.get(10), ci.get(12), ci.get(13))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(4), ci.get(5)),
            new ColumnPermutation(ci.get(11), ci.get(12))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(4), ci.get(6)),
            new ColumnPermutation(ci.get(11), ci.get(13))));

    testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testPaperExample.csv")
            .build();

    testDatabase.setUp();

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(testDatabase.asConnectionGenerator());

    InclusionDependencyParameters inclusionDependencyParameters = new InclusionDependencyParameters();
    inclusionDependencyParameters.setAlgorithmType(AlgorithmType.FILE);
    inclusionDependencyParameters
        .setConfigurationString("inputPath=" + getClass().getResource("ind_input.json").getFile());

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .inclusionDependencyParameters(inclusionDependencyParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
  }

  @Test
  void testHypercliqueOnHigherAryEdges() throws Exception {
    // GIVEN
    String relationName = "TEST";
    List<String> columnNames =
        asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N");

    List<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) {
      ci.add(new ColumnIdentifier(relationName, c));
    }

    List<ExIND> maximumINDs = new ArrayList<>();
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(0), ci.get(1), ci.get(3), ci.get(4)),
            new ColumnPermutation(ci.get(7), ci.get(8), ci.get(10), ci.get(11))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(0), ci.get(2), ci.get(3), ci.get(4)),
            new ColumnPermutation(ci.get(7), ci.get(9), ci.get(10), ci.get(11))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(1), ci.get(2), ci.get(3)),
            new ColumnPermutation(ci.get(8), ci.get(9), ci.get(10))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(1), ci.get(2), ci.get(4)),
            new ColumnPermutation(ci.get(8), ci.get(9), ci.get(11))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(3), ci.get(5), ci.get(6)),
            new ColumnPermutation(ci.get(10), ci.get(12), ci.get(13))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(4), ci.get(5)),
            new ColumnPermutation(ci.get(11), ci.get(12))));
    maximumINDs.add(
        new ExIND(
            new ColumnPermutation(ci.get(4), ci.get(6)),
            new ColumnPermutation(ci.get(11), ci.get(13))));

    testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testHypercliqueOnHigherAryEdges.csv")
            .build();

    testDatabase.setUp();

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(testDatabase.asConnectionGenerator());

    InclusionDependencyParameters inclusionDependencyParameters = new InclusionDependencyParameters();
    inclusionDependencyParameters.setAlgorithmType(AlgorithmType.FILE);
    inclusionDependencyParameters
        .setConfigurationString("inputPath=" + getClass().getResource("ind_input.json").getFile());

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .inclusionDependencyParameters(inclusionDependencyParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
  }

  @Test
  void testIrreducibleGraph() throws Exception {
    // GIVEN
    String relationName = "IRREDUCIBLE";
    List<String> columnNames =
        asList(
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R");

    List<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) {
      ci.add(new ColumnIdentifier(relationName, c));
    }

    List<ExIND> maximumINDs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 3; j < 9; j++) {
        maximumINDs.add(
            new ExIND(
                new ColumnPermutation(ci.get(i), ci.get(j)),
                new ColumnPermutation(ci.get(i + 9), ci.get(j + 9))));
      }
    }

    for (int i = 3; i < 6; i++) {
      for (int j = 6; j < 9; j++) {
        maximumINDs.add(
            new ExIND(
                new ColumnPermutation(ci.get(i), ci.get(j)),
                new ColumnPermutation(ci.get(i + 9), ci.get(j + 9))));
      }
    }

    testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testIrreducibleExample.csv")
            .build();

    testDatabase.setUp();

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(testDatabase.asConnectionGenerator());

    InclusionDependencyParameters inclusionDependencyParameters = new InclusionDependencyParameters();
    inclusionDependencyParameters.setAlgorithmType(AlgorithmType.FILE);
    inclusionDependencyParameters
        .setConfigurationString(
            "inputPath=" + getClass().getResource("ind_input_irreducible.json").getFile());

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .inclusionDependencyParameters(inclusionDependencyParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
  }
}
