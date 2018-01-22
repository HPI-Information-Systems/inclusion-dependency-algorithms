package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.util.TestDatabase;
import de.metanome.util.InclusionDependencyResultReceiverStub;
import de.metanome.validation.ValidationParameters;
import de.metanome.validation.database.QueryType;

import java.util.List;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FIND2Test {

  @Test
  void testPaperExample() throws AlgorithmExecutionException {
    // GIVEN
    String relationName = "TEST";
    List<String> columnNames =
        asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N");

    ArrayList<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) ci.add(new ColumnIdentifier(relationName, c));

    ArrayList<ExIND> maximumINDs = new ArrayList<>();
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

    TestDatabase testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testPaperExample.csv")
            .build();

    try {
      testDatabase.setUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
    DatabaseConnectionGenerator connectionGenerator = testDatabase.asConnectionGenerator();

    RelationalInput riMock = mock(RelationalInput.class);
    when(riMock.relationName()).thenReturn(relationName);
    when(riMock.columnNames()).thenReturn(columnNames);
    TableInputGenerator tigMock = mock(TableInputGenerator.class);
    when(tigMock.generateNewCopy()).thenReturn(riMock);

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(connectionGenerator);

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .databaseConnectionGenerator(connectionGenerator)
            .tableInputGenerator(tigMock)
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
  }

  @Test
  void testHypercliqueOnHigherAryEdges() throws AlgorithmExecutionException {
    // GIVEN
    String relationName = "HAE";
    List<String> columnNames =
        asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N");

    ArrayList<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) ci.add(new ColumnIdentifier(relationName, c));

    ArrayList<ExIND> maximumINDs = new ArrayList<>();
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

    TestDatabase testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testHypercliqueOnHigherAryEdges.csv")
            .build();

    try {
      testDatabase.setUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
    DatabaseConnectionGenerator connectionGenerator = testDatabase.asConnectionGenerator();

    RelationalInput riMock = mock(RelationalInput.class);
    when(riMock.relationName()).thenReturn(relationName);
    when(riMock.columnNames()).thenReturn(columnNames);
    TableInputGenerator tigMock = mock(TableInputGenerator.class);
    when(tigMock.generateNewCopy()).thenReturn(riMock);

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(connectionGenerator);

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .databaseConnectionGenerator(connectionGenerator)
            .tableInputGenerator(tigMock)
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
  }

  @Test
  void testIrreducibleGraph() throws AlgorithmExecutionException {
    // GIVEN
    String relationName = "IRREDUCIBLE";
    List<String> columnNames =
        asList(
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R");

    ArrayList<ColumnIdentifier> ci = new ArrayList<>();
    for (String c : columnNames) ci.add(new ColumnIdentifier(relationName, c));

    ArrayList<ExIND> maximumINDs = new ArrayList<>();
    for (int i = 0; i < 3; i++)
      for (int j = 3; j < 9; j++)
        maximumINDs.add(
            new ExIND(
                new ColumnPermutation(ci.get(i), ci.get(j)),
                new ColumnPermutation(ci.get(i + 9), ci.get(j + 9))));

    for (int i = 3; i < 6; i++)
      for (int j = 6; j < 9; j++)
        maximumINDs.add(
            new ExIND(
                new ColumnPermutation(ci.get(i), ci.get(j)),
                new ColumnPermutation(ci.get(i + 9), ci.get(j + 9))));

    TestDatabase testDatabase =
        TestDatabase.builder()
            .resourceClass(FIND2Test.class)
            .relationName(relationName)
            .columnNames(columnNames)
            .csvPath("testIrreducibleExample.csv")
            .build();

    try {
      testDatabase.setUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
    DatabaseConnectionGenerator connectionGenerator = testDatabase.asConnectionGenerator();

    RelationalInput riMock = mock(RelationalInput.class);
    when(riMock.relationName()).thenReturn(relationName);
    when(riMock.columnNames()).thenReturn(columnNames);
    TableInputGenerator tigMock = mock(TableInputGenerator.class);
    when(tigMock.generateNewCopy()).thenReturn(riMock);

    InclusionDependencyResultReceiverStub resultReceiver =
        new InclusionDependencyResultReceiverStub();

    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setQueryType(QueryType.NOT_IN);
    validationParameters.setConnectionGenerator(connectionGenerator);

    // EXECUTE
    FIND2Configuration config =
        FIND2Configuration.builder()
            .databaseConnectionGenerator(connectionGenerator)
            .tableInputGenerator(tigMock)
            .resultReceiver(resultReceiver)
            .validationParameters(validationParameters)
            .startK(2)
            .build();

    FIND2 find2 = new FIND2(config);
    find2.execute();

    // THEN
    assertTrue(maximumINDs.containsAll(resultReceiver.getReceivedResults()));
    assertTrue(resultReceiver.getReceivedResults().containsAll(maximumINDs));
  }
}
