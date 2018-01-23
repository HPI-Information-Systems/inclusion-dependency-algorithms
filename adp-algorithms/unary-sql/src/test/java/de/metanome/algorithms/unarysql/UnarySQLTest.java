package de.metanome.algorithms.unarysql;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.TestDatabase;
import de.metanome.validation.ValidationParameters;
import de.metanome.validation.database.QueryType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class UnarySQLTest {

  private static final String TABLE = "TABLE1";

  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private TestDatabase testDatabase;
  private Configuration configuration;
  private UnarySQL unarySQL;


  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    testDatabase = TestDatabase.builder()
        .relationName(TABLE)
        .columnNames(asList("A", "B", "C"))
        .resourceClass(UnarySQLTest.class)
        .csvPath("unaries.csv")
        .build();

    testDatabase.setUp();

    configuration = Configuration.builder()
        .tableInputGenerator(testDatabase.asTableInputGenerator())
        .resultReceiver(resultReceiver)
        .validationParameters(getValidationParameters())
        .build();

    unarySQL = new UnarySQL();
  }

  @AfterEach
  void tearDown() {
    testDatabase.tearDown();
  }

  private ValidationParameters getValidationParameters() {
    final ValidationParameters parameters = new ValidationParameters();
    parameters.setQueryType(QueryType.LEFT_OUTER_JOIN);
    parameters.setConnectionGenerator(testDatabase.asConnectionGenerator());
    return parameters;
  }

  @Test
  void discoverUnaryWithEmptyColumns() throws Exception {
    configuration.setProcessEmptyColumns(true);

    unarySQL.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(3)
        .contains(indWithEmptyLhs());
  }

  @Test
  void discoverUnaryDiscardEmptyColumns() throws Exception {
    configuration.setProcessEmptyColumns(false);

    unarySQL.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(1)
        .first()
        .isEqualTo(validInd());
  }

  private InclusionDependency indWithEmptyLhs() {
    return InclusionDependencyBuilder.dependent().column(TABLE, "A")
        .referenced().column(TABLE, "C").build();
  }

  private InclusionDependency validInd() {
    return InclusionDependencyBuilder.dependent().column(TABLE, "B")
        .referenced().column(TABLE, "C").build();
  }
}
