package de.metanome.algorithms.mind;

import static de.metanome.util.InclusionDependencyListConditions.binaryCountOf;
import static de.metanome.util.InclusionDependencyListConditions.unaryCountOf;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.input.TableInputGenerator;
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

class MindTest {

  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private TestDatabase testDatabase;
  private TableInputGenerator tableInputGenerator;

  private Configuration configuration;
  private Mind mind;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    testDatabase = TestDatabase.builder()
        .resourceClass(MindTest.class)
        .columnNames(asList("A", "B", "C", "D"))
        .csvPath("test.csv")
        .relationName("TEST")
        .build();

    testDatabase.setUp();

    tableInputGenerator = testDatabase.asTableInputGenerator();

    configuration = Configuration.builder()
        .tableInputGenerator(tableInputGenerator)
        .resultReceiver(resultReceiver)
        .validationParameters(validationParameters())
        .maxDepth(-1)
        .build();

    mind = new Mind();
  }

  private ValidationParameters validationParameters() {
    final ValidationParameters parameters = new ValidationParameters();
    parameters.setQueryType(QueryType.NOT_IN);
    parameters.setConnectionGenerator(tableInputGenerator.getDatabaseConnectionGenerator());
    return parameters;
  }

  @AfterEach
  void tearDown() {
    testDatabase.tearDown();
  }

  @Test
  void runMind() throws Exception {
    mind.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(8)
        .has(unaryCountOf(4))
        .has(binaryCountOf(4))
        .contains(exampleNary());
  }

  private InclusionDependency exampleNary() {
    final String relationName = testDatabase.getRelationName();

    return InclusionDependencyBuilder.dependent()
        .column(relationName, "A")
        .column(relationName, "B")
        .referenced()
        .column(relationName, "C")
        .column(relationName, "D")
        .build();
  }
}
