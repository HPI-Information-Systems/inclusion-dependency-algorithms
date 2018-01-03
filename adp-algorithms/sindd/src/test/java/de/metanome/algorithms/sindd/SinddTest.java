package de.metanome.algorithms.sindd;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.Row;
import de.metanome.util.TableInputGeneratorStub;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SinddTest {

  private Sindd impl;
  private static final String TABLE_NAME = "TEST";

  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private List<String> columnNames;

  private RelationalInputGenerator generator;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);

    columnNames = asList("a", "b", "c");

    generator = TableInputGeneratorStub.builder()
        .relationName(TABLE_NAME)
        .columnNames(columnNames)
        .row(Row.of("1", "1", "1"))
        .row(Row.of("1", "1", "3"))
        .row(Row.of(null, "2", "2"))
        .build();

    impl = new Sindd();
  }


  @Test
  void runSindd() throws Exception {
    impl.execute(getConfiguration());

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(3)
        .containsOnlyElementsOf(expectedInd());
  }

  private Configuration getConfiguration() throws Exception {
    return Configuration.builder()
        .relationalInputGenerator(generator)
        .resultReceiver(resultReceiver)
        .openFileNr(5)
        .partitionNr(1)
        .build();
  }

  private List<InclusionDependency> expectedInd() {
    final List<InclusionDependency> inds = new ArrayList<>();

    inds.add(InclusionDependencyBuilder
        .dependent().column(TABLE_NAME, "a")
        .referenced().column(TABLE_NAME, "b").build());

    inds.add(InclusionDependencyBuilder
        .dependent().column(TABLE_NAME, "a")
        .referenced().column(TABLE_NAME, "c").build());

    inds.add(InclusionDependencyBuilder
        .dependent().column(TABLE_NAME, "b")
        .referenced().column(TABLE_NAME, "c").build());

    return inds;
  }
}
