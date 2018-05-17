package de.metanome.algorithms.spiderbruteforce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.FileGeneratorFake;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.RelationalInputStub;
import de.metanome.util.Row;
import de.metanome.util.TPMMSConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SpiderBruteForceTest {

  private static final String COL_A = "A";
  private static final String COL_B = "B";
  private static final String COL_C = "C";

  @Mock
  private RelationalInputGenerator generator;
  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private RelationalInput input;
  private Configuration configuration;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    input = RelationalInputStub.builder()
        .relationName("Test")
        .columnName(COL_A).columnName(COL_B).columnName(COL_C)
        .row(Row.of("x", "z", null))
        .row(Row.of("x", "y", null))
        .row(Row.of("y", "x", null))
        .build();

    given(generator.generateNewCopy()).willReturn(input);

    configuration = Configuration.builder()
        .tempFileGenerator(new FileGeneratorFake())
        .resultReceiver(resultReceiver)
        .relationalInputGenerator(generator)
        .tpmmsConfiguration(TPMMSConfiguration.withDefaults())
        .build();
  }

  @Test
  void runSpider() throws Exception {
    final SpiderBruteForce spider = new SpiderBruteForce();

    spider.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(1)
        .first()
        .isEqualTo(expectedInd());
  }

  @Test
  void runSpiderWithEmptyColumn() throws Exception {
    final SpiderBruteForce spider = new SpiderBruteForce();
    configuration.setProcessEmptyColumns(true);

    spider.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(3);
  }

  private InclusionDependency expectedInd() {
    return InclusionDependencyBuilder
        .dependent().column(input.relationName(), COL_A)
        .referenced().column(input.relationName(), COL_B).build();
  }

}
