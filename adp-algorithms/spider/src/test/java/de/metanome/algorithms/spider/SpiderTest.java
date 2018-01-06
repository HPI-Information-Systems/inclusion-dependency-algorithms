package de.metanome.algorithms.spider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.FileGeneratorFake;
import de.metanome.util.RelationalInputStub;
import de.metanome.util.Row;
import de.metanome.util.TPMMSConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SpiderTest {

  private static final String COL_A = "A";
  private static final String COL_B = "B";

  @Mock
  private RelationalInputGenerator generator;
  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private RelationalInput input;
  private SpiderConfiguration configuration;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    input = RelationalInputStub.builder()
        .relationName("Test")
        .columnName(COL_A).columnName(COL_B)
        .row(Row.of("x", "z"))
        .row(Row.of("x", "y"))
        .row(Row.of("y", "x"))
        .build();

    given(generator.generateNewCopy()).willReturn(input);

    configuration = SpiderConfiguration.builder()
        .tempFileGenerator(new FileGeneratorFake())
        .resultReceiver(resultReceiver)
        .relationalInputGenerator(generator)
        .tpmmsConfiguration(TPMMSConfiguration.withDefaults())
        .build();
  }

  @Test
  void runSpider() throws Exception {
    final Spider spider = new Spider();

    spider.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(1)
        .first()
        .isEqualTo(expectedInd());
  }

  private InclusionDependency expectedInd() {
    final ColumnPermutation left = new ColumnPermutation(
        new ColumnIdentifier(input.relationName(), COL_A));
    final ColumnPermutation right = new ColumnPermutation(
        new ColumnIdentifier(input.relationName(), COL_B));
    return new InclusionDependency(left, right);
  }

}
