package de.metanome.algorithms.demarchi;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.RelationalInputGeneratorStub;
import de.metanome.util.Row;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DeMarchiBinderExampleTest {

  private static final String TABLE_NAME = "test";

  @Mock
  private TableInfoFactory tableInfoFactory;
  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private RelationalInputGenerator generator;
  private DeMarchi impl;

  private List<String> columnNames;
  private List<String> columnTypes;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    columnNames = asList("a", "b", "c", "d");
    columnTypes = columnNames.stream().map(x -> "str").collect(toList());
    generator = RelationalInputGeneratorStub.builder()
        .relationName("Test")
        .columnNames(columnNames)
        .row(Row.of("a", "c", "a", "c"))
        .row(Row.of("b", "c", "e", "c"))
        .row(Row.of("c", "b", "e", "d"))
        .row(Row.of("e", "e", "a", "c"))
        .row(Row.of("f", "b", "e", "c"))
        .build();

    given(tableInfoFactory.create(anyList(), anyList())).willReturn(tableFixture());

    impl = new DeMarchi(tableInfoFactory);
  }

  @Test
  void runDeMarchi() throws Exception {

    impl.execute(getConfiguration());

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(2)
        .containsAll(expectedInd());
  }

  private List<TableInfo> tableFixture() {
    return asList(TableInfo.builder().relationalInputGenerator(generator)
        .tableName(TABLE_NAME)
        .columnNames(columnNames)
        .columnTypes(columnTypes)
        .build());
  }

  private Configuration getConfiguration() {
    return Configuration.builder().relationalInputGenerator(generator)
        .resultReceiver(resultReceiver)
        .build();
  }

  private List<InclusionDependency> expectedInd() {
    return asList(
        InclusionDependencyBuilder.dependent().column(TABLE_NAME, "b")
            .referenced().column(TABLE_NAME, "a").build(),

        InclusionDependencyBuilder.dependent().column(TABLE_NAME, "c")
            .referenced().column(TABLE_NAME, "a")
            .build());
  }
}
