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

class DeMarchiFastPathTest {

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
        .row(Row.of("1", "1", "1", null))
        .row(Row.of("1", "1", "3", null))
        .row(Row.of(null, "2", "2", null))
        .build();

    given(tableInfoFactory.create(anyList(), anyList())).willReturn(tableFixture());

    impl = new DeMarchi(tableInfoFactory);
  }

  @Test
  void runDeMarchi() throws Exception {

    impl.execute(getConfiguration());

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(3)
        .contains(expectedInd());
  }

  @Test
  void runDeMarchiProcessEmptyColumns() throws Exception {
    final Configuration configuration = getConfiguration();
    configuration.setProcessEmptyColumns(true);

    impl.execute(configuration);

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(6)
        .contains(emptyColumnOnLhs());
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

  private InclusionDependency expectedInd() {
    return InclusionDependencyBuilder.dependent().column(TABLE_NAME, "b")
        .referenced().column(TABLE_NAME, "c").build();
  }

  private InclusionDependency emptyColumnOnLhs() {
    return InclusionDependencyBuilder.dependent().column(TABLE_NAME, "d")
        .referenced().column(TABLE_NAME, "a").build();
  }
}
