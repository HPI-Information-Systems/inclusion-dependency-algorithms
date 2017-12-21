package de.metanome.algorithms.demarchi;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.Row;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import de.metanome.util.TableInputGeneratorStub;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DeMarchiTest {

  private static final String TABLE_NAME = "test";

  @Mock
  private TableInfoFactory tableInfoFactory;
  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private TableInputGenerator generator;
  private DeMarchi impl;

  private List<String> columnNames;
  private List<String> columnTypes;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);

    columnNames = asList("a", "b", "c");
    columnTypes = asList("str", "int", "int");
    generator = TableInputGeneratorStub.builder()
        .relationName("Test")
        .columnNames(columnNames)
        .row(Row.of("1", "1", "1"))
        .row(Row.of("1", "1", "3"))
        .row(Row.of(null, "2", "2"))
        .build();

    impl = new DeMarchi(tableInfoFactory);
  }

  @Test
  void runDeMarchi() throws Exception {
    given(tableInfoFactory.createFromTableInputs(anyList())).willReturn(tableFixture());

    impl.execute(getConfiguration());

    verify(resultReceiver).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(1)
        .first()
        .isEqualTo(expectedInd());
  }

  private List<TableInfo> tableFixture() {
    return asList(TableInfo.builder().tableInputGenerator(generator)
        .tableName(TABLE_NAME)
        .columnNames(columnNames)
        .columnTypes(columnTypes)
        .build());
  }

  private Configuration getConfiguration() {
    return Configuration.builder().tableInputGenerator(generator).resultReceiver(resultReceiver)
        .build();
  }

  private InclusionDependency expectedInd() {
    final ColumnIdentifier lhs = new ColumnIdentifier(TABLE_NAME, "b");
    final ColumnIdentifier rhs = new ColumnIdentifier(TABLE_NAME, "c");
    return new InclusionDependency(new ColumnPermutation(lhs), new ColumnPermutation(rhs));
  }
}
