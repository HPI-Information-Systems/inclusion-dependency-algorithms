package de.metanome.algorithms.demarchi;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import de.metanome.util.RelationalInputGeneratorStub;
import de.metanome.util.Row;
import de.metanome.util.TableInfo;
import de.metanome.util.TableInfoFactory;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DeMarchiMultiRelationTest {

  private static final String TABLE_NAME_X = "tableX";
  private static final String TABLE_NAME_Y = "tableY";

  @Mock
  private TableInfoFactory tableInfoFactory;
  @Mock
  private InclusionDependencyResultReceiver resultReceiver;
  @Captor
  private ArgumentCaptor<InclusionDependency> ind;

  private RelationalInputGenerator generatorT1;
  private RelationalInputGenerator generatorT2;
  private DeMarchi impl;

  private List<String> columnNames;
  private List<String> columnTypes;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    columnNames = asList("a", "b", "c", "d", "e", "f", "g");
    columnTypes = Stream.generate(() -> "str").limit(columnNames.size())
        .collect(Collectors.toList());
    generatorT1 = RelationalInputGeneratorStub.builder()
        .relationName(TABLE_NAME_X)
        .columnNames(columnNames)
        .row(Row.of("1", "2", "3", "4", "5", "6", "7"))
        .row(Row.of("8", "9", "10", "11", "12", "13", "14"))
        .build();

    generatorT2 = RelationalInputGeneratorStub.builder()
        .relationName(TABLE_NAME_Y)
        .columnNames(columnNames)
        .row(Row.of("1", "2", "3", "4", "5", "0", "0"))
        .row(Row.of("0", "0", "0", "4", "0", "6", "7"))

        .row(Row.of("0", "0", "0", "0", "5", "6", "0"))
        .row(Row.of("0", "0", "0", "0", "5", "0", "7"))
        .row(Row.of("0", "0", "0", "0", "0", "6", "7"))

        .row(Row.of("8", "9", "10", "11", "12", "13", "14"))

        .build();

    given(tableInfoFactory.create(anyList(), anyList())).willReturn(tableFixture());

    impl = new DeMarchi(tableInfoFactory);
  }

  @Test
  void runDeMarchi() throws Exception {

    impl.execute(getConfiguration());

    verify(resultReceiver, atLeastOnce()).receiveResult(ind.capture());
    assertThat(ind.getAllValues())
        .hasSize(7)
        .satisfies(this::expectedProperty);
  }

  private void expectedProperty(final List<? extends InclusionDependency> inds) {
    final List<String> lhs = inds.stream()
        .flatMap(ind -> ind.getDependant().getColumnIdentifiers().stream())
        .map(ColumnIdentifier::getTableIdentifier)
        .collect(Collectors.toList());
    assertThat(lhs).containsOnly(TABLE_NAME_X);

    final List<String> rhs = inds.stream()
        .flatMap(ind -> ind.getReferenced().getColumnIdentifiers().stream())
        .map(ColumnIdentifier::getTableIdentifier)
        .collect(Collectors.toList());
    assertThat(rhs).containsOnly(TABLE_NAME_Y);
  }

  private List<TableInfo> tableFixture() {
    return asList(TableInfo.builder().relationalInputGenerator(generatorT1)
            .tableName(TABLE_NAME_X)
            .columnNames(columnNames)
            .columnTypes(columnTypes)
            .build(),

        TableInfo.builder().relationalInputGenerator(generatorT2)
            .tableName(TABLE_NAME_Y)
            .columnNames(columnNames)
            .columnTypes(columnTypes)
            .build());
  }

  private Configuration getConfiguration() {
    return Configuration.builder()
        .relationalInputGenerator(generatorT1)
        .relationalInputGenerator(generatorT2)
        .resultReceiver(resultReceiver)
        .build();
  }
}
