package de.metanome.util;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TableInfoFactoryTest {


  @Mock
  private RelationalInputGenerator generator;

  private TableInfoFactory factory;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
    factory = new TableInfoFactory();
  }


  @Test
  void collectFromRelationalSource() throws Exception {
    final RelationalInput input1 = RelationalInputStub.builder()
        .columnName("A").columnName("B")
        .build();
    given(generator.generateNewCopy()).willReturn(input1);

    final List<TableInfo> info = factory.createFromRelationalInputs(asList(generator));

    assertThat(info).hasSize(1);
    final TableInfo info1 = info.get(0);
    assertThat(info1.getColumnNames()).contains("A", "B");
    assertThat(info1.getColumnTypes()).containsOnly("String");
  }


}