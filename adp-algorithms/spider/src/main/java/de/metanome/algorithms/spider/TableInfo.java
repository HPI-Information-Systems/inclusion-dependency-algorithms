package de.metanome.algorithms.spider;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class TableInfo {

  private final String tableName;
  private final RelationalInputGenerator relationalInputGenerator;
  private final TableInputGenerator tableInputGenerator;
  private final List<String> columnNames;
  private final List<String> columnTypes;

  int getColumnCount() {
    return columnNames.size();
  }

  RelationalInputGenerator selectInputGenerator() {
    return relationalInputGenerator == null ? tableInputGenerator : relationalInputGenerator;
  }
}