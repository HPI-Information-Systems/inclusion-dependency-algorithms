package de.metanome.util;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableInfo {

  private final String tableName;
  private final RelationalInputGenerator relationalInputGenerator;
  private final TableInputGenerator tableInputGenerator;
  private final List<String> columnNames;
  private final List<String> columnTypes;

  public List<String> getColumnTypes() {
    return columnTypes;
  }

  public String getTableName() {
    return tableName;
  }

  public RelationalInputGenerator getRelationalInputGenerator() {
    return relationalInputGenerator;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public TableInputGenerator getTableInputGenerator() {
    return tableInputGenerator;
  }

  public int getColumnCount() {
    return columnNames.size();
  }

  public RelationalInputGenerator selectInputGenerator() {
    return relationalInputGenerator == null ? tableInputGenerator : relationalInputGenerator;
  }

}