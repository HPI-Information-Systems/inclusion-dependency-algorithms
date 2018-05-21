package de.metanome.algorithms.demarchi;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class Attribute {

  private final int id;
  private final String tableName;
  private final String name;
  private final String type;
  private final RelationalInputGenerator relationalInputGenerator;
  private final TableInputGenerator tableInputGenerator;

  public RelationalInputGenerator selectInputGenerator() {
    if (tableInputGenerator != null) {
      return tableInputGenerator;
    }
    return relationalInputGenerator;
  }
}
