package de.metanome.algorithms.demarchi;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class Attribute {

  private final int id;
  private final String tableName;
  private final int columnOffset;
  private final String name;
  private final String type;
  private final RelationalInputGenerator generator;
}
