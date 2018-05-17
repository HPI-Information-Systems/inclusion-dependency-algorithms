package de.metanome.algorithms.spiderbruteforce;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class Attribute {

  private final int id;
  private final String table;
  private final String column;
  private final RelationalInputGenerator input;

}
