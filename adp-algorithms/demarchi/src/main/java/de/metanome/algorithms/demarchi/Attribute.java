package de.metanome.algorithms.demarchi;

import de.metanome.algorithm_integration.input.TableInputGenerator;
import lombok.Data;

@Data
class Attribute {

  private final int id;
  private final String tableName;
  private final String name;
  private final TableInputGenerator generator;
}
