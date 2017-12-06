package de.metanome.algorithms.zigzag.configuration;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
public class ZigzagConfiguration {

  private final InclusionDependencyResultReceiver resultReceiver;
  private final Integer k;
  private final Integer epsilon;

  @Singular
  private final List<TableInputGenerator> tableInputGenerators;
  @Singular
  private final List<RelationalInputGenerator> relationalInputGenerators;
}
