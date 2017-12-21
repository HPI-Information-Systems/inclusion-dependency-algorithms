package de.metanome.algorithms.demarchi;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
class Configuration {

  @Singular
  private final List<TableInputGenerator> tableInputGenerators;
  @Singular
  private final List<RelationalInputGenerator> relationalInputGenerators;

  private final InclusionDependencyResultReceiver resultReceiver;

}
