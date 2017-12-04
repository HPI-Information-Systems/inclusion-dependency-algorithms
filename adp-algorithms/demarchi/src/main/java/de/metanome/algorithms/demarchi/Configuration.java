package de.metanome.algorithms.demarchi;

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
  private final InclusionDependencyResultReceiver resultReceiver;

}
