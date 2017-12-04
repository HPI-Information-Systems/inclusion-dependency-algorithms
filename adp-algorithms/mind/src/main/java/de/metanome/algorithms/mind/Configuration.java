package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class Configuration {

  private final InclusionDependencyResultReceiver resultReceiver;

}
