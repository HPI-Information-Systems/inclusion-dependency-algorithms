package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.input.ind.InclusionDependencyParameters;
import de.metanome.validation.ValidationParameters;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
class FIND2Configuration {

  private final InclusionDependencyResultReceiver resultReceiver;
  private final ValidationParameters validationParameters;
  private final InclusionDependencyParameters inclusionDependencyParameters;
  private final int startK;
}
