package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.input.ind.InclusionDependencyParameters;
import de.metanome.validation.ValidationParameters;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;


@Data
@Builder
public class Configuration {

  private InclusionDependencyResultReceiver resultReceiver;
  private InclusionDependencyParameters inclusionDependencyParameters;
  private ValidationParameters validationParameters;

  @Singular
  private List<TableInputGenerator> tableInputGenerators;

  private int maxDepth;
  private boolean outputMaxInd;

  public static Configuration withDefaults() {
    return builder()
        .resultReceiver(null)
        .maxDepth(-1)
        .tableInputGenerators(Collections.emptyList())
        .outputMaxInd(true)
        .build();
  }
}
