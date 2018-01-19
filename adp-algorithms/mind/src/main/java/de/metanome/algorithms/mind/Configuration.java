package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.util.TPMMSConfiguration;
import de.metanome.validation.ValidationParameters;

import java.util.Collections;
import java.util.List;

import lombok.Data;
import lombok.Builder;
import lombok.Singular;


@Data
@Builder
public class Configuration {

  private InclusionDependencyResultReceiver resultReceiver;
  private ValidationParameters validationParameters;

  @Singular
  private List<TableInputGenerator> tableInputGenerators;

  private int maxDepth;

  public static Configuration withDefaults() {
    return builder()
        .resultReceiver(null)
        .maxDepth(-1)
        .tableInputGenerators(Collections.emptyList())
        .build();
  }
}
