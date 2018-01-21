package de.metanome.algorithms.mind;

import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
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
  private ValidationParameters validationParameters;

  @Singular
  private List<TableInputGenerator> tableInputGenerators;

  private boolean processEmptyColumns;
  private int maxDepth;

  public static Configuration withDefaults() {
    return builder()
        .resultReceiver(null)
        .processEmptyColumns(false)
        .maxDepth(-1)
        .tableInputGenerators(Collections.emptyList())
        .build();
  }
}
