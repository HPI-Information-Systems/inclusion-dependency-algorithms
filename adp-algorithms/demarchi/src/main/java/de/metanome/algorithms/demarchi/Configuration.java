package de.metanome.algorithms.demarchi;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Configuration {

  @Singular
  private List<TableInputGenerator> tableInputGenerators;
  @Singular
  private List<RelationalInputGenerator> relationalInputGenerators;

  private boolean processEmptyColumns;
  private int inputRowLimit;

  private InclusionDependencyResultReceiver resultReceiver;

  public static Configuration withDefaults() {
    return builder().tableInputGenerators(Collections.emptyList())
        .relationalInputGenerators(Collections.emptyList())
        .processEmptyColumns(true)
        .inputRowLimit(-1)
        .resultReceiver(null)
        .build();
  }
}
