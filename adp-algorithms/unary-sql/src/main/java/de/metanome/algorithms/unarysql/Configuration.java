package de.metanome.algorithms.unarysql;

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

  private boolean processEmptyColumns;

  @Singular
  private List<TableInputGenerator> tableInputGenerators;

  private ValidationParameters validationParameters;

  private InclusionDependencyResultReceiver resultReceiver;

  public static Configuration withDefaults() {
    return Configuration.builder()
        .processEmptyColumns(false)
        .tableInputGenerators(Collections.emptyList())
        .resultReceiver(null)
        .build();
  }
}
