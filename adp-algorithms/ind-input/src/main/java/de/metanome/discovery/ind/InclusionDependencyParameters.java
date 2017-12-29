package de.metanome.discovery.ind;

import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
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
public class InclusionDependencyParameters {

  private AlgorithmType algorithmType;
  private String configurationString;

  @Singular
  private List<RelationalInputGenerator> relationalInputGenerators;
  @Singular
  private List<TableInputGenerator> tableInputGenerators;

}
