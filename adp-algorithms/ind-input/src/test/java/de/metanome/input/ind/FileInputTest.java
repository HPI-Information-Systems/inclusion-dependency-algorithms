package de.metanome.input.ind;

import static org.assertj.core.api.Assertions.assertThat;

import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.util.InclusionDependencyBuilder;
import java.util.List;
import org.junit.jupiter.api.Test;

class FileInputTest {

  @Test
  void readExampleUnaryInput() {
    final InclusionDependencyParameters parameters = getParameters("2018-01-18_13-21-37_inds", 1);
    final FileInput input = new FileInput(parameters);

    final List<InclusionDependency> result = input.execute();

    assertThat(result)
        .hasSize(10)
        .contains(sampleInd());
  }

  @Test
  void readExampleNaryInput() {
    final InclusionDependencyParameters parameters = getParameters("2018-01-18_13-21-37_inds", -1);
    final FileInput input = new FileInput(parameters);

    final List<InclusionDependency> result = input.execute();

    assertThat(result)
        .hasSize(11)
        .contains(sampleNary());
  }

  private InclusionDependencyParameters getParameters(final String inputFile, final int maxDepth) {
    final InclusionDependencyParameters parameters = new InclusionDependencyParameters();
    parameters.setConfigurationString("inputPath=" + getClass().getResource(inputFile).getFile() +
        ",maxDepth=" + maxDepth);
    return parameters;
  }

  private InclusionDependency sampleInd() {
    return InclusionDependencyBuilder.dependent().column("CLASSIFICATION.csv", "column5")
        .referenced().column("DESCRIPTION.csv", "column1").build();
  }

  private InclusionDependency sampleNary() {
    return new InclusionDependencyBuilder()
        .dependent()
        .column("CLASSIFICATION.csv", "column6")
        .column("CLASSIFICATION.csv", "column7")

        .referenced()
        .column("HIERARCHIE.csv", "column2")
        .column("HIERARCHIE.csv", "column3")
        .build();
  }
}
