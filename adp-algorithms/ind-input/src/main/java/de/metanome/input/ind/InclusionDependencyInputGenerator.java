package de.metanome.input.ind;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;

public class InclusionDependencyInputGenerator {

  @FunctionalInterface
  interface InputFactory {

    InclusionDependencyInput create(InclusionDependencyParameters parameters);
  }

  private final Map<AlgorithmType, InputFactory> inputs;

  public InclusionDependencyInputGenerator() {
    inputs = ImmutableMap.<AlgorithmType, InputFactory>builder()
        .put(AlgorithmType.DE_MARCHI, DeMarchiInput::new)
        .put(AlgorithmType.SPIDER, SpiderInput::new)
        .put(AlgorithmType.FILE, FileInput::new)
        .put(AlgorithmType.SQL, UnarySQLInput::new)
        .build();
  }

  public InclusionDependencyInput get(final InclusionDependencyParameters parameters) {
    final InputFactory inputFactory = inputs.get(parameters.getAlgorithmType());
    Objects.requireNonNull(inputFactory,
        "no input configured for type " + parameters.getAlgorithmType());
    return inputFactory.create(parameters);
  }
}
