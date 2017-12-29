package de.metanome.discovery.ind;

public class InclusionDependencyInputGenerator {

  private final DeMarchiInput deMarchi;
  private final SpiderInput spider;

  public InclusionDependencyInputGenerator() {
    deMarchi = new DeMarchiInput();
    spider = new SpiderInput();
  }

  public InclusionDependencyInput get(final InclusionDependencyParameters parameters) {
    switch (parameters.getAlgorithmType()) {
      case DE_MARCHI:
        return () -> deMarchi.execute(parameters);
      case SPIDER:
        return () -> spider.execute(parameters);
      default:
        throw new IllegalArgumentException(
            "unknown algorithm type " + parameters.getAlgorithmType());
    }
  }
}
