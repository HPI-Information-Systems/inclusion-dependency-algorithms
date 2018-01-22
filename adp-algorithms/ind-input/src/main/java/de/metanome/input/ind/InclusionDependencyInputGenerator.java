package de.metanome.input.ind;

public class InclusionDependencyInputGenerator {

  private final DeMarchiInput deMarchi;
  private final SpiderInput spider;
  private final MindInput mind;


  public InclusionDependencyInputGenerator() {
    deMarchi = new DeMarchiInput();
    spider = new SpiderInput();
    mind = new MindInput();
  }

  public InclusionDependencyInput get(final InclusionDependencyParameters parameters) {
    switch (parameters.getAlgorithmType()) {
      case DE_MARCHI:
        return () -> deMarchi.execute(parameters);
      case SPIDER:
        return () -> spider.execute(parameters);
      case MIND:
        return () -> mind.execute(parameters);
      default:
        throw new IllegalArgumentException(
            "unknown algorithm type " + parameters.getAlgorithmType());
    }
  }
}
