package de.metanome.algorithms.zigzag;

import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.List;

public class Hypergraph {

  private List<InclusionDependency> uINDs;

  public Hypergraph(List<InclusionDependency> uINDs) {
    this.uINDs = uINDs;
    generate(uINDs);
  }
}
