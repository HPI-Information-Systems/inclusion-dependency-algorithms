package de.metanome.validation;

import de.metanome.algorithm_integration.results.InclusionDependency;

public interface ValidationStrategy extends AutoCloseable {

  ValidationResult validate(InclusionDependency toCheck);

  @Override
  void close();
}