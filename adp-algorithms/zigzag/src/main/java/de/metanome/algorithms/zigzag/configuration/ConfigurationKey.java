package de.metanome.algorithms.zigzag.configuration;

public enum ConfigurationKey {
  TABLE,
  DATABASE, /* Required for Validation Strategy */
  K, /* Number of levels to check with unary algorithm before calculating the optimistic border */
  EPSILON /* error margin to decide what candidates to check next */
}