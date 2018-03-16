package de.metanome.algorithms.zigzag.configuration;

public enum ConfigurationKey {
  TABLE,
  START_K, /* Number of levels to check with unary algorithm before calculating the optimistic border */
  EPSILON /* error margin to decide what candidates to check next */
}