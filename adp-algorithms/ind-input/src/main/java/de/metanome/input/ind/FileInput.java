package de.metanome.input.ind;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

class FileInput implements InclusionDependencyInput {

  @Data
  public static class Configuration {

    private String inputPath;
    private int maxDepth;
  }

  private final InclusionDependencyParameters parameters;
  private final JsonFactory jsonFactory;
  private final ObjectMapper mapper;

  FileInput(final InclusionDependencyParameters parameters) {
    this.parameters = parameters;
    jsonFactory = new JsonFactory();
    mapper = new ObjectMapper();
  }

  @Override
  public List<InclusionDependency> execute() {
    final Configuration configuration = prepareConfiguration();
    validate(configuration);

    try {
      final List<InclusionDependency> inds = read(configuration);
      if (configuration.getMaxDepth() > 0) {
        return filterByLength(inds, configuration.getMaxDepth());
      } else {
        return inds;
      }
    } catch (final IOException e) {
      throw new RuntimeException("Cannot read INDs from " + configuration.getInputPath(), e);
    }
  }

  private Configuration prepareConfiguration() {
    final Configuration configuration = new Configuration();
    ConfigurationMapper.applyFrom(parameters.getConfigurationString(), configuration);
    return configuration;
  }

  private void validate(final Configuration configuration) {
    Preconditions.checkNotNull(configuration.getInputPath(), "IND input path not set");

    final File file = new File(configuration.getInputPath());
    Preconditions.checkState(file.exists(), "IND input path %s has to exist", file.getPath());
    Preconditions.checkState(file.isFile(), "IND input must be a file: %s ", file.getPath());
  }

  private List<InclusionDependency> read(final Configuration configuration) throws IOException {
    try (FileInputStream in = new FileInputStream(configuration.getInputPath())) {
      return mapper.readValues(jsonFactory.createParser(in), InclusionDependency.class).readAll();
    }
  }

  private List<InclusionDependency> filterByLength(final List<InclusionDependency> inds,
      final int maxDepth) {

    final List<InclusionDependency> result = new ArrayList<>(inds);
    result.removeIf(ind -> ind.getDependant().getColumnIdentifiers().size() > maxDepth);
    return result;
  }
}
