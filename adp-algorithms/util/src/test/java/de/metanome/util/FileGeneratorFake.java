package de.metanome.util;

import de.metanome.algorithm_integration.algorithm_execution.FileCreationException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileGeneratorFake implements FileGenerator {

  @Override
  public File getTemporaryFile() throws FileCreationException {
    try {
      final File file = Files.createTempFile(getClass().getSimpleName(), "tmp").toFile();
      file.deleteOnExit();
      return file;
    } catch (final IOException e) {
      throw new FileCreationException("failed to create file", e);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
