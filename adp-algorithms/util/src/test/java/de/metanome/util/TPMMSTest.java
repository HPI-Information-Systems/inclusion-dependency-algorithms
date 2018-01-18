package de.metanome.util;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TPMMSTest {

  private Path directory;
  private Path toProcess;

  @BeforeEach
  void setUp() throws Exception {
    directory = Files.createTempDirectory("tpmms");
    toProcess = directory.resolve("toProcess.txt");
  }

  @AfterEach
  void tearDown() throws Exception {
    for (final Path path : Files.list(directory).collect(toList())) {
      Files.delete(path);
    }
    Files.delete(directory);
  }

  private void createFixture(final List<String> items) throws Exception {

    try (BufferedWriter writer = Files.newBufferedWriter(toProcess)) {
      for (final String item : items) {
        writer.write(String.valueOf(item));
        writer.newLine();
      }
    }
  }

  @Test
  void testUniqueAndSort() throws Exception {
    final TPMMSConfiguration configuration = getConfiguration();
    final List<String> items = randomized(withDuplicates(asList("4", "3", "2", "1")));
    createFixture(items);

    new TPMMS(configuration).uniqueAndSort(toProcess);

    final List<String> actual = Files.lines(toProcess).collect(toList());
    assertThat(actual).as("Input: " + items).isEqualTo(asList("1", "2", "3", "4"));
  }

  private TPMMSConfiguration getConfiguration() {
    return TPMMSConfiguration.builder()
        .maxMemoryUsagePercentage(1)
        .memoryCheckInterval(1)
        .build();
  }

  private List<String> withDuplicates(final List<String> values) {
    final List<String> result = new ArrayList<>(values);
    result.addAll(values);
    return result;
  }

  private List<String> randomized(final List<String> values) {
    final List<String> result = new ArrayList<>(values);
    Collections.shuffle(result);
    return result;
  }
}