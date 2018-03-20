package de.metanome.algorithms.sindd;

import static org.assertj.core.api.Java6Assertions.assertThat;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import de.metanome.algorithms.sindd.util.FileUtil;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test CSV consistency in case of different CSV escape characters and in presence of different control
 * characters such "synchronous idle" (non-printable characters with very low code point).
 */
class CsvConsistencyTest {

  private File file;

  @BeforeEach
  void setUp() throws Exception {
    file = File.createTempFile("sindd", "temp");
    file.deleteOnExit();
  }

  @Test
  void csvReadEqualsWritten() throws Exception {
    final String[][] value = {
        {"\u0016Personal CD (some details from http://www.amazon.com/Whiskey-Icarus-DVD-Kyle-Kinane/dp/B00AIZ2DMA/ref=sr_1_2?ie=UTF8&qid=1360623801&sr=8-2&keywords=whiskey+icarus).",
            "4"},
        {"honorifics are set off with comma, and come after first name", "4"},
        {"\u00162306056", "\\x16", "8931607", "From \"CD booklet.",
            "2008-06-16 00:41:33.845054+00"},
        {"2306055", "374018", "8931603",
            "A VA classical release, compiled from a couple of recordings. I've made my best attempt at CSG, but it may well need further attention. Not that Britten indicated his variations with letters, not numbers, which I have preserved here. ",
            "2008-06-16 00:40:30.8462+00"}
    };
    final CSVWriter writer = FileUtil.createWriter(file);
    for (final String[] v : value) {
      writer.writeNext(v);
    }
    writer.close();

    Files.lines(file.toPath()).forEach(System.out::println);

    final CSVReader reader = FileUtil.createReader(file);
    final List<String[]> read = reader.readAll();
    reader.close();

    assertThat(read).hasSameSizeAs(value);
    for (int index = 0; index < value.length; ++index) {
      assertThat(value[index]).isEqualTo(read.get(index));
    }
  }

}
