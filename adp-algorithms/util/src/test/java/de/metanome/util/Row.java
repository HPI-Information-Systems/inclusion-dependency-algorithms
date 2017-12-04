package de.metanome.util;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Row {

  private final List<String> values;

  public static Row of(final String... values) {
    return new Row(Arrays.asList(values));
  }
}
