package de.metanome.util;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TPMMSConfiguration {

  private int inputRowLimit;
  private int maxMemoryUsagePercentage;
  private int memoryCheckInterval;

  public static TPMMSConfiguration withDefaults() {
    return builder()
        .inputRowLimit(-1)
        .maxMemoryUsagePercentage(50)
        .memoryCheckInterval(1000)
        .build();
  }


}
