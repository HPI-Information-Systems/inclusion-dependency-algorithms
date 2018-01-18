package de.metanome.algorithms.sindd.util;

public class TimeUtil {

  public static int getMinutes(long ms) {
    return getSeconds(ms) / 60;
  }

  public static int getSeconds(long ms) {
    return (int) (ms / 1000);
  }

  public static int getRemainingSeconds(long ms) {
    return getSeconds(ms) % 60;
  }

  public static int getRemainingMillisecond(long ms) {
    return (int) (ms % 1000);
  }

  public static String toString(long ms) {
    StringBuffer sb = new StringBuffer();
    sb.append(getMinutes(ms));
    sb.append(" m, ");
    sb.append(getRemainingSeconds(ms));
    sb.append(" s and ");
    sb.append(getRemainingMillisecond(ms));
    sb.append(" ms");
    return sb.toString();
  }
}
