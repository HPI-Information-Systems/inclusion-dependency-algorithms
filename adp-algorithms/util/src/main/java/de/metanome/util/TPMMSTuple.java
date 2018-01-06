package de.metanome.util;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class TPMMSTuple implements Comparable<TPMMSTuple> {

  private String value;
  private final int readerNumber;

  TPMMSTuple(final String value, final int readerNumber) {
    this.value = value;
    this.readerNumber = readerNumber;
  }

  @Override
  public int compareTo(TPMMSTuple other) {
    return this.value.compareTo(other.value);
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TPMMSTuple)) {
      return false;
    }
    TPMMSTuple other = (TPMMSTuple) obj;
    return this.value.equals(other.value);
  }

  @Override
  public String toString() {
    return "Tuple(" + this.value + "," + this.readerNumber + ")";
  }
}