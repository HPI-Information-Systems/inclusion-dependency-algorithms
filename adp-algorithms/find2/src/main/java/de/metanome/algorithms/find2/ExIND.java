package de.metanome.algorithms.find2;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.results.InclusionDependency;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

class ExIND extends InclusionDependency {

  private Boolean valid = null;

  ExIND(ColumnPermutation columnPermutation, ColumnPermutation columnPermutation1) {
    super(columnPermutation, columnPermutation1);
  }

  static ExIND toExIND(HashSet<ExIND> necessaryUnaries) {
    List<ColumnIdentifier> dep = new ArrayList<>();
    List<ColumnIdentifier> ref = new ArrayList<>();
    for (ExIND e : necessaryUnaries) {
      dep.addAll(e.getDependant().getColumnIdentifiers());
      ref.addAll(e.getReferenced().getColumnIdentifiers());
    }

    return new ExIND(
        new ColumnPermutation(dep.toArray(new ColumnIdentifier[dep.size()])),
        new ColumnPermutation(ref.toArray(new ColumnIdentifier[ref.size()])));
  }

  HashSet<ExIND> getAllUnaries() {
    List<ColumnIdentifier> dep = this.getDependant().getColumnIdentifiers();
    List<ColumnIdentifier> ref = this.getReferenced().getColumnIdentifiers();
    HashSet<ExIND> result = new HashSet<>();

    for (int i = 0; i < dep.size(); i++)
      result.add(new ExIND(new ColumnPermutation(dep.get(i)), new ColumnPermutation(ref.get(i))));
    return result;
  }

  boolean contains(ExIND other) {
    List<ColumnIdentifier> thisDep = this.getDependant().getColumnIdentifiers();
    List<ColumnIdentifier> otherDep = other.getDependant().getColumnIdentifiers();
    List<ColumnIdentifier> thisRef = this.getReferenced().getColumnIdentifiers();
    List<ColumnIdentifier> otherRef = other.getReferenced().getColumnIdentifiers();

    boolean contained = true;
    for (int i = 0; i < otherDep.size() && contained; i++) {
      contained = false;
      for (int j = 0; j < thisDep.size(); j++)
        if (thisDep.get(j).equals(otherDep.get(i)))
          if (thisRef.get(j).equals(otherRef.get(i))) {
            contained = true;
            break;
          }
    }
    return contained;
  }

  int size() {
    return this.getDependant().getColumnIdentifiers().size();
  }

  @Override
  public int hashCode() {
    return getDependant().getColumnIdentifiers().size();
  }

  // Beware: cannot compare ExIND with InclusionDependency and vice versa
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (getDependant().getColumnIdentifiers().size() <= 1) return super.equals(obj);

    ExIND other = (ExIND) obj;
    HashSet<ExIND> thisUnaries = getAllUnaries();
    HashSet<ExIND> otherUnaries = other.getAllUnaries();
    return thisUnaries.equals(otherUnaries);
  }

  Boolean isValid() {
    return valid;
  }

  void setValidity(boolean validity) {
    this.valid = validity;
  }
}
