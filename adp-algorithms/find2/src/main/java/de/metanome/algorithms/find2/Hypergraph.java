package de.metanome.algorithms.find2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Collectors;

class Hypergraph {
  private int k;
  private HashSet<ExIND> karies;
  private HashSet<ExIND> unaries;

  Hypergraph(int k, HashSet<ExIND> unaries, HashSet<ExIND> karies) {
    this.k = k;
    this.unaries = unaries;
    this.karies = karies;
  }

  HashSet<ExIND> getCliquesOfCurrentLevel() {
    HashSet<ExIND> cliques = new HashSet<>();
    boolean reducible;

    do {
      reducible = false;
      HashSet<ExIND> multipleCliqueEdges = new HashSet<>();

      for (ExIND kary : karies) {
        HashSet<ExIND> cliqueCandidate = generateCliqueCandidate(kary);
        if (validateCandidate(cliqueCandidate)) {
          ExIND validClique = ExIND.toExIND(cliqueCandidate);
          if (cliques.stream().noneMatch(validMaxClique -> validMaxClique.contains(validClique)))
            cliques.add(validClique);
          reducible = true;
        } else {
          multipleCliqueEdges.add(kary);
        }
      }
      if (!reducible) {
        HashSet<ExIND> cliqueCandidate;
        Iterator iterKaries = karies.iterator();
        Hypergraph G1, G2;

        // removal of disconnected nodes(!)
        unaries =
            unaries
                .stream()
                .filter(unary -> karies.stream().anyMatch(k -> k.contains(unary)))
                .collect(Collectors.toCollection(HashSet::new));

        do {
          // System.out.println("Warning: This graph is apparently not reducible");
          ExIND kary = (ExIND) iterKaries.next();
          cliqueCandidate = generateCliqueCandidate(kary);

          final HashSet<ExIND> cC = cliqueCandidate; // has to be final or effective final in lambda
          HashSet<ExIND> inducedEdges =
              karies
                  .stream()
                  .filter(k -> cC.containsAll(k.getAllUnaries()))
                  .collect(Collectors.toCollection(HashSet::new));

          HashSet<ExIND> otherEdges = new HashSet<>(karies);
          otherEdges.removeAll(inducedEdges);

          G1 = new Hypergraph(k, cliqueCandidate, inducedEdges);
          G2 = new Hypergraph(k, unaries, otherEdges);
        } while (cliqueCandidate.size() == unaries.size());
        cliques.addAll(G1.getCliquesOfCurrentLevel());
        cliques.addAll(G2.getCliquesOfCurrentLevel());
      }

      this.karies = multipleCliqueEdges;
    } while (!this.karies.isEmpty() && reducible);
    return cliques;
  }

  /*
  public int getK() {
    return this.k;
  }

  private boolean verifyUniformity() {
    for (ExIND kary : karies) if (kary.size() != this.k) return false;
    return true;
  }
  */

  private HashSet<ExIND> generateCliqueCandidate(ExIND kary) {
    HashSet<ExIND> cliqueCandidate = new HashSet<>();
    HashSet<ExIND> otherUnaries = new HashSet<>(this.unaries);
    cliqueCandidate.addAll(kary.getAllUnaries());
    otherUnaries.removeAll(kary.getAllUnaries());

    for (ExIND otherUnary : otherUnaries) {
      boolean addThis = true;

      // create necessaryIND with otherUnary. If it pass all, add otherUnary to cliqueCandidate.
      for (ExIND innerUnary : kary.getAllUnaries()) {
        HashSet<ExIND> necessaryUnaries = new HashSet<>(kary.getAllUnaries());
        necessaryUnaries.remove(innerUnary);
        necessaryUnaries.add(otherUnary);
        ExIND necessaryIND = ExIND.toExIND(necessaryUnaries);

        if (!this.karies.contains(necessaryIND)) {
          addThis = false;
          break;
        }
      }
      if (addThis) cliqueCandidate.add(otherUnary);
    }
    return cliqueCandidate;
  }

  private boolean validateCandidate(HashSet<ExIND> cliqueCandidate) {
    HashMap<ExIND, Integer> map = new HashMap<>();
    for (ExIND kary : karies)
      if (cliqueCandidate.containsAll(kary.getAllUnaries()))
        for (ExIND unary : kary.getAllUnaries()) map.put(unary, map.getOrDefault(unary, 0) + 1);
    return new HashSet<>(map.values()).size() == 1;
  }
}
