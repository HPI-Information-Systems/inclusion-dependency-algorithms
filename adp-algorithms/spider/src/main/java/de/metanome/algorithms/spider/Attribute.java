package de.metanome.algorithms.spider;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import lombok.Data;

@Data
class Attribute {

  private final int id;
  private final String tableName;
  private final String columnName;
  private final IntSet referenced;
  private final IntSet dependent;
  private final ReadPointer readPointer;

  private String currentValue;

  Attribute(final int id, final String tableName, final String columnName,
      final ReadPointer readPointer) {
    this.id = id;
    this.readPointer = readPointer;
    this.tableName = tableName;
    this.columnName = columnName;
    dependent = new IntLinkedOpenHashSet();
    referenced = new IntLinkedOpenHashSet();

    if (readPointer.hasNext()) {
      currentValue = readPointer.next();
    }
  }

  void addDependent(final IntSet dependent) {
    this.dependent.addAll(dependent);
  }

  void removeDependent(final int dependent) {
    this.dependent.remove(dependent);
  }

  void addReferenced(final IntSet referenced) {
    this.referenced.addAll(referenced);
  }

  void removeReferenced(final int referenced) {
    this.referenced.remove(referenced);
  }

  void nextValue() {
    if (readPointer.hasNext()) {
      currentValue = readPointer.next();
    } else {
      currentValue = null;
    }
  }

  void intersectReferenced(final IntSet attributes, final Int2ObjectMap<Attribute> attributeIndex) {
    final IntIterator referencedIterator = referenced.iterator();
    while (referencedIterator.hasNext()) {
      final int ref = referencedIterator.nextInt();
      if (attributes.contains(ref)) {
        continue;
      }

      referencedIterator.remove();
      attributeIndex.get(ref).removeDependent(id);
    }
  }

  boolean isFinished() {
    return !readPointer.hasNext() || (referenced.isEmpty() && dependent.isEmpty());
  }

  void close() throws IOException {
    readPointer.close();
  }
}
