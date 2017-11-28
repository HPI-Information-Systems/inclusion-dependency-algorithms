SPIDER
======

## Improvements over the reference implementation

1. Fixed I/O bug in TPMMS merge phase which sneaked nulls into the dataset in case duplicate entries
  were spread across more than one partition.
  In the original `TPMMS.java:139` newlines are generate despite the fact that `tuple.value` has
  never been written (l. 130) (since skipped due to a detected duplicate).

2. Previously the relation was scanned for each attribute individually. Now the relation is scanned
  in one pass while writing all attribute files simultaneously.
  This trades *n* scans of the relation (*n* begin the number of attributes) for one additional pass
  during disk-based TPMMS (since the attribute files are initially unsorted, which would be at least
  the case for relational inputs anyway).
