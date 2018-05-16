# Evaluation Framework

## Environment Variables

Below environment variables are available to the evaluation scripts.
This represents a filtered list - some of the variables exposed by `evaluation_env.sh` are only for
internal usage.

```
ADP_LIB         All required JARs for executing the algorithm
ALGORITHMS      Top-level algorithm directory
JVM_ARGS        Additional JVM arguments - like available heap
DEBUG           Configuration for remote debugging
DB              Metanome CLI parameters for establishing a database connection
DATASET			Identifier of the current dataset
$@              The ID of the run - probably a running counter
```

## Template

Evaluation scripts should roughly look like the following:

```
#!/bin/bash
export EXECUTION_ID=$DATASET"_my-algorithm"$@

java $DEBUG $JVM_ARGS \
-cp $ADP_LIB:$ALGORITHMS/my-algorithm/build/libs/my-algorithm-0.1.0-SNAPSHOT.jar \
de.metanome.cli.App \
--algorithm de.metanome.algorithms.my.MyAlgorithm \
$DB \
--table-key TABLE \
--tables load:$DATASET/$DATASET.txt 
--algorithm-config KEY:VALUE \
--output file:$EXECUTION_ID
```

`--tables` should be prefixed with `load:` and should contain a path pointing to a text file with
one column name on each line. The dataset directory should be part of the path since the working
directory will be `evaluation`.  
The identifier given in the `EXECUTION_ID` environment variable should uniquely identify the
execution since the output directory - `results/` - is hardcoded inside Metanome.

## Launching

Please do not forget to enter the actual database credentials to `pgpass_$DATASET.conf`.

### Simple
Below listing contains all necessary steps to create and execute an evaluation script of the *DeMarchi* algorithm
with the *SCOP* dataset.

<pre>
adp-algorithms$ ./gradlew assemble
adp-algorithms$ cd evaluation
$ vi ./demarchi.sh       # Create evaluation script
$ chmod +x demarchi.sh   # Make executable

# Below will
# - load relations from "scop/scop.txt",
# - obtain connection from "pgpass_scop.conf", and
# - prefix the output file
$ export DATASET=scop	

$ source evaluation_env.sh    # Load environment; make sure that $DATASET is present
$ ./demarchi.sh          	  # Execute
$ wc -l results/*             # Check IND count; check presence of logfile
   <i>  39 results/scop_demarchi_inds
      9 results/scop_demarchi.log</i>
</pre>

### Multiple Runs

Certainly a rough edge, but this would work with above template:

    seq 3 | xargs -Irun scop/demarchi.sh run
