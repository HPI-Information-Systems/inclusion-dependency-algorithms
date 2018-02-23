set -a
METANOME_CLI="metanome-cli-1.1.1-SNAPSHOT.jar"
POSTGRES="postgresql-42.2.1.jar"
ADP_LIB=$METANOME_CLI:$POSTGRES
ALGORITHMS=".."
JVM_ARGS="-Xmx83g -Dtinylog.configuration=tinylog-evaluation.properties"
DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
DB="--db-type postgresql --db-connection pgpass.conf"
set +a
