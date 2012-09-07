###########################
#
# Executes the HBaseImport client
#!/usr/bin/env bash


abspath=$(cd ${0%/*} && echo $PWD/${0##*/})
BIN_HOME=`dirname $abspath`

export HOME=$BIN_HOME/../

#source environment variables

JAVASH="/etc/profile.d/java.sh"
[ -f $JAVASH ] && . $JAVASH

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
    #echo "run java in $JAVA_HOME"
   JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
     echo "Error: JAVA_HOME is not set."
     exit 1
fi

JAVA=$JAVA_HOME/bin/java


if [ -z $JAVA_HEAP ]; then
 export JAVA_HEAP="-Xmx1024m"
fi

# check envvars which might override default args
# CLASSPATH initially contains $CONF_DIR
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar


# add libs to CLASSPATH.
CLASSPATH="${CLASSPATH}:$HOME/lib/*"


CLASSPATH="$CONF_DIR:$CONF_DIR/META-INF:$CLASSPATH"
DIR=$1
TBL=$2

for size in $(hadoop fs -lsr "$DIR" | awk '{print $5}')
do

 #if the file is larger than 3 gigs refuse to import
 #doing otherwise would cause hbase to fail
 echo "checking size $size"
 if [[ "$size" -gt 3221225472 ]]; then

   echo "One or more region sizes exeed 3 gigabytes in size: $size > 3221225472"
   exit -1;
 fi

done



HADOOP_CLASSPATH="$CLASSPATH" hadoop jar $HOME/lib/hbase-0.90.4.jar completebulkload $DIR $TBL
#exec "$JAVA" -XX:+DisableExplicitGC $JAVA_HEAP $JAVA_OPTS -classpath "$CLASSPATH" $CLASS $@

