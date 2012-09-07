###########################
#
# Executes the HBASE BulkLoad
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


# check envvars which might override default args
# CLASSPATH initially contains $CONF_DIR
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar


# add libs to CLASSPATH.
CLASSPATH="${CLASSPATH}:$HOME/lib/*"


CLASSPATH="$CONF_DIR:$CONF_DIR/META-INF:$CLASSPATH"

HADOOP_CLASSPATH="$CLASSPATH" hadoop jar $HOME/lib/hbase-0.90.4.jar completebulkload $@

