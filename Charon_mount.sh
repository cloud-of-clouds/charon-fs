#!/bin/sh 

. ./build.conf

LD_LIBRARY_PATH=./jni:$FUSE_HOME/lib $JDK_HOME/bin/java -Xmx1024m -Duid=$(id -u) -Dgid=$(id -g) \
   -classpath bin:lib/*:lib/javaMail/mail.jar:lib/commons/*:lib/hdfs/*:lib/http/*:lib/depsky/*:lib/jackson/* \
       -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog \
   -Dfuse.logging.level=WARN \
  charon.general.Charon -f -s $1 $2 $3 $4 $5 $6 $7 $8


