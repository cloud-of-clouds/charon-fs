#!/bin/sh


# ./C2F2_mount.sh 'pasta para mount' 'id'
# args: 1 - nome do mount
#       2 - id utilizador

#para fazer dubug pelo eclipse: -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=8888,suspend=y 

. ./build.conf

LD_LIBRARY_PATH=./jni:$FUSE_HOME/lib $JDK_HOME/bin/java -Xmx1024m -Duid=$(id -u) -Dgid=$(id -g) \
   -classpath bin:lib/*:lib/javaMail/mail.jar:lib/commons/*:lib/hdfs/*:lib/http/*:lib/depsky/*:lib/jackson/* \
       -Dorg.apache.commons.logging.Log=fuse.logging.FuseLog \
   -Dfuse.logging.level=WARN \
  charon.general.Charon -f -s $1 $2 $3 $4 $5 $6 $7 $8


