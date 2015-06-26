#!/bin/bash

JAR_PATH=/Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home/bin/jar
JAVAC_PATH=/Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home/bin/javac

mkdir ./obj

$JAVAC_PATH -classpath "./:./resources/voltdb/*" \
	-d ./obj  \
	./src/org/yottabase/lastfm/adapter/voltdb/procedure/*.java

$JAR_PATH cvf ./resources/voltdbProcedure.jar -C ./obj .

rm -r ./obj

