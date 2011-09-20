#!/bin/sh
hadoop jar \
/usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar \
-libjars target/HadoopInPracticeCode-1.0-SNAPSHOT-jar-with-dependencies.jar \
-input /test-data/ch1/moby-dick.txt \
-output /test-output/ch1 \
-mapper com.manning.hip.ch1.Map \
-reducer com.manning.hip.ch1.Reduce \
-verbose \
