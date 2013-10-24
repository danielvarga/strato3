#!/bin/bash -eu

strato_dir="../stratosphere"

javac -cp "$strato_dir"/'stratosphere-dist/target/stratosphere-dist-0.4-SNAPSHOT-bin/stratosphere-0.4-SNAPSHOT/lib/*' src/main/java/hu/strato3/*.java 

jar cvf wordcount.jar -C src/main/java .

exit

