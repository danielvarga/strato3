#/bin/bash -eu

#../stratosphere/stratosphere-dist/target/stratosphere-dist-0.4-SNAPSHOT-bin/stratosphere-0.4-SNAPSHOT/bin/pact-client.sh run -j wordcount.jar -c eu.stratosphere.pact.example.wordcount.WordCount -a 1 file:///home/zsfekete/hamlet.txt file:///home/zsfekete/wordcount_output1

#../stratosphere/stratosphere-dist/target/stratosphere-dist-0.4-SNAPSHOT-bin/stratosphere-0.4-SNAPSHOT/bin/pact-client.sh run -j wordcount.jar -c hu.strato3.AlsMain -a 1 file:///home/$USER/git/strato3/data/test_small.csv file:///home/zsfekete/git/strato3/out/out

../stratosphere/stratosphere-dist/target/stratosphere-dist-0.4-SNAPSHOT-bin/stratosphere-0.4-SNAPSHOT/bin/pact-client.sh run -j target/als-0.1-SNAPSHOT.jar -c hu.strato3.AlsMain -a 1 file:///home/$USER/git/strato3/data/test_small.csv file:///home/zsfekete/git/strato3/out/out 10 false 5


