Author: Ethan(Meng) Li, Rohit Patil

To compile:

In the current directory, run:
javac -d bin src/PageRank/SequentialPageRank.java 

To run:

cd ./bin

java PageRank.SequentialPageRank ../pagerank.input.1000.urls.19 ../li526_SequentialPageRank_output.txt 100 0.85


Then you can find the output file in the father directory of the "bin" directory. 


*The usage is:
java PageRank.SequentialPageRank [input file name] [output file name] [iteration count] [damping factor]
