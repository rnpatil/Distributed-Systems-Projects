if [ $# != 5 ] ; then
	echo "USAGE: $0 <numberOfCore> <input> <output> <damping factor> <maxIterations>"
        echo " e.g.: $0  2 ../pagerank.input ../output.txt 0.85 10"
        exit 1;
fi


javac -cp .:./lib/mpj.jar src/MPIPageRank/TestMPIPageRank.java -d bin/
cd bin
mpjrun.sh -np $1 MPIPageRank.TestMPIPageRank $2 $3 $4 $5
