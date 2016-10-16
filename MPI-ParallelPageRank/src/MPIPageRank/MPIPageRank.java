package MPIPageRank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import mpi.MPI;

public class MPIPageRank {
	boolean debugMode = false;
	// adjacency matrix read from file
	private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();

	// local adjacency matrix to store per rank urls
	private HashMap<Integer, ArrayList<Integer>> localAdjMatrix = new HashMap<Integer, ArrayList<Integer>>();
	// input file name
	private String inputFile = "";
	// output file name
	private String outputFile = "";
	// number of iterations
	private int iterations = 10;
	// damping factor
	private double df = 0.85;
	// number of URLs
	private int size = 0;
	// calculating rank values
	//simply assume that the urls are 0,1,...,(size-1)
	double rankValues[];
	//number of ranks
	private int nRanks;
	//id of ranks
	private int rank;

	public MPIPageRank(String[] args){
		MPI.Init(args); 
		rank = MPI.COMM_WORLD.Rank();
		nRanks = MPI.COMM_WORLD.Size();  
		parseArgs(args);
	}

	public void process(){
		loadAndDistribute();
		calculate();
		if(rank == 0)
		{
			printTop10RanksValues();
		}
	}

	/**
	 * Print the pagerank values. Before printing you should sort them according to decreasing order.
	 * Print all the values to the output file. Print only the first 10 values to console.
	 *
	 * @throws IOException if an error occurs
	 */
	private void printTop10RanksValues(){
		BufferedWriter out = null;
		try{
			out = new BufferedWriter(new FileWriter(new File(outputFile)));
			out.append("Top 10 URLs with Highest Page Rank values. 	\n");
			if(debugMode) System.out.println("Top 10 URLs with Highest Page Rank values. 	\n");

			HashSet<Integer> indexSet = new HashSet<Integer>();
			//To output top 10, we don't need to sort the whole array. Sorting causes O(nlogn);
			// We only need to find maximum of the array 10 times. The time complexity is O(10n) = O(n)
			double maximum ;
			int index ;
			for(int i=0; i<10; i++){
				index = 0;
				maximum = 0;
				for(int j=0; j<size-1; j++){
					if(rankValues[j] >= maximum && !indexSet.contains(j)){
						maximum = rankValues[j];
						index = j;
					}
				}
				indexSet.add(index);
				if(debugMode) System.out.println(index+" : " + maximum +"	\n");
				out.append(index+" : " + maximum +"	\n");
			}
			out.close();
		}
		catch(FileNotFoundException ex){
			System.err.println("File Not Found!");
			ex.printStackTrace();
		}
		catch(IOException ex){
			ex.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private void calculate(){
		// Assign initial pageRanks 
		rankValues = new double[size];
		for(int i=0; i < size; i++){
			rankValues[i] = 1.0 / size;
		}
		
		//compute
		for(int i=1; i <= iterations; i++){
			rankValues = calPerIteration(rankValues);
		}

	}


	private double[] calPerIteration(double[] rankValues){
		double[] newRankValues = new double[size];
		double danglingValues = 0;
		for (Map.Entry<Integer, ArrayList<Integer>> entry : localAdjMatrix.entrySet()) {
			Integer sourceUrl = entry.getKey();
			ArrayList<Integer> outgoingUrls = entry.getValue();
			int outgoingSize = outgoingUrls.size();
			if(outgoingSize == 0){
				danglingValues +=  rankValues[sourceUrl] / size * df;
			}else{
				for(int i=0; i<outgoingUrls.size(); i++){
					int aOutUrl = outgoingUrls.get(i);
					newRankValues[aOutUrl] +=  rankValues[sourceUrl] / outgoingSize *df;
				}
			}
		}

		for(int i=0; i<size; i++){
			newRankValues[i] +=  (1-df) / size / nRanks + danglingValues;
		}

		MPI.COMM_WORLD.Allreduce(newRankValues, 0, newRankValues, 0, size, MPI.DOUBLE, MPI.SUM);

		return newRankValues;
	}


	/**
	 * Parse the command line arguments and update the instance variables.
	 *
	 * @param args arguments
	 */
	private void parseArgs(String[] args) {
		if(args.length < 7)
		{
			printUsage();
			System.exit(-1);
		}else{
			try{
				inputFile= args[3];
				outputFile= args[4];
				if(!args[5].isEmpty())	df = Double.parseDouble(args[5]);
				if(!args[6].isEmpty())	iterations = Integer.parseInt(args[6]);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	private void printUsage(){
		String usage = "Usage single node mode : mpjrun.sh -np [no. of processes] MPIPageRank "
				+ "+[inputfilename] [outputfilename] [damping factor] [num_iterations] +"
				+ "Usage multi node mode : mpjrun.sh -dev niodev -np [no. of processes] MPIPageRank"
				+ " [inputfilename] [outputfilename] [damping factor] [num_iterations]"; 
		System.out.println(usage);
	}

	/**
	 * Read file
	 * Load Adjacency Matrix
	 * Distribute data to all workers
	 * 
	 *  Rank 0 - reads from input file, loads the data to Adjacency Matrix and Sends data to others Ranks as per the blockSize
	 *  All other Ranks - Receive their respective data (urls) from Rank0
	 *  
	 *  
	 *  All Ranks have a localAdjacency Matrix which holds a part of the Adjacency Matrix 
	 * 
	 */
	private  void loadAndDistribute() 
	{
		int totalNumOfUrls = 0;
		int numOfPartitions = 0;
		int remainderUrls = 0;
		int start = 0;
		int blockSize = 0;
		int sizeBuf[] = new int[1];
		ArrayList<Integer> outgoingUrls = new  ArrayList<Integer>();
		int source =0;
		int outgoingUrlsLength=0;

		if(rank == 0)
		{
			BufferedReader in=null;
			try{
				in = new BufferedReader(new FileReader(inputFile));
				ArrayList<Integer> adjList = null;
				String [] characterArray = null;
				String line = "";
				while ((line = in.readLine())!=null ) {
					adjList = new ArrayList<Integer>();
					characterArray = line.split(" ");
					for(int i = 1;i < characterArray.length;i++){
						adjList.add(Integer.parseInt(characterArray[i]));
					}
					adjMatrix.put(Integer.parseInt(characterArray[0]), adjList );
				}
				in.close();
			}
			catch(FileNotFoundException ex)
			{
				System.err.println("File Not Found!");
				ex.printStackTrace();
			}
			catch(IOException ex)
			{
				ex.printStackTrace();
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}

			totalNumOfUrls = adjMatrix.size();   
			numOfPartitions = (totalNumOfUrls) / nRanks;
			remainderUrls = (totalNumOfUrls) % nRanks;

			if(debugMode)System.out.println("np size : " + nRanks);
			try
			{
				ArrayList<Integer> sourceUrls = new ArrayList<Integer>(adjMatrix.keySet());
				if(debugMode)System.out.println("sourceUrls : "+sourceUrls);
				int relativeStartIndex = 0, sourceNumber=0;
				int urlIndex=0;
				for (int i = 0; i < nRanks; i++)
				{
					//Calculate block size
					
					if(remainderUrls == 0) blockSize = numOfPartitions;
					
					else
					{
						if(i<remainderUrls) blockSize = numOfPartitions +1;
						else blockSize=numOfPartitions;
					}

					int[] sourceArray = new int[blockSize];
					int[] outgoingUrlsLengthArray = new int[blockSize];

					urlIndex=0;
					start = relativeStartIndex;
					for (relativeStartIndex = start; relativeStartIndex <= ((start+blockSize)-1); relativeStartIndex++)
					{
						source = sourceUrls.get(sourceNumber++).intValue();
						outgoingUrls =  adjMatrix.get(source);
						outgoingUrlsLength = outgoingUrls.size();

						sourceArray[urlIndex] =source;
						outgoingUrlsLengthArray[urlIndex]= outgoingUrlsLength;
						urlIndex++;
					}

					if(i!= 0)
					{
						sizeBuf[0]=blockSize;
						MPI.COMM_WORLD.Send(sizeBuf, 0, 1, MPI.INT, i, 0);
						MPI.COMM_WORLD.Send(sourceArray, 0, blockSize, MPI.INT, i, 1);
						MPI.COMM_WORLD.Send(outgoingUrlsLengthArray, 0, blockSize, MPI.INT, i, 2);
					}

					for(int k=0;k<blockSize; k++)
					{
						source = sourceArray[k];
						outgoingUrls = adjMatrix.get(source);
						outgoingUrlsLength =outgoingUrlsLengthArray[k];

						int[] outgoingUrlsArray= new int[outgoingUrlsLength];

						for(int n=0;n<outgoingUrlsLength;n++)
						{
							outgoingUrlsArray[n]=outgoingUrls.get(n);
						}
						if(i==0)
						{
							localAdjMatrix.put(source, outgoingUrls);
							if(debugMode)	System.out.println("Rank : " +rank+" Source : "+source + " OutgoingUrls List : "+outgoingUrls);
						}
						else
						{
							MPI.COMM_WORLD.Send(outgoingUrlsArray, 0, outgoingUrlsLength, MPI.INT, i, 3);
						}
					}
				}
				if(debugMode)System.out.println("Local matrix :" + localAdjMatrix+" --- rank : "+rank);
			}
			catch(Exception ex)
			{
				System.out.println("Error: "+ ex.getMessage());
			}
		}
		else
		{
			MPI.COMM_WORLD.Recv(sizeBuf, 0, 1, MPI.INT, 0, 0);
			blockSize = sizeBuf[0];

			if(debugMode) System.out.println("Block size : "+blockSize +" for rank : " +rank);
			int[] sourceArray = new int[blockSize];

			MPI.COMM_WORLD.Recv(sourceArray, 0, blockSize, MPI.INT, 0, 1);

			int[] outgoingUrlsLengthArray = new int[blockSize];
			MPI.COMM_WORLD.Recv(outgoingUrlsLengthArray, 0, blockSize, MPI.INT, 0, 2);

			ArrayList<Integer> outgoingUrlsList = null;
			for(int i=0; i<blockSize;i++)
			{
				source = sourceArray[i];
				outgoingUrls = adjMatrix.get(source);
				outgoingUrlsLength =outgoingUrlsLengthArray[i];

				int[] outgoingUrlsArray= new int[outgoingUrlsLength];
				MPI.COMM_WORLD.Recv(outgoingUrlsArray, 0, outgoingUrlsLength, MPI.INT, 0, 3);
				outgoingUrlsList = new ArrayList<Integer>();
				for(int m=0;m<outgoingUrlsLength;m++)
				{
					outgoingUrlsList.add(outgoingUrlsArray[m]);
				}

				localAdjMatrix.put(source, outgoingUrlsList);
				if(debugMode) System.out.println("Rank : " +rank+" Source : "+source + " OutgoingUrls List : "+outgoingUrlsList);
			}
			if(debugMode) System.out.println("Local matrix :" + localAdjMatrix+" --- rank : "+rank);

		}

		 
       
		/** broadcast the size
		/   every process invokes the bcast
		 * 
		 * 
		 */
		int totalSize[] = new int[1];
		totalSize[0] = totalNumOfUrls;
		MPI.COMM_WORLD.Bcast(totalSize, 0, 1, MPI.INT, 0);
		size = totalSize[0];
		 // now every rank should have the same values for size
		if(debugMode)System.out.println(rank+": "+size);
	}

	public static void main(String[] args) {
		MPIPageRank mpiPageRank = new MPIPageRank( args);
		mpiPageRank.process();
	}


}
