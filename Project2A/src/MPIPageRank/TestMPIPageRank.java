package MPIPageRank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mpi.MPI;

public class TestMPIPageRank {

	// adjacency matrix read from file
	private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();
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
	private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();
	//number of ranks
	private int nRanks;
	//id of ranks
	private int rank;
	
	public TestMPIPageRank(String[] args){
		MPI.Init(args); 
		rank = MPI.COMM_WORLD.Rank();
		nRanks = MPI.COMM_WORLD.Size();  
		parseArgs(args);
	}
	
	public void process(){
		loadAndDistribute();
		printValues();
	}

	public static void main(String[] args) {
		TestMPIPageRank testMPIPageRank = new TestMPIPageRank( args);
		testMPIPageRank.process();
	}

	public void parseArgs(String[] args) {
		if(args.length < 7)  // need to change
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

	public void printUsage(){
		String usage = "Usage: mpjrun.sh -np [no. of processes] MPIPageRank "
				+ "[inputfilename] [outputfilename] [damping factor] [num_iterations]"; 
		System.out.println(usage);
	}

	public  void loadAndDistribute() 
	{
		int totalNumOfUrls = 0;
		int numOfPartitions = 0;
		int remainderUrls = 0;
		int start = 0;
		int blockSize = 0;
		int localSize = 0;
		
		System.out.println("rank : "+rank);
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

			System.out.println("np size : " + nRanks);

			try
			{
				ArrayList<Integer> sourceUrls = new ArrayList<Integer>(adjMatrix.keySet());
				System.out.println("sourceUrls : "+sourceUrls);
				int relativeStartIndex = 0, sourceNumber=0;
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

					int urlIndex=0;
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
							
							 ArrayList<Integer> outgoingUrlsList = new ArrayList<Integer>();

				             for(int m=0;m<outgoingUrlsLength;m++)
				             {
				            	 outgoingUrlsList.add(outgoingUrlsArray[m]);
				             }
				             
							System.out.println("localUrls : "+outgoingUrlsList + "   for source: " + source +" with rank: "+rank);
						}
						else
						{
							MPI.COMM_WORLD.Send(outgoingUrlsArray, 0, outgoingUrlsLength, MPI.INT, i, 3);
						}
						
					}

				}
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
			
			System.out.println("Block size : "+blockSize +" for rank : " +rank);
			int[] sourceArray = new int[blockSize];


			MPI.COMM_WORLD.Recv(sourceArray, 0, blockSize, MPI.INT, 0, 1);

			
			int[] outgoingUrlsLengthArray = new int[blockSize];
			MPI.COMM_WORLD.Recv(outgoingUrlsLengthArray, 0, blockSize, MPI.INT, 0, 2);
		
			for(int i=0; i<blockSize;i++)
			{
				source = sourceArray[i];
				outgoingUrls = adjMatrix.get(source);
				outgoingUrlsLength =outgoingUrlsLengthArray[i];
					
				int[] outgoingUrlsArray= new int[outgoingUrlsLength];
				MPI.COMM_WORLD.Recv(outgoingUrlsArray, 0, outgoingUrlsLength, MPI.INT, 0, 3);
				ArrayList<Integer> outgoingUrlsList = new ArrayList<Integer>();
				for(int m=0;m<outgoingUrlsLength;m++)
		        {
					outgoingUrlsList.add(outgoingUrlsArray[m]);
			    }
						
				System.out.println("Non localurls : "+outgoingUrlsList + "   for source :" + source+" with  rank :"+rank);
			}
		}
	}


	/**
	 * Print the pagerank values. Before printing you should sort them according to decreasing order.
	 * Print all the values to the output file. Print only the first 10 values to console.
	 *
	 * @throws IOException if an error occurs
	 */
	public void printValues()  {

		/** Sort the page rank values in descending order **/

		Set<Entry<Integer,Double>> pagerankSet = rankValues.entrySet();
		List<Entry<Integer,Double>> sortPagerankList = new ArrayList<Entry<Integer,Double>>(pagerankSet);

		Collections.sort( sortPagerankList, new Comparator<Map.Entry<Integer,Double>>()
		{
			public int compare( Map.Entry<Integer,Double> obj1, Map.Entry<Integer,Double> obj2 )
			{
				return (obj2.getValue()).compareTo( obj1.getValue() );
			}
		} );

		/***
		 * 
		 *  Write top 10 ranked pages to the output file.
		 * 
		 **/
		BufferedWriter out = null;
		try{
			out = new BufferedWriter(new FileWriter(new File(outputFile)));
			out.append("Top 10 URLs with Highest Page Rank values. 	\n");
			int i =0;
			while (i < sortPagerankList.size() && i <10) {
				Map.Entry<Integer,Double > rankEntry = sortPagerankList.get(i);
				out.append(rankEntry.getKey()+" : "+rankEntry.getValue()+"	\n");
				i++;
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
}
