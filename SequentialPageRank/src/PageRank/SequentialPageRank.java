package PageRank;



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

public class SequentialPageRank {

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

	// temporary rank Values tables
	private HashMap<Integer,Double> rankValues_temp =  new HashMap<Integer,Double>();;
	/**
	 * Parse the command line arguments and update the instance variables. Command line arguments are of the form
	 * <input_file_name> <output_file_name> <num_iters> <damp_factor>
	 *
	 * @param args arguments
	 */
	public void parseArgs(String[] args) {
		if(args.length != 4){
			printUsage();
			System.exit(-1);
		}else{
			try{
				inputFile= args[0];
				outputFile= args[1];
				if(!args[2].isEmpty())	iterations = Integer.parseInt(args[2]);
				if(!args[3].isEmpty())	df = Double.parseDouble(args[3]);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public void printUsage(){
		String usage = "Usage: Java SequentialPageRank [input file name] "
				+ "[output file name] [iteration count] [damping factor]";
		System.out.println(usage);
	}

	/**
	 * Read the input from the file and populate the adjacency matrix
	 *
	 * The input is of type
	 *
     0
     1 2
     2 1
     3 0 1
     4 1 3 5
     5 1 4
     6 1 4
     7 1 4
     8 1 4
     9 4
     10 4
	 * The first value in each line is a URL. Each value after the first value is the URLs referred by the first URL.
	 * For example the page represented by the 0 URL doesn't refer any other URL. Page
	 * represented by 1 refer the URL 2.
	 *
	 * @throws java.io.IOException if an error occurs
	 */
	public void loadInput() throws IOException {
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
		finally
		{
			in.close();
		}
	}

	/**
	 * Do fixed number of iterations and calculate the page rank values. You may keep the
	 * intermediate page rank values in a hash table.
	 * @throws IOException 
	 */
	public void calculatePageRank() throws IOException {

		// initial pageRank assignment 

		double initialPageRank = 1.0 / (double)adjMatrix.size();
		for(int i=0;i<adjMatrix.size();i++){
			rankValues.put(i,initialPageRank);	
		}

		// Call pageRank Calculator function for the given number of iterations.
		for(int count_of_loop = 0; count_of_loop < iterations; count_of_loop++)
		{
			calculator(rankValues,adjMatrix,df);
		}
	}


	/**
	 * Print the pagerank values. Before printing you should sort them according to decreasing order.
	 * Print all the values to the output file. Print only the first 10 values to console.
	 *
	 * @throws IOException if an error occurs
	 */
	public void printValues() throws IOException {

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
		finally{
			out.close();
		}
	}


	/**
	 * @param rankValues - hash table to store the url page ranks
	 * @param adjMatrix - adjacency list for a url and its outgoing  url links
	 * @param df - damping factor
	 * 
	 * This functions has the core logic to calculate pageRanks for the given adjacency matrix.
	 */
	public void calculator(HashMap<Integer,Double> rankValues,HashMap<Integer,ArrayList<Integer>> adjMatrix,double df)
	{
		ArrayList<Integer> outgoingURLs =new ArrayList<Integer>();
		int currentURL=0;
		int numberOfOutgoingURLs=0;
		int targetURL=0;
		double temp_rankValue=0.0;
		double danglingValue=0;
		double danglingValue_eachPage=0.0;
		int totalNumberUniqueURLs=adjMatrix.size();


		/**
		 * Initialize temp rankValue table
		 **/
		rankValues_temp.clear();

		for(int i = 0; i < totalNumberUniqueURLs; i++)
		{
			rankValues_temp.put(i,0.0);
		}

		Set<Entry<Integer, ArrayList<Integer>>> set =adjMatrix.entrySet();
		for (Entry<Integer, ArrayList<Integer>> s : set)
		{
			currentURL =  s.getKey().intValue();

			outgoingURLs = s.getValue();
			numberOfOutgoingURLs = outgoingURLs.size();


			/**  
			 *  Handle dangling links (Node with no outbound links)
			 **/
			if(numberOfOutgoingURLs == 0){
				danglingValue += rankValues.get(currentURL);		
			}else{// other nodes
				for(int i = 0; i < numberOfOutgoingURLs; i++){
					targetURL=outgoingURLs.get(i).intValue();
					temp_rankValue=rankValues_temp.get(targetURL) + ((rankValues.get(currentURL))/(numberOfOutgoingURLs));
					rankValues_temp.put(targetURL,temp_rankValue);
				}
			}
		}


		/** 
		 * Sum of page rank of all dangling URLs (danglingValue) is distributed over the total number of URLs in the adjacency matrix
		 **/
		danglingValue_eachPage = danglingValue / (double)totalNumberUniqueURLs;

		for(int i=0;i<totalNumberUniqueURLs;i++){
			rankValues.put(i, rankValues_temp.get(i) + danglingValue_eachPage);
		}

		/**
		 * The PageRank theory holds that even an imaginary surfer who is randomly clicking on links will eventually stop clicking. 
		 * The probability, at any step, that the person will continue is a damping factor d. 
		 * Various studies have tested different damping factors, but it is generally assumed that the damping factor will be around 0.85. 
		 * The formula considering damping factor is shown in Eqn.2. N refers to the total number of unique URLs.
		 *
		 *   PR(i)=  (1-df) * ((1.0)/ (double) adjMatrix.size()) + df * rankValues.get(i))
		 **/



		/**
		 *  Include Damping factor in the final pageRank calculations and assign the new pageRanks to rankValues hash table
		 */

		for(int i=0;i<totalNumberUniqueURLs;i++)
		{
			rankValues.put(i,((1-df)*((1.0)/(double)totalNumberUniqueURLs) + df*rankValues.get(i)));
		}
	}
	// end of calculator
	public static void main(String[] args) throws IOException {

		SequentialPageRank sequentialPR = new SequentialPageRank();
		sequentialPR.parseArgs(args);
		sequentialPR.loadInput();
		sequentialPR.calculatePageRank();
		sequentialPR.printValues();
	}
}

