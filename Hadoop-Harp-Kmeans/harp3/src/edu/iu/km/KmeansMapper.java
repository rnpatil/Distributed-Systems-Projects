package edu.iu.km;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.CollectiveMapper;

import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;

public class KmeansMapper extends CollectiveMapper<String, String, Object, Object> {

	private int jobID;
	private int numMappers;
	private int vectorSize;
	private int numCentroids;
	private int numCenPartitions;
	private int numofIterations;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("begin setup : " + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
		jobID = configuration.getInt( KMeansConstants.JOB_ID, 0);
		numMappers =configuration.getInt(KMeansConstants.NUM_MAPPERS, 10);
		numCentroids =configuration.getInt(KMeansConstants.NUM_CENTROIDS, 20);
		numCenPartitions = numMappers;
		vectorSize =configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
		
		numofIterations = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);
		
		long endTime = System.currentTimeMillis();
		LOG.info("config (ms) :" + (endTime - startTime));
	}

	protected void mapCollective( KeyValReader reader, Context context) throws IOException, InterruptedException {
		LOG.info("Start  kmean collective mapper.");
		long startTime = System.currentTimeMillis();
		List<String> pointFiles = new ArrayList<String>();
		while (reader.nextKeyValue()) {
			String key = reader.getCurrentKey();
			String value = reader.getCurrentValue();
			LOG.info("Key: " + key + ", Value: " + value);
			pointFiles.add(value);
		}
		Configuration conf = context.getConfiguration();
		runKmeans(pointFiles, conf, context);
		LOG.info("Total times in master view: " + (System.currentTimeMillis() - startTime));
	}

	private void broadcastCentroids( Table<DoubleArray> cenTable) throws IOException{  
		//broadcast centroids to all other worker nodes
		boolean isSuccess = false;
		try {
			isSuccess = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID(),false);
		} catch (Exception e) {
			LOG.error("Fail to bcast.", e);
		}
		if (!isSuccess) {
			throw new IOException("Fail to bcast");
		}
	}

	private void kmeanComputation(Table<DoubleArray> newCenTable, Table<DoubleArray> currentCenTable,ArrayList<DoubleArray> dataPoints){

		for(DoubleArray aPoint: dataPoints){

			// 	For each data point, find the nearest centroid

			double minDist = Double.MAX_VALUE;
			double tempDist = 0;

			int nearestPartitionID = -1;
			for(Partition aCentroidPartition: currentCenTable.getPartitions()){
				DoubleArray aCentroid = (DoubleArray) aCentroidPartition.get();
				tempDist = calcEuclideanDistance(aPoint, aCentroid, vectorSize);
				if(tempDist < minDist){
					minDist = tempDist;
					nearestPartitionID = aCentroidPartition.id();            // Assign each data point to a nearest centroid 
				}						
			}

			/**
			 * 
			 * For a certain data point, found the nearest centroid.
			 * 
			 *  Add the data point to a partition in the new cenTable.
			 *  
			 **/

			double[] partial = new double[vectorSize+1];
			for(int j=0; j < vectorSize; j++){
				partial[j] = aPoint.get()[j];
			}
			partial[vectorSize]=1;

			// Create new partition for the centroid table if nearestPartitionID is null

			if(newCenTable.getPartition(nearestPartitionID) == null){
				Partition<DoubleArray> tmpAp = new Partition<DoubleArray>(nearestPartitionID, new DoubleArray(partial, 0, vectorSize+1));
				newCenTable.addPartition(tmpAp);

			}else{
				Partition<DoubleArray> apInCenTable = newCenTable.getPartition(nearestPartitionID);
				for(int i=0; i < vectorSize +1; i++){
					apInCenTable.get().get()[i] += partial[i];
				}
			}
		}
	}

	private void runKmeans(List<String> fileNames, Configuration conf, Context context) throws IOException {


		/**
		 * load data points
		 **/
		ArrayList<DoubleArray> dataPoints = loadData(fileNames, vectorSize, conf);


		/***
		 * 
		 *	Load centroids
		 *	For every partition in the centroid table, we will use the last element to store the number of points 
		 *	which are clustered to the particular partitionID
		 * 
		 * 
		 ***/

		Table<DoubleArray> cenTable = new Table<>(0, new DoubleArrPlus());
		if (this.isMaster()) {
			loadCentroids(cenTable, vectorSize, conf.get(KMeansConstants.centroid_file), conf);
		}

		System.out.println("After loading centroids");

		printTable(cenTable);

		/**
		 * Broadcast centroids to all other workers nodes
		 **/
		broadcastCentroids(cenTable);

		/**
		 * after broadcasting
		 */
		System.out.println("After brodcasting centroids");
		printTable(cenTable);



		Table<DoubleArray> newCenTable= new Table<>(0, new DoubleArrPlus());


		System.out.println("Iteraton No."+jobID);

		/**
		 * Compute new partial centroid table using previousCentroid Table and Data points
		 */
		kmeanComputation(newCenTable, cenTable, dataPoints);

		//	allReduce operation 

		allreduce("main", "allreduce_"+jobID, newCenTable);

		System.out.println("After allreduce");
		printTable(newCenTable);

		//we can calculate new centroids

		calculateCentroids(newCenTable);




		if(this.isMaster()){
			updateCentroidFile(newCenTable,conf,conf.get(KMeansConstants.centroid_file));
			
			// Final iteration
			
			if(jobID==numofIterations-1)
			{
				outputCentroids(newCenTable,  conf,   context);
			}
		}

	}

/**
 * 
 * 
 * @param cenTable
 * @param conf
 * @param cFileName
 * @throws IOException
 * 
 * Update centroid file after every iteration
 */
	private void updateCentroidFile(Table<DoubleArray>  cenTable,Configuration conf, String cFileName) throws IOException{

		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(cFileName),true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
	
		String output="";
		for( Partition<DoubleArray> ap: cenTable.getPartitions()){
			double res[] = ap.get().get();
			for(int i=0; i<vectorSize;i++)
				output+= res[i]+"\t";
			output+="\n";
		}
		try {
			bw.write(output);
			
			bw.flush();
			bw.close();
			System.out.println("Written updated centroids " +"to file "+ KMeansConstants.centroid_file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	
	  
	 
	/***
	 * 
	 * @param cenTable
	 * @param conf
	 * @param context
	 * output final updated centroids to output file
	 */
	 
	  private void outputCentroids(Table<DoubleArray>  cenTable,Configuration conf, Context context){
		  String output="";
		  for( Partition<DoubleArray> ap: cenTable.getPartitions()){
			  double res[] = ap.get().get();
			  for(int i=0; i<vectorSize;i++)
				 output+= res[i]+"\t";
			  output+="\n";
		  }
			try {
				context.write(null, new Text(output));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  

	/**
	 * 
	 * @param cenTable
	 * 
	 * Calculate  mean to compute new updated centroids
	 * 
	 * After allreduce operation, compute mean on centroid table  to calculate new centroids.
	 *  
	 **/


	private void calculateCentroids( Table<DoubleArray> cenTable){
		for( Partition<DoubleArray> partialCenTable: cenTable.getPartitions()){
			double[] doubles = partialCenTable.get().get();
			for(int h = 0; h < vectorSize; h++){
				doubles[h] /= doubles[vectorSize];
			}

			doubles[vectorSize] = 0;
		}
		System.out.println("after calculate new centroids");
		printTable(cenTable);
	}


	/**
	 * calculate Euclidean distance.
	 * @param aPoint
	 * @param otherPoint
	 * @param vectorSize
	 * @return
	 */
	private double calcEuclideanDistance(DoubleArray aPoint, DoubleArray aCentroidPoint, int vectorSize){
		double euclideanDistanceSquare=0;
		for(int i=0; i < vectorSize; i++){
			euclideanDistanceSquare += Math.pow(aPoint.get()[i]-aCentroidPoint.get()[i],2);
		}
		return Math.sqrt(euclideanDistanceSquare);
	}

	/**
	 * 
	 * loading centroids from HDFS
	 * @param cenTable
	 * @param vectorSize
	 * @param cFile
	 * @param configuration
	 * @throws IOException
	 */
	private void loadCentroids( Table<DoubleArray> cenTable, int vectorSize,  String cFileName, Configuration configuration) throws IOException{
		Path cPath = new Path(cFileName);
		FileSystem fs = FileSystem.get(configuration);
		FSDataInputStream in = fs.open(cPath);
		BufferedReader br = new BufferedReader( new InputStreamReader(in));
		String line="";
		String[] vector=null;
		int partitionId=0;
		while((line = br.readLine()) != null){
			vector = line.split("\\s+");
			if(vector.length != vectorSize){
				System.out.println("Errors while loading the centroids .");
				System.exit(-1);
			}else{
				double[] aCen = new double[vectorSize+1];

				for(int i=0; i<vectorSize; i++){
					aCen[i] = Double.parseDouble(vector[i]);
				}
				aCen[vectorSize]=0;
				Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId, new DoubleArray(aCen, 0, vectorSize+1));
				cenTable.addPartition(ap);
				partitionId++;
			}
		}
	}
	//load data form HDFS
	private ArrayList<DoubleArray>  loadData(List<String> fileNames,  int vectorSize, Configuration conf) throws IOException{
		ArrayList<DoubleArray> data = new  ArrayList<DoubleArray> ();
		for(String filename: fileNames){
			FileSystem fs = FileSystem.get(conf);
			Path dPath = new Path(filename);
			FSDataInputStream in = fs.open(dPath);
			BufferedReader br = new BufferedReader( new InputStreamReader(in));
			String line="";
			String[] vector=null;
			while((line = br.readLine()) != null){
				vector = line.split("\\s+");

				if(vector.length != vectorSize){
					System.out.println("Errors while loading data.");
					System.exit(-1);
				}else{
					double[] aDataPoint = new double[vectorSize];

					for(int i=0; i<vectorSize; i++){
						aDataPoint[i] = Double.parseDouble(vector[i]);
					}
					DoubleArray da = new DoubleArray(aDataPoint, 0, vectorSize);
					data.add(da);
				}
			}
		}
		return data;
	}

	/**
	 * 
	 * for printing table
	 * @param dataTable
	 */
	private void printTable(Table<DoubleArray> dataTable){
		for( Partition<DoubleArray> ap: dataTable.getPartitions()){

			double res[] = ap.get().get();
			System.out.print("ID: "+ap.id() + ":");
			for(int i=0; i<res.length;i++)
				System.out.print(res[i]+"\t");
			System.out.println();
		}
	}
}