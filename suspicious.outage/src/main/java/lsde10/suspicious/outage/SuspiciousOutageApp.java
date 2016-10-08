package lsde10.suspicious.outage;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;


import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessage1;
import dk.dma.ais.message.AisMessage2;
import dk.dma.ais.message.AisMessage3;
import dk.dma.ais.message.AisPositionMessage;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

import org.apache.spark.SparkConf;


public class SuspiciousOutageApp {
	
	private SparkConf sparkConf;
	private static JavaSparkContext javaSparkContext;
	private void init(){
		sparkConf = new SparkConf().setAppName("SuspiciousOutageApp");
		javaSparkContext = new JavaSparkContext(sparkConf);
		
	}
	private static SuspiciousOutageApp instance = null;
	private SuspiciousOutageApp() {
		this.init();
	}
	public static SuspiciousOutageApp getInstance() {
		if (instance == null) {
			instance = new SuspiciousOutageApp();
		}
		return instance;
	}
	public SparkConf getSparkConf() {
		return sparkConf;
	}
	public void setSparkConf(SparkConf sparkConf) {
		this.sparkConf = sparkConf;
	}
	public JavaSparkContext getJavaSparkContext() {
		return javaSparkContext;
	}
	public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
		this.javaSparkContext = javaSparkContext;
	}

	public static void main( String[] args )
    {
		
		SuspiciousOutageApp app = SuspiciousOutageApp.getInstance();
		JavaSparkContext sc = app.getJavaSparkContext();
		final Processor processor = Processor.getInstance();
		
		JavaPairRDD<String, String> files = sc.wholeTextFiles("/user/hannesm/lsde/ais/10/01");
		
		//get rid of the time information inside the files, but keep all lines
		JavaPairRDD<String, String> filteredAIS = files.filter(new Function<Tuple2<String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,String> line) throws Exception {
				if(!line._2.startsWith("!")){
					int index = line._2.indexOf("!", 0);
					if(index == -1){
						return false;
					}
					return true;
				}
				return true;
			}
		});
		
		//remove the exclamation marks
		JavaPairRDD<String, String> cleanedAIS = filteredAIS.mapValues(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String line) throws Exception {
				return processor.cleanAISMsg(line);
			}
		});
		
		//decode the lines to AISMessages
		JavaPairRDD<String,AisMessage> rawAIS = cleanedAIS.mapValues(new Function<String, AisMessage>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(String msg) throws Exception {
				return processor.decodeAisMessage(msg);
			}
		});
		
		//filter AIS messages for wrong messages and types without position information
		JavaPairRDD<String,AisMessage> decodedAIS = rawAIS.filter(new Function<Tuple2<String,AisMessage>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String,AisMessage> v1) throws Exception {
				if(v1._2 == null)
					return false;
				if((v1._2 instanceof AisPositionMessage)){
					return true;
				}
					
				return false;
				
			}
		});
		
	/*	JavaPairRDD<Integer, AisMessage> mmsi = decodedAIS.mapToPair(new PairFunction<AisMessage, Integer, AisMessage>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, AisMessage> call(AisMessage t) throws Exception {
				return new Tuple2<Integer, AisMessage>(t.getUserId(), t);
			}
		});*/
		
		/*JavaPairRDD<String,String> cleanAIS = files.mapToPair(new PairFunction<Tuple2<String,String>, String, String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> file) throws Exception {
				return processor.cleanAISMsg(file);
			}
			
		});*/
		
		
		//decode the lines to AISMessages

		//JavaRDD<AISMessage> decoded = cleanAIS.flatMap(s -> processor.decodeAISMessage(s));
		/* JavaRDD<AisMessage> decoded = cleanAIS.flatMap(new FlatMapFunction<Tuple2<String,String>, AisMessage>() {

			@Override
			public Iterable<AisMessage> call(Tuple2<String, String> file) throws Exception {
				return processor.decodeAISMessage(file);
			}
		}); */
	
		//decoded.persist(StorageLevel.MEMORY_ONLY());
		decodedAIS.sample(false, 0.25).coalesce(1).saveAsTextFile("/user/lsde10/success");
		
		long count = decodedAIS.count();
		System.out.println(count);
		
		//create a JavaRDD to find max min
        final JavaRDD<AisMessage> allMessages = decodedAIS.map(new Function<Tuple2<String, AisMessage>,AisMessage>(){ 
        			/**
        			 * 
        			 */
        			private static final long serialVersionUID = 1L;

        			@Override
        			public AisMessage call(Tuple2<String, AisMessage> msg) throws Exception {
        				return msg._2;
        			}
                });		

		//can reduce handle when the message is returned as null?
		AisMessage maxLatMsg = allMessages.reduce(new Function2<AisMessage, AisMessage, AisMessage>() 
		{
			
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(AisMessage msg1, AisMessage msg2) throws Exception {
					return processor.maximumLatitude(msg1, msg2);
			}
		});
		
		AisMessage minLatMsg = allMessages.reduce(new Function2<AisMessage, AisMessage, AisMessage>() 
		{
			
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(AisMessage msg1, AisMessage msg2) throws Exception {
					return processor.minimumLatitude(msg1, msg2);
			}
		});
		
		AisMessage maxLonMsg = allMessages.reduce(new Function2<AisMessage, AisMessage, AisMessage>() 
		{
			
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(AisMessage msg1, AisMessage msg2) throws Exception {
					return processor.maximumLongtitude(msg1, msg2);
			}
		});
		
		AisMessage minLonMsg = allMessages.reduce(new Function2<AisMessage, AisMessage, AisMessage>() 
		{
			
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(AisMessage msg1, AisMessage msg2) throws Exception {
					return processor.minimumLongtitude(msg1, msg2);
			}
		});
		
		float maxLat = processor.getValue(maxLatMsg, true);
		float minLat = processor.getValue(minLatMsg, true);
		
		float maxLon = processor.getValue(maxLonMsg, false);
		float minLon = processor.getValue(minLonMsg, false);
				
		decodedAIS.sample(false, 0.25).coalesce(1).saveAsTextFile("/user/lsde10/success");
		
		//TODO go over list create track of each message and add that message to the 
		//list of ships		
		
		//TODO read the Messages and train a grid-like World-map
		//decoded.foreach(m -> processor.trainGridMap(m)); //  NOT SURE ABOUT THIS
		
		float lat = minLat;
		float lon = minLon;
		int size = 100;
		
		/*
		//create the grids of the whole area
		List<Grid> grids;
		while(lat < maxLat && lon < maxLon)
		{
			Grid map = new Grid(lat,lon,size);
			grids.add(map);
			
			lat = lat+size;
			lon = lon+size;
		}
		
		//TODO go over the files and put the mmsi where the ships are
		//TODO find ships that have suspicious outage time
		for(int i = 0; i< decoded.count(); i++)
		{
			//add the ship to its grid
			//
			
		}*/
		//create rdd with this information
		//JavaRDD<Grid> GridMap = javaSparkContext.parallelize(grids);
		
		
		//TODO check if the found ships are in a area where other ships are able to send
		//signals
		
		
		//TODO make a final ranking by taking the type of the ship into account
		
		
		/*
		 * Code from the local version 
		 * String path = System.getProperty("user.dir");
		
		List<InputStream> iss = null;
		try {
			iss = Files.list(Paths.get(path + "//data//06//"))
			        .filter(Files::isRegularFile)
			        .map(f -> {
			            try {
			                return new FileInputStream(f.toString());
			            } catch (Exception e) {
			                throw new RuntimeException(e);
			            }
			        }).collect(Collectors.toList());
		} catch (IOException e1) {
			
			e1.printStackTrace();
		} 

		SequenceInputStream stream = new SequenceInputStream(Collections.enumeration(iss));

		
		File dir = new File(path + "//data//06//");
		int c = 0;
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
		    	if(child.getName().startsWith("!") && (c < 240)){
		    		AisTracker.readCsv(child.getAbsolutePath());
		    		c++;
		    	}
			}
		}*/

		//AisTracker.printOutages(30);
		//AisTracker.plotOnMap(180);
    }

}
