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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.List;

import org.apache.spark.SparkConf;


public class SuspiciousOutageApp {
	
	private SparkConf sparkConf;
	private JavaSparkContext javaSparkContext;
	private Configuration config; 

	private void init(){
		sparkConf = new SparkConf().setAppName("SuspiciousOutageApp");
		javaSparkContext = new JavaSparkContext(sparkConf);
		config =  new Configuration();
		config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
		config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
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
	public Configuration getConfig() {
		return config;
	}
	public void setConfig(Configuration config) {
		this.config = config;
	}

	
	
	public static void main( String[] args )
    {
		
		SuspiciousOutageApp app = SuspiciousOutageApp.getInstance();
		JavaSparkContext sc = app.getJavaSparkContext();
		final Processor processor = Processor.getInstance();
		
		//JavaRDD<String> files = sc.textFile("/user/hannesm/lsde/ais/10/01/00-00.txt.gz");
		JavaPairRDD<String, String> wholeFiles = sc.wholeTextFiles("/user/hannesm/lsde/ais/10/01/00-00.txt.gz");
		//file:/10/01/00-00.txt.gz
		
		JavaPairRDD<String, String> files = wholeFiles.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				String oldname = t._1();
				int index = t._1().indexOf("/ais/10/");
				String filename = oldname.substring(index + 8);
				String[] split = filename.split("/");
				String result = split[0] + split[1].replace("-", "").substring(0, 4);
				
				return new Tuple2<String, String>(result, t._2());
			}
		});
		
		    
		 
	    try {
	    	FileSystem fs = FileSystem.get(app.getConfig()); 
			Path filenamePath = new Path("/user/lsde10/out/input.txt"); 
			if (fs.exists(filenamePath)) {
			    fs.delete(filenamePath, true);
			}
			FSDataOutputStream fin = fs.create(filenamePath);
		    fin.writeUTF("hello");
		    fin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//remove the exclamation marks
		JavaPairRDD<String, String> cleanedAIS = files.mapValues(new Function<String, String>() {

			private static final long serialVersionUID = 1L;

			public String call(String file) throws Exception {
				return processor.cleanAISMsg(file);
			}

		});
		
		
		//decode the lines to AISMessages
		JavaPairRDD<String,AisMessage> rawAIS = cleanedAIS.mapValues(new Function<String, AisMessage>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(String msg) throws Exception {
				return processor.decodeAisMessage(msg);
			}
		});
		
		//filter AIS messages for wrong messages and types without position information
		JavaPairRDD<String,AisMessage> decodedAIS = rawAIS.filter(new Function<Tuple2<String,AisMessage>, Boolean>() {

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
		

		
		//decodedAIS.persist(StorageLevel.MEMORY_ONLY());
		//decodedAIS.sample(false, 0.25).coalesce(1).saveAsTextFile("/user/lsde10/success");*/

		
		//create a JavaRDD to find max min
        final JavaRDD<AisMessage> allMessages = decodedAIS.map(new Function<Tuple2<String, AisMessage>,AisMessage>(){ 

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
		}*/
		
		//TODO go over the files and put the mmsi where the ships are
		//TODO find ships that have suspicious outage time

		//create rdd with this information
		//JavaRDD<Grid> GridMap = javaSparkContext.parallelize(grids);
		
		
		//TODO check if the found ships are in a area where other ships are able to send
		//signals
		
		
		//TODO make a final ranking by taking the type of the ship into account
		
		

    }
	
}
		

