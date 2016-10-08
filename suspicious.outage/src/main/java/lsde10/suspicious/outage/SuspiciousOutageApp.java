package lsde10.suspicious.outage;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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

		    
		
		
		
		//get rid of the time information inside the files, but keep all lines
		/*JavaRDD<String> filteredAIS = files.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				if(!line.startsWith("!")){
					int index = line.indexOf("!", 0);
					if(index == -1){
						return false;
					}
					return true;
				}
				return true;
			}
		});*/
		/*JavaPairRDD<String, String> cleanedFiles = files.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> file) throws Exception {
				return processor.cleanAISMsg(file);
			}
		});*/
		
		
		
		
		/*JavaRDD<String> cleanedAIS = filteredAIS.map(new Function<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(String line) throws Exception {
				return processor.cleanAISMsg(line);
			}
		});
		
		//decode the lines to AISMessages
		JavaRDD<AisMessage> rawAIS = cleanedAIS.map(new Function<String, AisMessage>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(String msg) throws Exception {
				return processor.decodeAisMessage(msg);
			}
		});
		
		//filter AIS messages for wrong messages and types without position information
		JavaRDD<AisMessage> decodedAIS = rawAIS.filter(new Function<AisMessage, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(AisMessage v1) throws Exception {
				if(v1 == null)
					return false;
				if((v1 instanceof AisPositionMessage)){
					AisPositionMessage v2 = (AisPositionMessage) v1;
					v2.get
					return true;
				}
					
				return false;
				
			}
		});
		
		JavaPairRDD<Integer, Tuple2<AisMessage,Integer>> mmsi = decodedAIS.mapToPair(new PairFunction<AisMessage, Integer, Tuple2<AisMessage,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Tuple2<AisMessage,Integer>> call(AisMessage t) throws Exception {
				return new Tuple2<Integer, AisMessage>(t.getUserId(), t.get);
			}
		});
		
		JavaPairRDD<Integer, TimeGap> mmsiGrouped = mmsi.reduceByKey(new Function2<AisMessage, AisMessage, AisMessage>() {
			
			/**
			 * 
			 */
		/*	private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(AisMessage v1, AisMessage v2) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
		decodedAIS.persist(StorageLevel.MEMORY_ONLY());
		
		decodedAIS.sample(false, 0.25).coalesce(1).saveAsTextFile("/user/lsde10/success");*/
		
		
		/*long count = decoded.count();
		System.out.println(count);

		//can reduce handle when the message is returned as null?
		AISMessage maxLatMsg = decoded.reduce((a,b) -> processor.compareLatitude(a, b, true));
		AISMessage minLatMsg = decoded.reduce((a,b) -> processor.compareLatitude(a, b, false));
		
		AISMessage maxLonMsg = decoded.reduce((a,b) -> processor.compareLongitude(a, b, true));
		AISMessage minLonMsg = decoded.reduce((a,b) -> processor.compareLongitude(a, b, false));
		
		float maxLat = processor.getValue(maxLatMsg, true);
		float minLat = processor.getValue(minLatMsg, true);
		
		float maxLon = processor.getValue(maxLonMsg, false);
		float minLon = processor.getValue(minLonMsg, false);
		
		System.out.printf("maximum latitude: %.5f", maxLat);
		System.out.printf("minimum latitude: %.5f", minLat);
		System.out.printf("maximum longtitude: %.5f", maxLon);
		System.out.printf("maximum longtitude: %.5f", minLon);*/
		
		
		//TODO read the Messages and train a grid-like World-map
		//decoded.foreach(m -> processor.trainGridMap(m)); //  NOT SURE ABOUT THIS
		
		//TODO find ships that have suspicious outage time
		
		
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
