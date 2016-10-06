package lsde10.suspicious.outage;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import dk.dma.ais.message.AisMessage;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;


public class SuspiciousOutageApp {
	
	private SparkConf sparkConf;
	private JavaSparkContext javaSparkContext;
	
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
		
		
		JavaRDD<String> files = sc.textFile("\\user\\hannesm\\lsde\\ais\\10\\01\\00-00.txt.gz");
		
		//get rid of the time information inside the files, but keep all lines
		//JavaPairRDD<String,String> cleanAIS = files.mapToPair(s ->  processor.cleanAISMsg(s));
		JavaRDD<String> filteredAIS = files.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
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
		});
		
		JavaRDD<String> cleanedAIS = filteredAIS.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String line) throws Exception {
				return processor.cleanAISMsg(line);
			}
		});
		
		JavaRDD<AisMessage> rawAIS = cleanedAIS.map(new Function<String, AisMessage>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public AisMessage call(String msg) throws Exception {
				return processor.decodeAisMessage(msg);
			}
		});
		
		JavaRDD<AisMessage> decodedAIS = rawAIS.filter(new Function<AisMessage, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(AisMessage v1) throws Exception {
				if(v1 == null)
					return false;
				return true;
			}
		});
		
		decodedAIS.persist(StorageLevel.MEMORY_ONLY());
		
		
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
