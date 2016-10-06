package lsde10.suspicious.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 * Hello world!
 *
 */
public class App 
{
	private SparkConf sparkConf;
	private JavaSparkContext javaSparkContext;
	
	private void init(){
		sparkConf = new SparkConf().setAppName("Suspicious Outage Test");
		javaSparkContext = new JavaSparkContext(sparkConf);
		
	}
	
	private static App instance = null;
	
	private App() {
		this.init();
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

	private static App getInstance() {
		if (instance == null) {
			instance = new App();
		}
		return instance;
	}
	
    public static void main( String[] args )
    {
    	App app = App.getInstance();
    	JavaSparkContext sc = app.getJavaSparkContext();
		JavaRDD<String> files = sc.textFile("\\user\\hannesm\\lsde\\ais\\10\\01\\00-00.txt.gz");
		
		files.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String v1) throws Exception {
				if(!v1.startsWith("!"))
					return false;
				return true;
			}
		});
		
		//files.saveAsTextFile("success.txt");
		
		//spark-submit --class lsde10.suspicious.test.App     --master yarn --name supicious.outage.test     --deploy-mode cluster     suspicious.test-0.0.1-SNAPSHOT-jar-with-dependencies.jar

		
    }
}