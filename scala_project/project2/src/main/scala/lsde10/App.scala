package lsde10

import dk.dma.ais.sentence.Vdm
import dk.dma.ais.message.AisMessage
import dk.dma.ais.message.AisPositionMessage
import dk.dma.ais.message.AisMessageException
import dk.dma.ais.binary.SixbitException
import dk.dma.ais.sentence.SentenceException
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time._

/**
 * @author ${user.name}
 */
object App {
	
	//useful commands
	//spark-shell --jars ais-lib-messages-2.0.jar,enav-model-0.3.jar --master yarn
	//spark-submit --class lsde10.App --master yarn --deploy-mode cluster project2-0.0.1-jar-with-dependencies.jar
	//hdfs dfs -rm -r <dir_name>
	//hdfs dfs -copyToLocal <input> <output>

  def main(args : Array[String]) {
  
	def decode (line: String) : AisPositionMessage = {
		try{
			val vdm = new Vdm()
			vdm.parse(line)
			val msg = AisMessage.getInstance(vdm)
			if(msg.isInstanceOf[AisPositionMessage])
				return msg.asInstanceOf[AisPositionMessage]
			else 
				return null
		}
		catch{
			case ex: AisMessageException =>{}
			case ex: SixbitException =>{}
			case ex: SentenceException =>{}
		}
		return null
	}
	
	def getTimeKey(time: DateTime) : String = {
		val hour = time.getHourOfDay()
		val strhour = "%02d".format(hour)
		val day = time.getDayOfMonth()
		val strday = "%02d".format(day)
		
		//here we can easily change the granularity by just mapping the hour to a specific key
		
		var str = strday+strhour
		return str
	}

	val conf = new SparkConf().setAppName("Finding Outage")
	val sc = new SparkContext(conf)
	
	var path :String = "/user/hannesm/lsde/ais/10/01/*.txt.gz\n/user/hannesm/lsde/ais/10/02/*.txt.gz\n/user/hannesm/lsde/ais/10/03/*.txt.gz\n/user/hannesm/lsde/ais/10/04/*.txt.gz\n"
						
	  val text = sc.wholeTextFiles(path).flatMapValues(y => y.split("\n")).values.flatMap(file => {
	  val lines = file.split("\n")
	  val id = lines.head.split(" ").head
	  lines.tail.map((id, _))
	})

	text.cache()
	
	//decode and filter
	val decoded = text.filter(p => p._2.contains("!"))
						.map(p => (p._1, decode(p._2.substring(p._2.indexOf("!")))))
						.filter(p => if(p._2 == null) false else true)
	text.unpersist()
	decoded.cache()
	
	//####################Compute the outages########################################################

	
	//allShipsLocation = ((mmsi,timestamp), GeoLocation) 
	//Check if GeoLocation is null

	
	val allShipsLocation = decoded.map(p => ((p._2.getUserId(),p._1.substring(0,p._1.indexOf(".")).toLong), p._2.asInstanceOf[AisPositionMessage].getPos().getGeoLocation()))
							.filter(p => if(p._2 == null) false else true)
	decoded.unpersist()
	allShipsLocation.cache()
	
	//get the distinct values
	//sort by (mmsi,timestamp)
	//get the gaps, the resulting type is (mmsi,gap,starttime, endtime, lat,long)
	//both latitude and longitude are truncated to reflect the "area"
	//key is here (mmsi,timestamp)

	//check if mmsi number is correct
	//gap interval between 20 mins to 10 hours

	
	var outageData = allShipsLocation.map(kv => (kv._1,kv)).reduceByKey {case (a,b) => a}.map(_._2)
							.sortBy(_._1)
							.sliding(2).collect({case Array((key1, val1), (key2, val2)) if key1._1 == key2._1 => (key1._1, key2._2 - key1._2, getTimeKey(new DateTime(key1._2*1000).toDateTime), getTimeKey(new DateTime(key2._2*1000).toDateTime), (math floor val1.getLatitude() *10)/10, (math floor val1.getLongitude() *10)/10)})
							.filter(p => if(p._1.toString.length==9)true else false)
							.filter(p => if(p._2 > 1200 && p._2 < 36000) true else false)
	outageData.cache()	
							
	//####################Connectivity for area########################################################
	
	
	//************transmitting ships****************
	//get Location information : format = ((lat,long,timekey), mmsi)
	//get number of ships sending in the area and timeinterval
	var allAreas = allShipsLocation.map(p => (((math floor p._2.getLatitude() *10)/10, (math floor p._2.getLongitude() *10)/10, getTimeKey(new DateTime(p._1._2*1000).toDateTime)),p._1._1))
									.distinct()
									.groupByKey().map(p => (p._1,p._2.size))
	allShipsLocation.unpersist()
	allAreas.cache()
	
	//***********not transmitting ships*****************
	//same idea, just for getting the number of ships that have the start of gaps in that place (there will be overlaps)
	var nonTransmittingAreas = outageData.map(p => ((p._5,p._6,p._3),p._1))
										.distinct()
										.groupByKey().map(p => (p._1,p._2.size))
	nonTransmittingAreas.cache()
	
	//((lat,lon,timekey),(size1,size2))
	//join the two rdds to calculate the connectivity
	//((lat,lon,timekey),percentage)
	var result = allAreas.join(nonTransmittingAreas)
							.map(p => (p._1,(p._2._1.toFloat)/p._2._2.toFloat))
							
	allAreas.unpersist()
	nonTransmittingAreas.unpersist()
	result.cache()
	
	//#################Ranking Outages######################################################
	
	def rankShip(gap: Long, percentage: Float, time : String) : Float = {
		//very basic version of ranking
		
		var rank = 0F
		var alpha = 0.5F
		var beta = 0.3F
		var gamma = 0.2F
		var night = 0.6F
		var day = 0.4F
		
		//WE NEED TO NORMALIZE GAP TOO, but how??	
		//gap:seconds
		//turn into hours and add to rank
		rank += alpha * (gap/600
		
		//percentage:percentage of ships not sending messages
		//turn into 1 to 10 and add to rank
		rank += beta * percentage
		
		//time:day/hour, we only need to get hour and check (9 pm - 6 am night, otherwise day)
		//night - add 1 otherwise 0
		var hour : Int = time.substring(2).toInt
		if(hour < 21 && hour > 6){
			rank += gamma * night
		}
		else{
			rank += gamma * day
		}
		
		return rank
			
	}
	
	//outageData(mmsi, gap, starttime, endtime, lat, long) gaps of all ships
	//result((lat,lon,timekey),percentage)
	//what we want to have (mmsi, gap, geo_percentage, starttime)
	//percentage = percentage of ships that have an outage
	
	//((lat,lon,startime),(mmsi,gap))
	val gapShips = outageData.map(p => ((p._5,p._6,p._3),(p._1,p._2))) 
	
	//((lat,lon,starttime),((mmsi,gap),percentage)) - result of join
	//(mmsi, rank) - final product
	val rankedShips = gapShips.join(result)
					.map(p => (p._2._1._1,rankShip(p._2._1._2, p._2._2, p._1._3)))	
					.filter(p => p._2 > 50)
									
	outageData.unpersist()
	result.unpersist()
	
	rankedShips.saveAsTextFile("result")
	
	}
}
