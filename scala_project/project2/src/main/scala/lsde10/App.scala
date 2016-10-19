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
  
	def decode (line: String) : AisMessage = {
		try{
			val vdm = new Vdm()
			vdm.parse(line)
			val msg = AisMessage.getInstance(vdm)
			if(msg.isInstanceOf[AisPositionMessage])
				return msg
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
	
	val text = sc.wholeTextFiles("/user/hannesm/lsde/ais/10/01/*.txt.gz").values.flatMap(file => {
	  val lines = file.split("\n")
	  val id = lines.head.split(" ").head
	  lines.tail.map((id, _))
	})
	
	//decode and filter
	val data1 = text.filter(p => p._2.contains("!"))
	val data2 = data1.map(p => (p._1, decode(p._2.substring(p._2.indexOf("!")))))
	val decoded = data2.filter(p => if(p._2 == null) false else true)
	
	
	//####################Compute the outages########################################################
	//location = ((mmsi,timestamp), GeoLocation) 
	val location = decoded.map(p => ((p._2.getUserId(),p._1.substring(0,p._1.indexOf(".")).toLong), p._2.asInstanceOf[AisPositionMessage].getPos().getGeoLocation()))
	location.cache()
	
	//Check if GeoLocation is null
	val data5 = location.filter(p => if(p._2 == null) false else true)
	
	//get the distinct values
	val distinct = data5.map(kv => (kv._1,kv)).reduceByKey {case (a,b) => a}.map(_._2)

	//sort by (mmsi,timestamp)
	val sorted = distinct.sortBy(_._1)
	
	//get the gaps, the resulting type is (mmsi,gap,starttime, endtime, lat,long)
	//both latitude and longitude are truncated to reflect the "area"
	//key is here (mmsi,timestamp)
	var data = sorted.sliding(2).collect({case Array((key1, val1), (key2, val2)) if key1._1 == key2._1 => (key1._1, key2._2 - key1._2, getTimeKey(new DateTime(key1._2*1000).toDateTime), getTimeKey(new DateTime(key2._2*1000).toDateTime), (math floor val1.getLatitude() *10)/10, (math floor val1.getLongitude() *10)/10)})
	//data.cache()
 
	//check if mmsi number is correct
	var digitCheck = data.filter(p => if(p._1.toString.length==9)true else false)
	
	//gap interval between 20 mins to 10 hours
	var reduced = digitCheck.filter(p => if(p._2 > 1200 && p._2 < 36000) true else false)
	
	//var reduced = data.reduceByKey(Math.max(_, _))	
	reduced.saveAsTextFile("reduced_solutions")	
	
	
	//####################Connectivity for area########################################################
	
	def calculatePercentage(sendingShips: Int, gappingShips: Int) : Float = {
		val percentage = (100*gappingShips.toFloat)/sendingShips.toFloat
		return percentage	
	}
	
	//************transmitting ships****************
	//get Location information : format = ((lat,long,timekey), mmsi)
	var geo1 = data5.map(p => (((math floor p._2.getLatitude() *10)/10, (math floor p._2.getLongitude() *10)/10, getTimeKey(new DateTime(p._1._2*1000).toDateTime)),p._1._1))
	//get number of ships sending in the area and timeinterval
	var geo2 = geo1.distinct().groupByKey().map(p => (p._1,p._2.size))
	
	//***********not transmitting ships*****************
	//same idea, just for getting the number of ships that have the start of gaps in that place (there will be overlaps)
	var geogap = reduced.map(p => ((p._5,p._6,p._3),p._1))
	var geogap2 = geogap.distinct().groupByKey().map(p => (p._1,p._2.size))
	//((lat,lon,timekey),(size1,size2))
	
	//join the two rdds to calculate the connectivity
	var result = geo2.join(geogap2)
	//((lat,lon,timekey),percentage)
	var result1 = result.map(p => (p._1,(calculatePercentage(p._2._1,p._2._2))))
	//var percentages = result1.filter(p => if (p._2 <= 20) true else false)
	
	//save to hdfs
	//geo2.saveAsTextFile("geo_solutions")
	result.saveAsTextFile("joined_tables")
	result1.saveAsTextFile("percentages")
	
	//#################Ranking Outages######################################################
	
	def rankShip(gap: Long, percentage: Float, time : String) : Float = {
		//very basic version of ranking
		
		var rank = 0F
		
		//WE NEED TO NORMALIZE GAP TOO, but how??	
		//gap:seconds
		//turn into hours and add to rank
		rank = rank + gap/60
		
		//percentage:percentage of ships not sending messages
		//turn into 1 to 10 and add to rank
		rank = rank + percentage/10
		
		//time:day/hour, we only need to get hour and check (9 pm - 6 am night, otherwise day)
		//night - add 1 otherwise 0
		var hour : Int = time.substring(2).toInt
		if(hour < 21 && hour > 6){
			//do nothing
		}
		else{
			rank = rank+1
		}
		
		return rank
			
	}
	
	//reduced(mmsi, gap, starttime, endtime, lat, long) gaps of all ships
	//result1((lat,lon,timekey),percentage)
	//what we want to have (mmsi, gap, geo_percentage, starttime)
	//percentage = percentage of ships that have an outage
	
	val gapShips = reduced.map(p => ((p._5,p._6,p._3),(p._1,p._2))) //((lat,lon,startime),(mmsi,gap))
	val joinedGapShips = gapShips.join(result1)	//((lat,lon,starttime),((mmsi,gap),percentage))
	val rankingReady = joinedGapShips.map(p => (p._2._1._1, p._2._1._2, p._2._2, p._1._3)) //(mmsi, gap, geo_percentage, starttime)
	val rankedShips = rankingReady.map(p => (p._1,rankShip(p._2, p._3, p._4)))	//(mmsi, rank)
	
	
	rankedShips.saveAsTextFile("ranked_ships")
	
  }

}
