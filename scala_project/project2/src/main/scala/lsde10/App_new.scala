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

/**
 * @author ${user.name}
 */
object App {
	
	//spark-shell --jars ais-lib-messages-2.0.jar,enav-model-0.3.jar --master yarn
	//spark-submit --class lsde10.App --master yarn --deploy-mode cluster project2-0.0.1-jar-with-dependencies2.jar

  def main(args : Array[String]) {
  
	def fun (line: String) : AisMessage = {
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
  
  
    val conf = new SparkConf().setAppName("Finding Outage")
	val sc = new SparkContext(conf)
	
	val text = sc.wholeTextFiles("/user/hannesm/lsde/ais/10/01/*.txt.gz").values.flatMap(file => {
	  val lines = file.split("\n")
	  val id = lines.head.split(" ").head
	  lines.tail.map((id, _))
	})
	
	val data1 = text.filter(p => p._2.contains("!"))
	val data2 = data1.map(p => (p._1, fun(p._2.substring(p._2.indexOf("!")))))
	val data3 = data2.filter(p => if(p._2 == null) false else true)
	val data4 = data3.map(p => (p._2.getUserId(),p._1.substring(0,p._1.indexOf(".")).toInt))
	val data5 = data4.distinct()
	val data6 = data5.sortBy(identity)
	var data = data6.sliding(2).collect({case Array((key1, val1), (key2, val2)) if key1 == key2 => (key1, val2 - val1)})
	data.cache()

	var digitCheck = data.filter(p => if(p._1.toString.length==9)true else false)
	
	//gap interval between 20 mins to 10 hours
	var reduced = data.filter(p => if(p._2 > 1200 && p._2 < 36000) true else false)
	//var reduced = data.reduceByKey(Math.max(_, _))	
	reduced.saveAsTextFile("reduced_solutions")
	//hdfs dfs -rm -r <dir_name>
	//hdfs dfs -copyToLocal <input> <output>
  }

}
