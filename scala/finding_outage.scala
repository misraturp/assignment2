import dk.dma.ais.sentence.Vdm
import dk.dma.ais.message.AisMessage
import dk.dma.ais.message.AisPositionMessage
import dk.dma.ais.message.AisMessageException
import dk.dma.ais.binary.SixbitException
import dk.dma.ais.sentence.SentenceException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.mllib.rdd.RDDFunctions._


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


val path :String = "/user/hannesm/lsde/ais/10/01/00-00.txt.gz,/user/hannesm/lsde/ais/10/01/00-01.txt.gz,/user/hannesm/lsde/ais/10/01/00-02.txt.gz,/user/hannesm/lsde/ais/10/01/00-03.txt.gz,/user/hannesm/lsde/ais/10/01/00-04.txt.gz,/user/hannesm/lsde/ais/10/01/00-05.txt.gz,/user/hannesm/lsde/ais/10/01/00-06.txt.gz"





val text = sc.wholeTextFiles(path).flatMapValues(y => y.split("\n"))
val text2 = text.map(p => {
	val index = p._1.indexOf("/ais/10/")
	(p._1.substring(index + 8,index + 16).replace("/","").replace("-",""),p._2)
})

val notime = text2.filter(p => p._2.contains("!"))
val not = notime.map(p => (p._1, p._2.substring(p._2.indexOf("!"))))
val decoded = not.map(p => (p._1, fun(p._2)))
val clean = decoded.filter(p => if(p._2 == null) false else true)

//rdd of online (mmsi,timestamp)
val mmsi = clean.map(p => (p._2.getUserId(),p._1.toInt))

val sorted = mmsi.sortBy(identity)

val partitionedAndSorted = mmsi.mapValues(_._1)
	.repartitionAndSortWithinPartitions(
    new org.apache.spark.HashPartitioner(sorted.partitions.size)
  )

val lagged = partitionedAndSorted.mapPartitions(_.sliding(2).collect {
  case Seq((key1, val1), (key2, val2)) if key1 == key2 => (key1, val2 - val1)
}, preservesPartitioning=true)

lagged.reduceByKey(Math.max(_, _)).take(10).foreach(println)






//val df = mmsi.mapValues(_._1).toDF("key", "timestamp")

//val keyTimestampWindow = Window.partitionBy("key").orderBy("timestamp")

//val withGap = df.withColumn(
//  "gap", $"timestamp" - lag("timestamp", 1).over(keyTimestampWindow)
//)

//val sorted = mmsi.mapValues(_._1).sortBy(identity)

//val res = sorted.sliding(2).collect {
//  case Array((key1, val1), (key2, val2)) if key1 == key2 => (key1, val2 - val1)
//}.reduceByKey(Math.max(_, _))

//res.take(10).foreach(println)




//spark-shell --jars ais-lib-messages-2.0.jar,enav-model-0.3.jar
