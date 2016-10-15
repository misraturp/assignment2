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


var path :String = "/user/hannesm/lsde/ais/10/01/*.txt.gz"

var text = sc.wholeTextFiles(path).flatMapValues(y => y.split("\n"))

var data = text.map(p => {
		val index = p._1.indexOf("/ais/10/")
		(p._1.substring(index + 8,index + 16).replace("/","").replace("-",""),p._2)
	}).filter(p => p._2.contains("!")).map(p => (p._1, fun(p._2.substring(p._2.indexOf("!"))))).filter(p => if(p._2 == null) false else true).map(p => (p._2.getUserId(),(p._1.toInt, p._2.asInstanceOf[AisPositionMessage]))).sortBy(p => (p._1,p._2._1)).sliding(2).collect({
		case Array((key1, val1), (key2, val2)) if key1 == key2 => (key1, val2._1 - val1._1)
	})
	
var reduced = data.reduceByKey(Math.max(_, _))	
reduced.take(10).foreach(println)