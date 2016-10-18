//spark-shell --jars ais-lib-messages-2.0.jar,enav-model-0.3.jar

import dk.dma.ais.sentence.Vdm
import dk.dma.ais.message.AisMessage
import dk.dma.ais.message.AisPositionMessage
import dk.dma.ais.message.AisMessageException
import dk.dma.ais.binary.SixbitException
import dk.dma.ais.sentence.SentenceException
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{NewHadoopRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

val text = sc.textFile("/user/lsde10/ais/10/01/*.txt.gz")
val notime = text.filter(line => line.contains("!") )
val not = notime.map(line => line.substring(line.indexOf("!")))

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

/****test****/
val line = "!AIVDM,1,1,,A,23aIhd@P1@PHRwPM`<U@`OvN2>`<,0*4C"
val hele = fun(line)
val posmsg = hele.asInstanceOf[AisPositionMessage]
val pos = posmsg.getPos()
val res = pos.getGeoLocation()
//31067285,3217392
//51.778805,5.36232
//////////////////////////

val decoded = not.map(line => fun(line))
val clean = decoded.filter(msg => if(msg == null) false else true)

val values = clean.map(line => line.asInstanceOf[AisPositionMessage])

val latitudes = values.map(line => line.getPos().getGeoLocation())
val cleanL = latitudes.filter(msg => if(msg == null) false else true)
val latClean = cleanL.map(line => line.getLatitude())
val maxLat = latClean.max()
//90.0
val minLat = latClean.min()
//-90.0

val longitudes = values.map(line => line.getPos().getGeoLocation())
val cleanN = longitudes.filter(msg => if(msg == null) false else true)
val lonClean = cleanN.map(line => line.getLongitude())
val maxLon = lonClean.max()
//179.4029
val minLon = lonClean.min()
//-179.987243

(maxLat,maxLon,minLat,minLon)
//OK until here
