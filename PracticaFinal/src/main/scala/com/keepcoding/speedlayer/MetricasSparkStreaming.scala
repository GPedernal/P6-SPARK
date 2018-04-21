package com.keepcoding.speedlayer

import java.util.Properties

import com.keepcoding.dominio.{Cliente, Transaccion}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import java.text.SimpleDateFormat
import java.util.Date



object MetricasSparkStreaming {

  def run(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Práctica Final - Speed Layer - Kafka").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> "consumer-test-group"
    )


    val inputEnBruto: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, String](Array(s"${args(0)}"), kafkaParams))

    val transaccionesStream: DStream[String] = inputEnBruto.map(_.value())


    val acumulador: LongAccumulator = ssc.sparkContext.longAccumulator("dniCliente")
    val acumuladorTransaccion: LongAccumulator = ssc.sparkContext.longAccumulator("dniTransaccion")
    val acumuladorOcio: LongAccumulator = ssc.sparkContext.longAccumulator("IDocio")
    val acumuladorFecha: LongAccumulator = ssc.sparkContext.longAccumulator("IDFecha")

    val streamCliente = transaccionesStream.map(_.toString.split(",")).map(evento => {

      acumulador.add(1)


      Cliente(acumulador.value.toLong, evento(4)toString, evento(6).toString, 0, evento(3).toString, evento(5).toString)

    })

    val streamTransaccion = transaccionesStream.map(_.toString.split(",")).map(evento => {

      acumuladorTransaccion.add(1)

      Transaccion(acumuladorTransaccion.value.toLong,
        evento(2).toDouble, evento(10).toString, "N/A", evento(3)toString, evento(5).toString, evento(0).toString)

    })

    streamCliente.print()

    /*
    He abordado las tareas con transformaciones de spark en vez de trabajar con los objetos
    cliente y transacciones porque me daban muchos problemas al gestionar el producer. De este modo
    les doy un enfoque diferente al que les dí al trabajar en la capa batch.
    Después de mucho pelearme he conseguido que las cosas funcionen más o menos como deben y por cada mensaje
    que publico en TopicoInput aparezca uno transformado en los topics pasados como argumento.
    No en todos los casos el mensaje que recibo es el que esperaba pero con el tiempo invertido/disponible me doy
    por satisfecho en este punto.
     */


    /*
    TAREA 1: Número de transacciones por ciudad.
     */
    val dsTarea1 = transaccionesStream.map(_.toString.split(",")(5)).map(x=> (x,1)).reduceByKey(_+_)
      .map(tupla => tupla.swap).transform(rdd => rdd.sortByKey(false)).map(_._1.toString)


    dsTarea1.foreachRDD(rdd => rdd.foreachPartition(writeToKafka1(s"${args(1)}")))

    def writeToKafka1(str: String)(partitionOfRecords: Iterator[String]): Unit = {
      val producer1 = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer1.send(new ProducerRecord[String, String](s"${args(1)}", data.toString)))
      producer1.flush()
      producer1.close()
    }


    /*
    TAREA 2: Clientes con pagos superiores a 5000
    */

    val dsTarea2 = transaccionesStream.map(x =>{
      (x.toString.split(",")(4),x.toString.split(",")(2).toDouble)
    }).filter(_._2 > 5000).map(_._2.toString)

    dsTarea2.foreachRDD(rdd => rdd.foreachPartition(writeToKafka2(s"${args(2)}")))

    def writeToKafka2(str: String)(partitionOfRecords: Iterator[String]): Unit = {
      val producer2 = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer2.send(new ProducerRecord[String, String](s"${args(2)}", data.toString)))
      producer2.flush()
      producer2.close()
    }

    /*
    TAREA 3: Transacciones por cliente en cada ciudad
     */
    val dsTarea3 = transaccionesStream.map(x =>{
      (x.toString.split(",")(4),x.toString.split(",")(5))
    }).map(par=> (par,1)).reduceByKey(_+_).map(tupla => tupla.swap).transform(rdd => rdd.sortByKey(false))
        .map(_._1.toString)

    dsTarea3.foreachRDD(rdd => rdd.foreachPartition(writeToKafka3(s"${args(3)}")))

    def writeToKafka3(str: String)(partitionOfRecords: Iterator[String]): Unit = {
      val producer3 = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer3.send(new ProducerRecord[String, String](s"${args(3)}", data.toString)))
      producer3.flush()
      producer3.close()
    }

    /*
    TAREA 4: Operaciones con categoría ocio
     */
    val dsTarea4 = transaccionesStream.map(x =>{

      acumuladorOcio.add(1)

      (acumuladorOcio.value, x.toString.split(",")(10))

    }).filter(_._2 == "Cinema").filter(_._2 == "Restaurant").filter(_._2 == "Sports").map(_._1.toString)

    dsTarea4.foreachRDD(rdd => rdd.foreachPartition(writeToKafka4(s"${args(4)}")))

    def writeToKafka4(str: String)(partitionOfRecords: Iterator[String]): Unit = {
      val producer4 = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer4.send(new ProducerRecord[String, String](s"${args(4)}", data.toString)))
      producer4.flush()
      producer4.close()
    }

    /*
    TAREA 5: Transacciones de cada cliente en ultimos 10 días (21 al 31 de enero de 2009)
     */

    val formato = new java.text.SimpleDateFormat("[MM/dd/yy HH:mm")
    val dsTarea5 = transaccionesStream.map(x =>{

      acumuladorFecha.add(1)

      (acumuladorFecha.value, x.toString.split(",")(0))

    }).filter(campo =>formato.parse(campo._2).after(new java.util.Date("01/20/2009"))).map(_._2.toString)


    dsTarea5.foreachRDD(rdd => rdd.foreachPartition(writeToKafka5(s"${args(5)}")))

    def writeToKafka5(str: String)(partitionOfRecords: Iterator[String]): Unit = {
      val producer5 = new KafkaProducer[String, String](getKafkaConfig())
      partitionOfRecords.foreach(data => producer5.send(new ProducerRecord[String, String](s"${args(5)}", data.toString)))
      producer5.flush()
      producer5.close()
    }

    /*
    2 MÉTRICAS ADICIONALES

    1: Ingresos de cada compañía de tarjetas de crédito por comisiones -> Supondremos una comisión del 2% de cada transacción
     */

    val metrica1 = transaccionesStream.map(x =>{
      (x.toString.split(",")(3),x.toString.split(",")(2).toDouble)
    }).reduceByKey(_+_).map(_._2*0.02)

    metrica1.saveAsTextFiles(s"${args(6)}","txt")

    /*
    2: Clientes que viven de alquiler -> Aquellos con pagos cuya descripción es leasing.
     */

    val metrica2 = transaccionesStream.map(x =>{
      (x.toString.split(",")(4), x.toString.split(",")(10))
    }).filter(_._2 == "leasing").map(_._1.toString)

    metrica2.saveAsTextFiles(s"${args(7)}", "txt")


    ssc.start()
    ssc.awaitTermination()

  }

  def getKafkaConfig(): Properties = {

    val prop = new Properties()

    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer" )
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    prop
  }

}
