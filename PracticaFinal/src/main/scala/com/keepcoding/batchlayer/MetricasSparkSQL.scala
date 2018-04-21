package com.keepcoding.batchlayer

import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_timestamp


object MetricasSparkSQL {

  def run(args: Array[String]): Unit ={

    val sparkSession = SparkSession.builder().master("local[*]")
              .appName("Práctica Final - Batch Layer - Spark SQL")
              .getOrCreate()

    import sparkSession.implicits._

    val rddTransacciones = sparkSession.read.csv(s"file:///${args(0)}")

    val cabecera = rddTransacciones.first

    val rddSinCabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString.split(","))

    val acumulador: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniCliente")
    val acumuladorTransaccion: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniTransaccion")
    val acumuladorGeoloc: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniGeoloc")

     /*
    Cliente(ID: Long, nombre: String, cuentaCorriente: String, balance: Double, tarjetaCredito: String, ciudad: String)
     */
    val dfClientes = rddSinCabecera.map(columna => {

      acumulador.add(1)

      Cliente(acumulador.value, columna(4), columna(6), 0, columna(3), columna(5))
    })

    dfClientes.show(10)

    /*
    Transaccion(ID: Long, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String,
                       ciudad: String, fecha: String)
     */
    val dfTransacciones = rddSinCabecera.map(columna => {

      acumuladorTransaccion.add(1)

      Transaccion(acumuladorTransaccion.value.toLong, columna(2).toDouble, columna(10), "N/A", columna(3), columna(5), columna(0))

    })

    dfTransacciones.show(10)

    /*
    Geolocalizacion(ID: Long, latitud:Double, longitud: Double, ciudad: String, pais: String)
     */
    val dfGeolocalizacion = rddSinCabecera.map(columna => {

      acumuladorGeoloc.add(1)

      Geolocalizacion(acumuladorGeoloc.value.toLong, columna(8).toDouble, columna(9).toDouble, columna(5), "N/A")

    })

    dfGeolocalizacion.show(10)

    /*
    TABLAS:
     */

    dfClientes.createOrReplaceGlobalTempView("CLIENTES")
    dfTransacciones.createOrReplaceGlobalTempView("TRANSACCIONES")
    dfGeolocalizacion.createOrReplaceGlobalTempView("GEOLOC")

    /*
    TAREA 1: Agrupar clientes por ciudad. Número de transacciones por ciudad.
     */
    val dfTAREA1 = sparkSession.sql("SELECT COUNT (ID), ciudad FROM global_temp.CLIENTES GROUP BY ciudad")

    dfTAREA1.show(20)

    sparkSession.sql("SELECT ID, nombre, ciudad FROM global_temp.CLIENTES WHERE ciudad='Amsterdam'").show(5)

    /*
    dfTAREA1.write.csv(s"file:///${args(1)}") -> Comento una vez creado
     */

    /*
    TAREA 2: Clientes con pagos superiores a 5000
     */
    val dfTAREA2 = sparkSession.sql("SELECT c.ID, c.nombre FROM global_temp.TRANSACCIONES t " +
      "JOIN global_temp.CLIENTES c ON c.ID=t.ID WHERE t.importe>5000")

    dfTAREA2.show(20)

    /*
    dfTAREA2.write.parquet(s"file:///${args(2)}") -> Comento una vez creado
     */

    /*
    TAREA 3: Transacciones agrupadas por cliente en cada ciudad
     */
    val dfTAREA3 = sparkSession.sql("SELECT COUNT (ID), nombre, ciudad FROM global_temp.CLIENTES" +
      " GROUP BY nombre, ciudad")

    dfTAREA3.show(20)

    /*
    dfTAREA3.write.json(s"file:///${args(3)}") -> Comento una vez creado
     */

    /*
    OBJETO TRANSACCION:
     */

    val tiempo = to_timestamp($"fecha", "[MM/dd/yy HH:mm")

    val dfTransaccionesFinal = dfTransacciones.withColumn("categoria",
      when($"descripcion"==="Cinema]" or $"descripcion"==="Restaurant]"
        or $"descripcion"==="Sports]","Ocio").otherwise("N/A")).withColumn("fecha", tiempo)

    dfTransaccionesFinal.createOrReplaceGlobalTempView("TRANSACCIONES")

    /*
    TAREA 4: Operaciones con categoria Ocio
     */
    val dfTAREA4 = sparkSession.sql("SELECT ID FROM global_temp.TRANSACCIONES" +
      " WHERE categoria='Ocio'")

    dfTAREA4.show(20)

    /*
    dfTAREA4.write.csv(s"file:///${args(4)}") -> Comento una vez creado
     */

    /*
    TAREA 5: Transacciones de cada cliente en ultimos 10 días (21 al 31 de enero de 2009)
     */
    val dfTAREA5 = sparkSession.sql("SELECT ID FROM global_temp.TRANSACCIONES" +
      " WHERE fecha>'2009-01-20'")

    dfTAREA5.show(20)

    /*
    dfTAREA5.write.parquet(s"file:///${args(5)}") -> Comento una vez creado
     */

    /*
    TAREA 6 (OPCIONAL): País correspondiente a cada coordenada y almacenarlo en su columna


    val json = scala.io.Source.fromURL(s"https:" +
      s"//maps.googleapis.com/maps/api/geocode/json?latlng=${dfGeolocalizacion.select("latitud").take(1)}," +
      s"${dfGeolocalizacion.select("longitud").take(1)}&key=AIzaSyCUyoBE1aFV7rXeLHqLD0_3aM0ggDi67Co").mkString.split("\n")

    val jsonRDD = sparkSession.sparkContext.parallelize(json).foreach(println)

    Intentando la tarea opcional se me ocurre algo así. Habría que iterar los campos de longitud y latitud recorriendo toda la tabla
    e idealmente guardando el resultado del país a la vez. Quizá se podría guardar el archivo de la API de google para leerlo como json
    y así aprovechar su esquema al crear el rdd, aunque supongo que iterar ese proceso es complicado.

    No pruebo mucho más porque no tengo el tiempo que me gustaría para pensarlo en profundidad y además me temo que mi ordenador sufre
    un montón con este tipo de operaciones y no creo que le dé si hay que recorrer la tabla entera!

    Con esto doy por finalizada la parte batch.*/

  }

}
