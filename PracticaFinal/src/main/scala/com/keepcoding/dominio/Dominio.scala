package com.keepcoding.dominio

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Geolocalizacion(ID: Long, latitud:Double, longitud: Double, ciudad: String, pais: String)

case class Transaccion(ID: Long, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String,
                       ciudad: String, fecha: String)

case class Cliente(ID: Long, nombre: String, cuentaCorriente: String, balance: Double, tarjetaCredito: String, ciudad: String)


