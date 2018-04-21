package com.keepcoding

import com.keepcoding.batchlayer.MetricasSparkSQL

object BatchApplication {

  def main(args: Array[String]): Unit = {

    if (args.length==6) {
      MetricasSparkSQL.run(args)
    }else {
      println("Se está intentando arrancar el job de Spark sin los parámetros correctos.")
    }
  }

}
