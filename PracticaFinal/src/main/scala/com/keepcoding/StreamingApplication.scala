package com.keepcoding

import com.keepcoding.speedlayer.MetricasSparkStreaming

object StreamingApplication {

  def main(args: Array[String]): Unit = {

    if (args.length==8) {
      MetricasSparkStreaming.run(args)
    }else {
      println("Se está intentando arrancar el job de Spark sin los parámetros correctos.")
    }
  }

}
