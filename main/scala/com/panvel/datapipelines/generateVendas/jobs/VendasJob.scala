package com.panvel.datapipelines.generateVendas.jobs

import com.panvel.datapipelines.generateVendas.commons.session.SparkSessionWrapper
import com.panvel.datapipelines.generateVendas.commons.dataframe.RunnableJob
import com.panvel.datapipelines.generateVendas.books.Variables._
import com.panvel.datapipelines.generateVendas.books.Transformations._
import com.panvel.datapipelines.generateVendas.commons.dataframe.CommonConstants._
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.typesafe.config.{Config, ConfigFactory}

object VendasJob extends RunnableJob with SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    runJob()
  }

  override def runJob(): Unit = {
    val conf: Config = ConfigFactory.load

    //carrega tabelas
    val vendasRawDf: DataFrame =
      spark.read.parquet(conf.getString(VENDAS_RAW_PATH))
        .select(vendasColSeq: _*)

    val pedidosRawDf: DataFrame =
      spark.read.parquet(conf.getString(PEDIDOS_RAW_PATH))
      .select(pedidosColSeq: _*)

    val itensVendasRawDf: DataFrame =
      spark.read.parquet(conf.getString(ITENS_VENDAS_RAW_PATH))
        .select(itensVendasColSeq: _*)

    val pedidoVendaRawDf: DataFrame =
      spark.read.parquet(conf.getString(PEDIDO_VENDA_RAW_PATH))
        .select(pedidoVendaColSeq: _*)

    generateVendas(
      vendasRawDf,
      pedidosRawDf,
      itensVendasRawDf,
      pedidoVendaRawDf)
    .write.mode(SaveMode.Overwrite)
      .parquet(conf.getString(VENDAS_OUTPUT_PATH))

  }
}
