package com.panvel.datapipelines.generateVendas.books

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import com.panvel.datapipelines.generateVendas.commons.session.SparkSessionWrapper
import com.panvel.datapipelines.generateVendas.books.Functions
import com.panvel.datapipelines.generateVendas.books.Constants._
import com.panvel.datapipelines.generateVendas.books.Variables._

object Transformations extends SparkSessionWrapper {

  def generateVendas(vendasRawDf: DataFrame,
                     pedidosRawDf: DataFrame,
                     itensVendasRawDf: DataFrame,
                     pedidoVendaRawDf: DataFrame): DataFrame = {

    val functions: Functions = new Functions

    val vendasTransformedDf: DataFrame =
        vendasRawDf
          .filter(col(CODIGO_FILIAL).isNotNull && col(CODIGO_CUPOM_VENDA).isNotNull)
          .dropDuplicates()

    val pedidosTransformedDf: DataFrame =
        pedidosRawDf
          .join(pedidoVendaRawDf, Seq(N_ID_PDD), "inner")
          .filter(col(CODIGO_FILIAL).isNotNull && col(CODIGO_CUPOM_VENDA).isNotNull)
          .transform(functions.createCanalVendaCol)
          .select(CODIGO_FILIAL, CODIGO_CUPOM_VENDA, CANAL_VENDA)

    val itensVendasTransformedDf: DataFrame =
      itensVendasRawDf
        .filter(col(CODIGO_FILIAL).isNotNull && col(CODIGO_CUPOM_VENDA).isNotNull)
        .dropDuplicates()
        .transform(functions.createTipoDescontoCol)
        .transform(functions.createValorUnitario)
        .select(itensVendasSelCol: _*)

    val vendasDf = itensVendasTransformedDf
      .join(vendasTransformedDf, Seq(CODIGO_FILIAL, CODIGO_CUPOM_VENDA), "inner")
      .join(pedidosTransformedDf, Seq(CODIGO_FILIAL, CODIGO_CUPOM_VENDA), "inner")
      .transform(functions.treatNull)

    vendasDf
  }
}