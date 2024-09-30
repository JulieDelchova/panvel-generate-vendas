package com.panvel.datapipelines.generateVendas.books

import com.panvel.datapipelines.generateVendas.books.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class Functions {

  def createCanalVendaCol(df: DataFrame): DataFrame = {
    df.withColumn(CANAL_VENDA,
      when(col(CANAL_VENDA) === "L", "Loja")
      .otherwise(when(col(CANAL_VENDA) === "S", "Site")
        .otherwise(when(col(CANAL_VENDA) === "A", "App")
          .otherwise(null))))
  }

  def createTipoDescontoCol(df: DataFrame) = {
    df
      .withColumn(TIPO_DESCONTO,
        when(col(N_VLR_DESC) > 0 && trim(col(V_IT_VD_CONV)) === "SIM", "Convênio")
          .otherwise(
            when(col(N_VLR_DESC) > 0, "Promoção")
              .otherwise(null)))
  }

  def createValorUnitario(df: DataFrame): DataFrame = {
    df.withColumn(VALOR_UNITARIO, (col(N_VLR_VD)/col(QUANTIDADE)).cast(DecimalType(18,2)))
  }

  def treatNull(df: DataFrame): DataFrame = {
    df.na.fill("-1")
      .na.fill(-1)
  }

}
