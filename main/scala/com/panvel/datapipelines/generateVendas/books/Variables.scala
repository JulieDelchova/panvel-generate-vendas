package com.panvel.datapipelines.generateVendas.books

import com.panvel.datapipelines.generateVendas.books.Constants._
import com.panvel.datapipelines.generateVendas.books.Functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Variables {

  val functions: Functions = new Functions

  val vendasColSeq = Seq(
    col(D_DT_VD).cast(DateType).as(DATA_EMISSAO),
    col(N_ID_FIL).cast(LongType).as(CODIGO_FILIAL),
    col(N_ID_VD_FIL).cast(LongType).as(CODIGO_CUPOM_VENDA),
    col(V_CLI_COD).cast(StringType).as(CODIGO_CLIENTE)
  )

  val itensVendasColSeq = Seq(
      col(N_ID_FIL).cast(LongType).as(CODIGO_FILIAL),
      col(N_ID_VD_FIL).cast(LongType).as(CODIGO_CUPOM_VENDA),
      col(N_ID_IT).cast(LongType).as(CODIGO_ITEM),
      col(V_IT_VD_CONV).cast(StringType),
      col(N_VLR_VD).cast(DecimalType(18, 2)),
      col(N_VLR_DESC).cast(DecimalType(18, 2)),
      col(N_QTD).cast(IntegerType).as(QUANTIDADE)
  )

  val pedidoVendaColSeq = Seq(
    col(N_ID_FIL).cast(LongType).as(CODIGO_FILIAL),
    col(N_ID_VD_FIL).cast(LongType).as(CODIGO_CUPOM_VENDA),
    col(N_ID_PDD).cast(LongType)
  )

  val pedidosColSeq = Seq(
    col(N_ID_PDD).cast(LongType),
    col(V_CNL_ORIG_PDD).cast(StringType).as(CANAL_VENDA)
  )

  val itensVendasSelCol: Seq[Column] = Seq(
    col(CODIGO_FILIAL),
    col(CODIGO_CUPOM_VENDA),
    col(CODIGO_ITEM),
    col(QUANTIDADE),
    col(TIPO_DESCONTO),
    col(VALOR_UNITARIO)
  )




}