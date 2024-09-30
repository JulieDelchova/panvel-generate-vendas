package com.panvel.datapipelines.generateVendas.books

object Constants {

  /**
   * Column Names from the Vendas final table
   */
  val CODIGO_FILIAL: String = "codigo_filial"
  val CODIGO_CUPOM_VENDA: String = "codigo_cupom_venda"
  val DATA_EMISSAO: String = "data_emissao"
  val CODIGO_ITEM: String = "codigo_item"
  val VALOR_UNITARIO: String = "valor_unitario"
  val QUANTIDADE: String = "quantidade"
  val CODIGO_CLIENTE: String = "codigo_cliente"
  val TIPO_DESCONTO: String = "tipo_desconto"
  val CANAL_VENDA: String = "canal_venda"

  /**
   * Columns from vendas raw table
   */
  val D_DT_VD: String = "d_dt_vd"
  val N_ID_FIL: String = "n_id_fil"
  val N_ID_VD_FIL: String = "n_id_vd_fil"
  val V_CLI_COD: String = "v_cli_cod"
  val N_VLR_TOT_VD: String = "n_vlr_tot_vd"
  val N_VLR_TOT_DESC: String = "n_vlr_tot_desc"
  val V_CPN_EML: String = "v_cpn_eml"
  val TP_PGT: String = "tp_pgt"

  /**
   * Column names from pedidos raw tables
   */
  val N_ID_PDD: String = "n_id_pdd"
  val D_DT_EFT_PDD: String = "d_dt_eft_pdd"
  val D_DT_ENTR_PDD: String = "d_dt_entr_pdd"
  val V_CNL_ORIG_PDD: String = "v_cnl_orig_pdd"
  val V_UF_ENTR_PDD: String = "v_uf_entr_pdd"
  val V_LC_ENT_PDD: String = "v_lc_ent_pdd"
  val N_VLR_TOT_PDD: String = "n_vlr_tot_pdd"

  /**
   *  Column names from itens_vendas Raw table
   */
  val N_ID_IT: String = "n_id_it"
  val V_RC_ELT: String = "v_rc_elt"
  val V_IT_VD_CONV: String = "v_it_vd_conv"
  val N_VLR_PIS: String = "n_vlr_pis"
  val N_VLR_VD: String = "n_vlr_vd"
  val N_VLR_DESC: String = "n_vlr_desc"
  val N_QTD: String = "n_qtd"

}