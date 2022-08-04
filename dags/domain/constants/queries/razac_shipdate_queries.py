INSERT_INTO_RAZAC_SHIPDATE = """
INSERT INTO public.{table_name} (
    impo_id_processo,
    impo_cd_ref_externa,
    impo_sg_pais_embarque,
    impo_tp_processo,
    impo_sg_incoterm,
    impo_nm_exportador_shipper,
    impo_st_skf,
    impo_cd_prioridade,
    impo_dt_recebimento_invoice,
    impo_nm_modal,
    impo_dt_etd,
    impo_dt_departure,
    impo_dt_eta,
    impo_dt_arrival,
    impo_dt_provisao_imposto,
    impo_dt_prev_reg_di,
    impo_dt_registro_di,
    impo_cd_di,
    impo_cd_transmissao_di,
    impo_dt_prev_ci,
    impo_dt_ci,
    impo_nm_armazem_geral,
    impo_vl_frete_moeda_negociada,
    impo_nm_moeda_frete,
    impo_dt_prev_chegada_armz_geral,
    impo_dt_chegada_arm_geral,
    impo_dt_prev_fat_razac,
    impo_dt_fat_razac,
    impo_dt_ship_date,
    impo_nm_agente_de_cargas,
    impo_nm_porto_de_origem,
    impo_nm_porto_de_destino,
    impo_cd_hbl_crt,
    impo_nu_container,
    impo_qt_fcl_20,
    impo_qt_fcl_40,
    impo_cd_ctnr_consolidado,
    impo_nm_lcl,
    impo_nm_free_time,
    impo_st_recebimento_ohbl,
    impo_vl_total_dolar,
    impo_vl_total_reais_previsao,
    impo_vl_prev_imposto,
    impo_vl_prev_faturamento,
    impo_vl_fator,
    impo_al_imp_nacional,
    impo_vl_cotacao_dolar,
    impo_qt_peso_liquido,
    impo_qt_peso_bruto,
    impo_qt_packs,
    impo_qt_transit_time,
    impo_ds_exportador_agrupado,
    impo_ds_container_agrupado,
    impo_ds_pos,
    cttd_id,
    impo_cd_despachante
) VALUES (
    '{impo_id_processo}',
    left('{impo_cd_ref_externa}', 30),
    left('{impo_sg_pais_embarque}', 3),
    '{impo_tp_processo}',
    left('{impo_sg_incoterm}', 3),
    '{impo_nm_exportador_shipper}',
    '{impo_st_skf}',
    '{impo_cd_prioridade}',
    '{impo_dt_recebimento_invoice}',
    '{impo_nm_modal}',
    '{impo_dt_etd}',
    '{impo_dt_departure}',
    '{impo_dt_eta}',
    '{impo_dt_arrival}',
    '{impo_dt_provisao_imposto}',
    '{impo_dt_prev_reg_di}',
    '{impo_dt_registro_di}',
    left('{impo_cd_di}', 12),
    left('{impo_cd_transmissao_di}', 10),
    '{impo_dt_prev_ci}',
    '{impo_dt_ci}',
    '{impo_nm_armazem_geral}',
    {impo_vl_frete_moeda_negociada},
    '{impo_nm_moeda_frete}',
    '{impo_dt_prev_chegada_armz_geral}',
    '{impo_dt_chegada_arm_geral}',
    '{impo_dt_prev_fat_razac}',
    '{impo_dt_fat_razac}',
    '{impo_dt_ship_date}',
    '{impo_nm_agente_de_cargas}',
    '{impo_nm_porto_de_origem}',
    '{impo_nm_porto_de_destino}',
    '{impo_cd_hbl_crt}',
    '{impo_nu_container}',
    {impo_qt_fcl_20},
    {impo_qt_fcl_40},
    '{impo_cd_ctnr_consolidado}',
    left('{impo_nm_lcl}', 3),
    left('{impo_nm_free_time}', 30),
    left('{impo_st_recebimento_ohbl}', 2),
    {impo_vl_total_dolar},
    {impo_vl_total_reais_previsao},
    {impo_vl_prev_imposto},
    {impo_vl_prev_faturamento},
    {impo_vl_fator},
    {impo_al_imp_nacional},
    {impo_vl_cotacao_dolar},
    {impo_qt_peso_liquido},
    {impo_qt_peso_bruto},
    {impo_qt_packs},
    {impo_qt_transit_time},
    '{impo_ds_exportador_agrupado}',
    '{impo_ds_container_agrupado}',
    '{impo_ds_pos}',
    {cttd_id},
    {impo_cd_despachante}
)
"""
