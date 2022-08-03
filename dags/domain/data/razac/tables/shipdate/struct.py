SHIPDATE_TABLE_STRUCTURE = """
    IMPO_ID_PROCESSO INTEGER,
    IMPO_CD_REF_EXTERNA VARCHAR(30),
    IMPO_SG_PAIS_EMBARQUE VARCHAR(3),
    IMPO_TP_PROCESSO VARCHAR(4000),
    IMPO_SG_INCOTERM VARCHAR(3),
    IMPO_NM_EXPORTADOR_SHIPPER VARCHAR(4000),
    IMPO_ST_SKF VARCHAR(4000),
    IMPO_CD_PRIORIDADE VARCHAR(4000),
    IMPO_DT_RECEBIMENTO_INVOICE DATE,
    IMPO_NM_MODAL VARCHAR(4000),
    IMPO_DT_ETD DATE,
    IMPO_DT_DEPARTURE DATE,
    IMPO_DT_ETA DATE,
    IMPO_DT_ARRIVAL DATE,
    IMPO_DT_PROVISAO_IMPOSTO DATE,
    IMPO_DT_PREV_REG_DI DATE,
    IMPO_DT_REGISTRO_DI DATE,
    IMPO_CD_DI VARCHAR(12),
    IMPO_CD_TRANSMISSAO_DI VARCHAR(10),
    IMPO_DT_PREV_CI DATE,
    IMPO_DT_CI DATE,
    IMPO_NM_ARMAZEM_GERAL VARCHAR(4000),
    IMPO_VL_FRETE_MOEDA_NEGOCIADA DECIMAL(256),
    IMPO_NM_MOEDA_FRETE VARCHAR(4000),
    IMPO_DT_PREV_CHEGADA_ARMZ_GERAL DATE,
    IMPO_DT_CHEGADA_ARM_GERAL DATE,
    IMPO_DT_PREV_FAT_RAZAC DATE,
    IMPO_DT_FAT_RAZAC DATE,
    IMPO_DT_SHIP_DATE DATE,
    IMPO_NM_AGENTE_DE_CARGAS VARCHAR(4000),
    IMPO_NM_PORTO_DE_ORIGEM VARCHAR(4000),
    IMPO_NM_PORTO_DE_DESTINO VARCHAR(4000),
    IMPO_CD_HBL_CRT VARCHAR(4000),
    IMPO_NU_CONTAINER VARCHAR(4000),
    IMPO_QT_FCL_20 INTEGER,
    IMPO_QT_FCL_40 INTEGER,
    IMPO_CD_CTNR_CONSOLIDADO VARCHAR(4000),
    IMPO_NM_LCL VARCHAR(3),
    IMPO_NM_FREE_TIME INTEGER,
    IMPO_ST_RECEBIMENTO_OHBL VARCHAR(2),
    IMPO_VL_TOTAL_DOLAR DECIMAL(256),
    IMPO_VL_TOTAL_REAIS_PREVISAO DECIMAL(256),
    IMPO_VL_PREV_IMPOSTO DECIMAL(256),
    IMPO_VL_PREV_FATURAMENTO DECIMAL(256),
    IMPO_VL_FATOR DECIMAL(256),
    IMPO_AL_IMP_NACIONAL DECIMAL(256),
    IMPO_VL_COTACAO_DOLAR DECIMAL(256),
    IMPO_QT_PESO_LIQUIDO DECIMAL(256),
    IMPO_QT_PESO_BRUTO DECIMAL(256),
    IMPO_QT_PACKS BIGINT,
    IMPO_QT_TRANSIT_TIME BIGINT,
    IMPO_DS_EXPORTADOR_AGRUPADO VARCHAR(4000),
    IMPO_DS_CONTAINER_AGRUPADO VARCHAR(4000),
    IMPO_DS_POS VARCHAR(4000),
    CTTD_ID NUMERIC(10),
    IMPO_CD_DESPACHANTE INTEGER
"""
