SELECT  PDV.NUM_PEDIDO                 AS idExterno
      ,PDV.EST_ID_FAT                 AS idExterno_Contato
      ,PDV.EMPR_ID                    AS idExterno_organizacao
      ,EST.DESCRICAO                  AS nome
      ,'Ganha'                        AS statusNegociacao
      ,PDV.VLR_BRUTO                  AS valor
      ,PDV.OBS                        AS observacoes
      ,PDV.DT_GERACAO                 AS criadaEm
      ,ITE.COD_ITEM || ',' || ITE.DESC_TECNICA
                                      AS idExterno_produto
      ,ITPDV.VLR_BRUTO                AS valorUnitario
      ,ITPDV.QTDE                     AS quantidade
      ,CASE
        WHEN ITPDV.VLR_DESC > 0
         THEN TRUNC((((ITPDV.VLR_DESC ) / ITPDV.VLR_BRUTO) * 100),5)
       END                            AS percentualDesconto
      ,ITPDV.VLR_BRUTO * ITPDV.QTDE   AS valorTotal
      ,ITPDV.OBS                      AS comentarios
  FROM TPEDIDOS_VENDA PDV    INNER JOIN TITENS_PDV        ITPDV
       ON (PDV.ID    = ITPDV.PDV_ID)
                             INNER JOIN TITENS_COMERCIAL  ITCM
       ON (ITCM.ID   = ITPDV.ITCM_ID)
                             INNER JOIN TITENS_EMPR       ITEMPR
       ON (ITEMPR.ID = ITCM.ITEMPR_ID
        AND ITEMPR.EMPR_ID = PDV.EMPR_ID)
                             INNER JOIN TITENS            ITE
       ON (ITE.ID   = ITEMPR.ITEM_ID)
                             INNER JOIN TESTABELECIMENTOS EST
       ON (EST.ID    = PDV.EST_ID_FAT)
WHERE PDV.SIT_PDV <> 'C'
order by criadaem desc
