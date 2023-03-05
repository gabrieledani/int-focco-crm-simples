SELECT PDV.NUM_PEDIDO                                   AS idExterno
      ,cli.COD_CLI||'-'||est.COD_EST                    AS idExterno_Contato
      ,NULL                                             AS idExterno_organizacao
      ,'Negociacao '|| focco3i_util.Converte_acentos(cli.descricao) ||'-Numero Pedido: '||PDV.NUM_PEDIDO AS nome
      ,(CASE WHEN PDV.VLR_FRETE > 0 
            THEN '-Valor de Frete: '||TO_CHAR(PDV.VLR_FRETE,'FM999G999G990D00')
            ELSE NULL
        END || 
        CASE WHEN PDV.VLR_DESC > 0 
            THEN'-Percentual de desconto: '||TO_CHAR(((PDV.VLR_DESC / PDV.VLR_BRUTO)*100),'FM999G999G990D00')
            ELSE NULL
        END ||
        CASE WHEN PDV.FORN_ID IS NOT NULL
            THEN '-Transportadora: '||(SELECT focco3i_util.Converte_acentos(FO.DESCRICAO) FROM TFORNECEDORES FO WHERE PDV.FORN_ID = FO.ID)
            ELSE NULL
        END)                                            AS descricao
      ,PDV.EMPR_ID                                      AS categoriaNegociacao
      ,1671                                             AS idEtapaNegociacao
      ,'Pendente'                                       AS statusNegociacao
      ,PDV.VLR_BRUTO                                    AS valor
      ,NVL((SELECT ID FROM TUSUARIOS WHERE LOGIN = TRIM(SUBSTR(PDV.USUARIO,1,INSTR(PDV.USUARIO,'(')-1))),1) AS idExternoUsuarioInclusao
      ,PDV.DT_EMIS                                      AS criadaEm
      ,ITE.COD_ITEM                                     AS idExterno_produto
      ,ITPDV.VLR_BRUTO                                 AS valorUnitario
      ,ITPDV.QTDE                                       AS quantidade
      ,CASE
        WHEN ITPDV.VLR_DESC > 0
         THEN TRUNC((((ITPDV.VLR_DESC ) / ITPDV.VLR_BRUTO) * 100),5)
       END                                              AS percentualDesconto
      ,ITPDV.VLR_BRUTO * ITPDV.QTDE                     AS valorTotal 
      ,focco3i_util.Converte_acentos(ITE.DESC_TECNICA)                                 AS comentarios
      ,'Previsao de Entrega: '||TO_CHAR(ITPDV.DT_ENTREGA,'DD/MM/RRRR')||
      CASE WHEN (SELECT WM_CONCAT(DISTINCT NFS.NUM_NF) FROM THIST_MOV_ITE_PDV HIS, TITENS_NFS ITNFS, TNFS_SAIDA NFS WHERE HIS.ITPDV_ID = ITPDV.ID AND HIS.ITNFS_ID = ITNFS.ID AND ITNFS.NFS_ID = NFS.ID) IS NOT NULL THEN ', atendido pela NF no: '||(SELECT WM_CONCAT(DISTINCT NFS.NUM_NF) FROM THIST_MOV_ITE_PDV HIS, TITENS_NFS ITNFS, TNFS_SAIDA NFS WHERE HIS.ITPDV_ID = ITPDV.ID AND HIS.ITNFS_ID = ITNFS.ID AND ITNFS.NFS_ID = NFS.ID) ELSE NULL END        AS anotacao
      ,PDV.DT_ENTREGA                                   AS previsaoFechamento
      ,(SELECT WM_CONCAT(DISTINCT NFS.NUM_NF) FROM THIST_MOV_ITE_PDV HIS, TITENS_NFS ITNFS, TNFS_SAIDA NFS WHERE HIS.ITPDV_ID = ITPDV.ID AND HIS.ITNFS_ID = ITNFS.ID AND ITNFS.NFS_ID = NFS.ID) NOTAFISCAL
  FROM TPEDIDOS_VENDA PDV, 
       TCLIENTES CLI,
       TITENS_PDV        ITPDV,
       TITENS_COMERCIAL  ITCM,
       TITENS_EMPR       ITEMPR,
       TITENS            ITE,
       TESTABELECIMENTOS EST
WHERE PDV.ID    = ITPDV.PDV_ID
AND ITCM.ID   = ITPDV.ITCM_ID
AND ITEMPR.ID = ITCM.ITEMPR_ID
AND ITEMPR.EMPR_ID = PDV.EMPR_ID
AND ITE.ID   = ITEMPR.ITEM_ID
AND EST.ID    = PDV.EST_ID_FAT
AND PDV.CLI_ID = CLI.ID
AND cli.cod_cli in (33105,32636,25766,13804,5692,33111,33767)
AND cli.cod_cli NOT in (5135)
ORDER BY PDV.NUM_PEDIDO;