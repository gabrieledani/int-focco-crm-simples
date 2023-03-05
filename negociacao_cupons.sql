SELECT NFS.NUM_NF                                       AS idExterno
      ,cli.COD_CLI||'-'||est.COD_EST                    AS idExterno_Contato
      ,NFS.CNPJ_CPF_CLI
      ,NULL                                             AS idExterno_organizacao
      ,'Negociacao '|| focco3i_util.Converte_acentos(cli.descricao) ||'-Numero NFCe: '||NFS.NUM_NF AS nome
      ,(CASE WHEN NFS.VLR_FRETE > 0 
            THEN '-Valor de Frete: '||TO_CHAR(NFS.VLR_FRETE,'FM999G999G990D00')
            ELSE NULL
        END || 
        CASE WHEN NFS.VLR_DESC > 0 
            THEN'-Percentual de desconto: '||TO_CHAR(((NFS.VLR_DESC / NFS.VLR_BRUTO)*100),'FM999G999G990D00')
            ELSE NULL
        END ||
        CASE WHEN NFS.FORN_ID IS NOT NULL
            THEN '-Transportadora: '||(SELECT focco3i_util.Converte_acentos(FO.DESCRICAO) FROM TFORNECEDORES FO WHERE NFS.FORN_ID = FO.ID)
            ELSE NULL
        END)                                            AS descricao
      ,NFS.EMPR_ID                                      AS categoriaNegociacao
      ,1671                                             AS idEtapaNegociacao
      ,'Ganha'                                       AS statusNegociacao
      ,NFS.VLR_BRUTO                                    AS valor
      ,NVL((SELECT ID FROM TUSUARIOS WHERE LOGIN = TRIM(SUBSTR(NFS.USUARIO,1,INSTR(NFS.USUARIO,'(')-1))),1) AS idExternoUsuarioInclusao
      ,NFS.DT_EMIS                                      AS criadaEm
      ,ITE.COD_ITEM                                     AS idExterno_produto
      ,ITNFS.VLR_BRUTO                                 AS valorUnitario
      ,ITNFS.QTDE                                       AS quantidade
      ,CASE
        WHEN ITNFS.VLR_DESC > 0
         THEN TRUNC((((ITNFS.VLR_DESC ) / ITNFS.VLR_BRUTO) * 100),5)
       END                                              AS percentualDesconto
      ,ITNFS.VLR_BRUTO * ITNFS.QTDE                     AS valorTotal 
      ,focco3i_util.Converte_acentos(ITE.DESC_TECNICA)                                 AS comentarios
      --,'Previsao de Entrega: '||TO_CHAR(ITNFS.DT_ENTREGA,'DD/MM/RRRR')         AS anotacao
      --,NFS.DT_ENTREGA                                   AS previsaoFechamento
  FROM --TPEDIDOS_VENDA PDV,
       TNFS_SAIDA NFS,
       TSERIES_NF SER,
       TCLIENTES CLI,
       --TITENS_PDV        ITPDV,
       TITENS_NFS        ITNFS,
       TITENS_COMERCIAL  ITCM,
       TITENS_EMPR       ITEMPR,
       TITENS            ITE,
       TESTABELECIMENTOS EST
WHERE NFS.ID    = ITNFS.NFS_ID
AND NFS.SRNF_ID = SER.ID
AND SER.COD_SER_NFEL = 65
AND ITCM.ID   = ITNFS.ITCM_ID
AND ITEMPR.ID = ITCM.ITEMPR_ID
AND ITEMPR.EMPR_ID = NFS.EMPR_ID
AND ITE.ID   = ITEMPR.ITEM_ID
AND EST.ID    = NFS.EST_ID
AND NFS.CLI_ID = CLI.ID
--AND cli.cod_cli in (33105,32636,25766,13804,5692,33111)
ORDER BY NFS.NUM_NF DESC;
