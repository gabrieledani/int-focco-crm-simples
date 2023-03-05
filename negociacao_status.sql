SELECT DISTINCT PDV.NUM_PEDIDO                                   AS idExternoNegociacao
      ,cli.COD_CLI||'-'||est.COD_EST                    AS idExternoContato
      ,'Ganha'                                         AS statusNegociacao
      ,NVL((SELECT ID FROM TUSUARIOS WHERE LOGIN = TRIM(SUBSTR(NFS.USUARIO,1,INSTR(NFS.USUARIO,'(')-1))),1)                                      AS idExternoUsuarioConclusao
      ,NFS.DT_EMIS                                      AS concluidaEm
      ,0                                    AS motivoGanhoPerda
  FROM TPEDIDOS_VENDA PDV, 
       TCLIENTES CLI,
       TESTABELECIMENTOS EST,
       TITENS_PDV        ITPDV,
       THIST_MOV_ITE_PDV HIS,
       TITENS_NFS        ITNFS,
       TNFS_SAIDA        NFS
WHERE PDV.EST_ID_FAT = EST.ID
AND PDV.CLI_ID = CLI.ID 
AND ITPDV.PDV_ID = PDV.ID
AND HIS.ITPDV_ID = ITPDV.ID
AND HIS.ITNFS_ID = ITNFS.ID
AND ITNFS.NFS_ID = NFS.ID
AND cli.cod_cli NOT in (5135)
AND cli.cod_cli in (33105,32636,25766,13804,5692,33111,33767)
UNION
SELECT DISTINCT PDV.NUM_PEDIDO                                   AS idExternoNegociacao
      ,cli.COD_CLI||'-'||est.COD_EST                    AS idExternoContato
      ,'Perdida'                                         AS statusNegociacao
      ,NVL((SELECT ID FROM TUSUARIOS WHERE LOGIN = TRIM(SUBSTR(PDV.USUARIO,1,INSTR(PDV.USUARIO,'(')-1))),1) AS idExternoUsuarioConclusao
      ,HIS.DT                                      AS concluidaEm
      ,MOT.COD                                    AS motivoGanhoPerda
  FROM TPEDIDOS_VENDA PDV, 
       TCLIENTES CLI,
       TESTABELECIMENTOS EST,
       TITENS_PDV        ITPDV,
       THIST_MOV_ITE_PDV HIS,
       TMOTIVOS_CANCEL  MOT
WHERE PDV.EST_ID_FAT = EST.ID
AND PDV.CLI_ID = CLI.ID 
AND ITPDV.PDV_ID = PDV.ID
AND HIS.ITPDV_ID = ITPDV.ID
AND HIS.MTCN_ID = MOT.ID
AND PDV.POS_PDV =  'C'
--AND PDV.NUM_PEDIDO = 19001
AND cli.cod_cli NOT in (5135)
AND cli.cod_cli in (33105,32636,25766,13804,5692,33111,33767)
;