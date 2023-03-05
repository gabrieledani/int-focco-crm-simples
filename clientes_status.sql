SELECT DISTINCT cli.COD_CLI||'-'||est.COD_EST                           AS idExterno, 
       (CASE WHEN este.ativo = 1 THEN tc.cod_tp_cli ELSE 0 END)         AS statusContato,
       este.ativo AS status
FROM   tclientes cli,
       ttp_cli tc,
       testabelecimentos est,
       temp_cli clie,
       tempresas emp,
       temp_est este
 WHERE cli.cod_cli in (33105,32636,25766,13804,5692,33111,33767)
   AND est.ttp_cli_id = tc.id
   AND est.cli_id = cli.id
   AND clie.cli_id = cli.id
   AND clie.empr_id = emp.id
   AND este.est_id = est.id 
   AND este.empcli_id = clie.id 
   AND cli.cod_cli NOT in (5135)
order by cli.COD_CLI||'-'||est.COD_EST,este.ativo;