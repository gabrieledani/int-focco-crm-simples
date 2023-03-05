SELECT cli.COD_CLI              AS  idExterno
      ,est.DESCRICAO            AS  nome
      ,CASE 
        WHEN est.TP_PES = 'F'
         THEN 'Fìsica'
         ELSE 'Jurídica'
       END                      AS  tipoPessoa
      ,NVL(est.CNPJ,est.CPF)    AS  cnpjCpf
      ,emp.cod_emp              AS  organizacao
      ,''                       AS  fontecontato
      ,CASE
        WHEN este.ativo = 1
         THEN 'Ativo'
         ELSE 'Inativo'
       END                      AS  statusContato 
      ,''                       AS  dataNascimento
      ,est.obs                  AS  observacoes
      ,'Todos'                  AS  visivelPara
      ,1                        AS  ranking
      ,0                        AS  score
      ,0                        AS  idUsuarioInclusao
      ,0                        AS  idExternoUsuarioInclusao
      ,est_cob.dt_cad           AS  contatoDesde
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN 'Cobrança'
        WHEN cli.est_id_entr = est.id 
         THEN 'Entrega'
        WHEN cli.est_id_fat = est.id 
         THEN 'Outro'
       END                      AS  selectTipoEndereco
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN est_cob.endereco 
        WHEN cli.est_id_entr = est.id 
         THEN est_ent.endereco 
        WHEN cli.est_id_fat = est.id 
         THEN est_out.endereco 
       END                      AS  endereco
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN est_cob.nr_endereco
        WHEN cli.est_id_entr = est.id 
         THEN est_ent.nr_endereco 
        WHEN cli.est_id_fat = est.id 
         THEN est_out.nr_endereco
       END                      AS  numero
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN est_cob.complemento
        WHEN cli.est_id_entr = est.id 
         THEN est_ent.complemento 
        WHEN cli.est_id_fat = est.id 
         THEN est_out.complemento
       END                      AS  complemento
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN est_cob.bairro
        WHEN cli.est_id_entr = est.id 
         THEN est_ent.bairro 
        WHEN cli.est_id_fat = est.id 
         THEN est_out.bairro
       END                      AS  bairro
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN cid_cob.cidade
        WHEN cli.est_id_entr = est.id 
         THEN cid_ent.cidade 
        WHEN cli.est_id_fat = est.id 
         THEN cid_out.cidade
       END                      AS  cidade
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN uf_cob.uf
        WHEN cli.est_id_entr = est.id 
         THEN uf_ent.uf 
        WHEN cli.est_id_fat = est.id 
         THEN uf_out.uf
       END                      AS  uf
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN pais_cob.descricao
        WHEN cli.est_id_entr = est.id 
         THEN pais_ent.descricao
        WHEN cli.est_id_fat = est.id 
         THEN pais_out.descricao
       END                      AS  pais
      ,CASE
        WHEN cli.est_id_cobr = est.id 
         THEN est_cob.cep
        WHEN cli.est_id_entr = est.id 
         THEN est_ent.cep
        WHEN cli.est_id_fat = est.id 
         THEN est_out.cep
       END                      AS  cep
      ,tel.ddd || tel.telefone  AS  descricao_fone
      ,CASE
        WHEN tel.tp_tel = 'M'
         THEN 'Celular'
        WHEN tel.tp_tel = 'F'
         THEN 'Fax'
        WHEN tel.tp_tel = 'R'
         THEN 'Residencial'
        WHEN tel.tp_tel = 'C'
         THEN 'Trabalho'
         ELSE 'Outro'
       END                     AS  selectTipo_fone
      ,mail.e_mail             AS  descricao_email
      ,CASE
        WHEN mail.e_mail IS NOT NULL
         THEN 'Trabalho'
       END                     AS  selectTipo_email
      ,cnt.descricao           AS  nome_contato 
      ,tp_cnt.descricao        AS  cargoRelacao
      ,tel_cnt.ddd || tel_cnt.telefone AS fone
      ,mail_cnt.e_mail         AS  email
      ,(SELECT LISTAGG(cod_rep, ',') WITHIN GROUP (ORDER BY cod_rep)
          FROM (SELECT DISTINCT rep.cod_rep
                  FROM test_rep est_rep   INNER JOIN  trepresentantes  rep
                       ON (rep.id    =  est_rep.rep_id)
                 WHERE est_rep.est_id    = est.id
                   AND est_rep.empcli_id = clie.id))
                               AS  listIdRepresentantes
      ,(SELECT LISTAGG(descricao, ',') WITHIN GROUP (ORDER BY descricao)
          FROM (SELECT DISTINCT rep.descricao 
                  FROM test_rep est_rep   INNER JOIN  trepresentantes  rep
                       ON (rep.id    =  est_rep.rep_id)
                 WHERE est_rep.est_id    = est.id
                   AND est_rep.empcli_id = clie.id))
                               AS  listIdExternoRepresentantes
  FROM tclientes  cli   INNER JOIN testabelecimentos est
       ON (cli.id            = est.cli_id)
                        LEFT  JOIN tcontatos         cnt
       ON (cnt.est_id        = est.id)
                        LEFT  JOIN ttp_cnt           tp_cnt
       ON (tp_cnt.id         = cnt.tpcnt_id)
                        LEFT  JOIN ttel_cnt          tel_cnt
       ON (tel_cnt.cnt_id    = cnt.id
        AND tel_cnt.ranking  = (SELECT MIN(RANKING)
                                  FROM ttel_cnt  tel_cnt1
                                 WHERE tel_cnt1.cnt_id = tel_cnt.cnt_id) )
                        LEFT  JOIN temail_cnt        mail_cnt
       ON (mail_cnt.cnt_id   = cnt.id
        AND mail_cnt.ranking = (SELECT MIN(RANKING)
                                  FROM temail_cnt  mail_cnt1
                                 WHERE mail_cnt1.cnt_id = mail_cnt.cnt_id) )
                        LEFT  JOIN ttel_est          tel
       ON (tel.est_id        = est.id)
                        LEFT  JOIN temail_est        mail
       ON (mail.est_id       = est.id)
                        INNER JOIN testabelecimentos est_cob
       ON (cli.est_id_cobr   = est_cob.id)
                        INNER JOIN tcidades          cid_cob
       ON (cid_cob.id        = est_cob.cid_id)
                        INNER JOIN tufs              uf_cob
       ON (uf_cob.id         = cid_cob.uf_id)
                        INNER JOIN tpaises           pais_cob
       ON (pais_cob.id       = uf_cob.pais_id)
                        INNER JOIN testabelecimentos est_ent
       ON (cli.est_id_entr   = est_ent.id)
                        INNER JOIN tcidades          cid_ent
       ON (cid_ent.id        = est_ent.cid_id)
                        INNER JOIN tufs              uf_ent
       ON (uf_ent.id         = cid_ent.uf_id)
                        INNER JOIN tpaises           pais_ent
       ON (pais_ent.id       = uf_ent.pais_id)
                        INNER JOIN testabelecimentos est_out
       ON (cli.est_id_fat    = est_out.id)
                        INNER JOIN tcidades          cid_out
       ON (cid_out.id        = est_out.cid_id)
                        INNER JOIN tufs              uf_out
       ON (uf_out.id         = cid_out.uf_id)
                        INNER JOIN tpaises           pais_out
       ON (pais_out.id       = uf_out.pais_id)
                        INNER JOIN temp_cli          clie
       ON (clie.cli_id       = cli.id)
                        INNER JOIN tempresas         emp
       ON (emp.id            = clie.empr_id)
                        INNER JOIN temp_est          este
       ON (este.est_id       = est.id
        AND este.empcli_id   = clie.id) 
ORDER BY cli.COD_CLI
        ,est.COD_EST;
		