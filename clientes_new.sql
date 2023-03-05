SELECT cli.COD_CLI||'-'||est.COD_EST                                            AS idExterno, 
       focco3i_util.Converte_acentos(cli.descricao)    AS nome, 
       CASE 
         WHEN est.tp_pes = 'F' THEN 'Fisica' 
         ELSE 'Juridica' 
       END                                               AS tipoPessoa, 
       Nvl(est.cnpj, est.cpf)                            AS cnpjCpf, 
       NULL                                              AS organizacao, 
       'ERP'                                        AS fontecontato, 
       CASE
        WHEN este.ativo = 1
         THEN 'Ativo'
         ELSE 'Inativo'
       END                                             AS statusContato, 
      cnt.DT_NASC                                                AS dataNascimento, 
       est.obs                                           AS observacoes, 
       'Responsaveis'                                    AS visivelPara, 
       1                                                 AS ranking, 
       0                                                 AS score, 
       0                                                 AS idUsuarioInclusao, 
       (SELECT Listagg(cod_rep, ',') 
                 within GROUP (ORDER BY cod_rep) 
        FROM   (SELECT DISTINCT rep.cod_rep 
                FROM   test_rep est_rep 
                       inner join trepresentantes rep 
                               ON ( rep.id = est_rep.rep_id ) 
                WHERE  est_rep.est_id = est.id 
                       AND est_rep.empcli_id = clie.id)) AS idExternoUsuarioInclusao, 
       est_cob.dt_cad                                    AS contatoDesde, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN 'Cobranca' 
         WHEN cli.est_id_entr = est.id THEN 'Entrega' 
         WHEN cli.est_id_fat = est.id THEN 'Outro' 
       END                                               AS selectTipoEndereco, 
       focco3i_util.Converte_acentos(CASE 
                                       WHEN cli.est_id_cobr = est.id THEN 
                                       est_cob.endereco 
                                       WHEN cli.est_id_entr = est.id THEN 
                                       est_ent.endereco 
                                       WHEN cli.est_id_fat = est.id THEN 
                                       est_out.endereco 
                                     END)                AS endereco, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN est_cob.nr_endereco 
         WHEN cli.est_id_entr = est.id THEN est_ent.nr_endereco 
         WHEN cli.est_id_fat = est.id THEN est_out.nr_endereco 
       END                                               AS numero, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN est_cob.complemento 
         WHEN cli.est_id_entr = est.id THEN est_ent.complemento 
         WHEN cli.est_id_fat = est.id THEN est_out.complemento 
       END                                               AS complemento, 
       focco3i_util.Converte_acentos(CASE 
                                       WHEN cli.est_id_cobr = est.id THEN 
                                       est_cob.bairro 
                                       WHEN cli.est_id_entr = est.id THEN 
                                       est_ent.bairro 
                                       WHEN cli.est_id_fat = est.id THEN 
                                       est_out.bairro 
                                     END)                AS bairro, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN cid_cob.cidade 
         WHEN cli.est_id_entr = est.id THEN cid_ent.cidade 
         WHEN cli.est_id_fat = est.id THEN cid_out.cidade 
       END                                               AS cidade, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN uf_cob.uf 
         WHEN cli.est_id_entr = est.id THEN uf_ent.uf 
         WHEN cli.est_id_fat = est.id THEN uf_out.uf 
       END                                               AS uf, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN pais_cob.descricao 
         WHEN cli.est_id_entr = est.id THEN pais_ent.descricao 
         WHEN cli.est_id_fat = est.id THEN pais_out.descricao 
       END                                               AS pais, 
       CASE 
         WHEN cli.est_id_cobr = est.id THEN est_cob.cep 
         WHEN cli.est_id_entr = est.id THEN est_ent.cep 
         WHEN cli.est_id_fat = est.id THEN est_out.cep 
       END                                               AS cep, 
       tel.ddd 
       || tel.telefone                                   AS descricao_fone, 
       CASE 
         WHEN tel.tp_tel = 'M' THEN 'Celular' 
         WHEN tel.tp_tel = 'F' THEN 'Fax' 
         WHEN tel.tp_tel = 'R' THEN 'Residencial' 
         WHEN tel.tp_tel = 'C' THEN 'Trabalho' 
         ELSE 'Outro' 
       END                                               AS selectTipo_fone, 
       mail.e_mail                                       AS descricao_email, 
       CASE 
         WHEN mail.e_mail IS NOT NULL THEN 'Trabalho' 
       END                                               AS selectTipo_email, 
       focco3i_util.Converte_acentos(cnt.descricao)      AS nome_contato, 
       tp_cnt.descricao                                  AS cargoRelacao, 
       tel_cnt.ddd 
       || tel_cnt.telefone                               AS fone, 
       mail_cnt.e_mail                                   AS email, 
       NULL                                              AS listIdRepresentantes 
       , 
       (SELECT Listagg(cod_rep, ',') 
                 within GROUP (ORDER BY cod_rep) 
        FROM   (SELECT DISTINCT rep.cod_rep 
                FROM   test_rep est_rep 
                       inner join trepresentantes rep 
                               ON ( rep.id = est_rep.rep_id ) 
                WHERE  est_rep.est_id = est.id 
                       AND est_rep.empcli_id = clie.id)) AS 
       listIdExternoRepresentantes, 
       Decode(EMP.id, 3, 5985, 
                      5984)                              AS idtag, 
       Decode(EMP.id, 3, 'Praticipri', 
                      'Frutticipri')                     AS desctag 
FROM   tclientes cli 
       inner join testabelecimentos est 
               ON ( cli.id = est.cli_id 
                    AND cli.dt_transmissao IS NULL ) 
       left join tcontatos cnt 
              ON ( cnt.est_id = est.id ) 
       left join ttp_cnt tp_cnt 
              ON ( tp_cnt.id = cnt.tpcnt_id ) 
       left join ttel_cnt tel_cnt 
              ON ( tel_cnt.cnt_id = cnt.id 
                   AND tel_cnt.ranking = (SELECT Min(ranking) 
                                          FROM   ttel_cnt tel_cnt1 
                                          WHERE 
                       tel_cnt1.cnt_id = tel_cnt.cnt_id) ) 
       left join temail_cnt mail_cnt 
              ON ( mail_cnt.cnt_id = cnt.id 
                   AND mail_cnt.ranking = (SELECT Min(ranking) 
                                           FROM   temail_cnt mail_cnt1 
                                           WHERE 
                       mail_cnt1.cnt_id = mail_cnt.cnt_id) ) 
       left join ttel_est tel 
              ON ( tel.est_id = est.id ) 
       left join temail_est mail 
              ON ( mail.est_id = est.id ) 
       inner join testabelecimentos est_cob 
               ON ( cli.est_id_cobr = est_cob.id ) 
       inner join tcidades cid_cob 
               ON ( cid_cob.id = est_cob.cid_id ) 
       inner join tufs uf_cob 
               ON ( uf_cob.id = cid_cob.uf_id ) 
       inner join tpaises pais_cob 
               ON ( pais_cob.id = uf_cob.pais_id ) 
       inner join testabelecimentos est_ent 
               ON ( cli.est_id_entr = est_ent.id ) 
       inner join tcidades cid_ent 
               ON ( cid_ent.id = est_ent.cid_id ) 
       inner join tufs uf_ent 
               ON ( uf_ent.id = cid_ent.uf_id ) 
       inner join tpaises pais_ent 
               ON ( pais_ent.id = uf_ent.pais_id ) 
       inner join testabelecimentos est_out 
               ON ( cli.est_id_fat = est_out.id ) 
       inner join tcidades cid_out 
               ON ( cid_out.id = est_out.cid_id ) 
       inner join tufs uf_out 
               ON ( uf_out.id = cid_out.uf_id ) 
       inner join tpaises pais_out 
               ON ( pais_out.id = uf_out.pais_id ) 
       inner join temp_cli clie 
               ON ( clie.cli_id = cli.id ) 
       inner join tempresas emp 
               ON ( emp.id = clie.empr_id ) 
       inner join temp_est este 
               ON ( este.est_id = est.id 
                    AND este.empcli_id = clie.id ) 
AND cli.cod_cli in (33491,1)
ORDER  BY cli.cod_cli, 
          est.cod_est; 