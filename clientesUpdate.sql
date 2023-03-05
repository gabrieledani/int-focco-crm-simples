UPDATE tclientes 
SET    dt_transmissao = Trunc(sysdate) 
WHERE  tclientes.id IN (SELECT DISTINCT cli.id 
                        FROM   tclientes cli 
                               INNER JOIN testabelecimentos est 
                                       ON ( cli.id = est.cli_id 
                                            AND cli.dt_transmissao IS NULL ) 
                               LEFT JOIN tcontatos cnt 
                                      ON ( cnt.est_id = est.id ) 
                               LEFT JOIN ttp_cnt tp_cnt 
                                      ON ( tp_cnt.id = cnt.tpcnt_id ) 
                               LEFT JOIN ttel_cnt tel_cnt 
                                      ON ( tel_cnt.cnt_id = cnt.id 
                                           AND tel_cnt.ranking = 
                                               (SELECT Min(ranking) 
                                                FROM   ttel_cnt tel_cnt1 
                                                WHERE 
                                               tel_cnt1.cnt_id = tel_cnt.cnt_id) 
                                         ) 
                               LEFT JOIN temail_cnt mail_cnt 
                                      ON ( mail_cnt.cnt_id = cnt.id 
                                           AND mail_cnt.ranking = 
                                               (SELECT Min(ranking) 
                                                FROM 
                                               temail_cnt mail_cnt1 
                                                                   WHERE 
                                               mail_cnt1.cnt_id = 
                                               mail_cnt.cnt_id) ) 
                               LEFT JOIN ttel_est tel 
                                      ON ( tel.est_id = est.id ) 
                               LEFT JOIN temail_est mail 
                                      ON ( mail.est_id = est.id ) 
                               INNER JOIN testabelecimentos est_cob 
                                       ON ( cli.est_id_cobr = est_cob.id ) 
                               INNER JOIN tcidades cid_cob 
                                       ON ( cid_cob.id = est_cob.cid_id ) 
                               INNER JOIN tufs uf_cob 
                                       ON ( uf_cob.id = cid_cob.uf_id ) 
                               INNER JOIN tpaises pais_cob 
                                       ON ( pais_cob.id = uf_cob.pais_id ) 
                               INNER JOIN testabelecimentos est_ent 
                                       ON ( cli.est_id_entr = est_ent.id ) 
                               INNER JOIN tcidades cid_ent 
                                       ON ( cid_ent.id = est_ent.cid_id ) 
                               INNER JOIN tufs uf_ent 
                                       ON ( uf_ent.id = cid_ent.uf_id ) 
                               INNER JOIN tpaises pais_ent 
                                       ON ( pais_ent.id = uf_ent.pais_id ) 
                               INNER JOIN testabelecimentos est_out 
                                       ON ( cli.est_id_fat = est_out.id ) 
                               INNER JOIN tcidades cid_out 
                                       ON ( cid_out.id = est_out.cid_id ) 
                               INNER JOIN tufs uf_out 
                                       ON ( uf_out.id = cid_out.uf_id ) 
                               INNER JOIN tpaises pais_out 
                                       ON ( pais_out.id = uf_out.pais_id ) 
                               INNER JOIN temp_cli clie 
                                       ON ( clie.cli_id = cli.id ) 
                               INNER JOIN tempresas emp 
                                       ON ( emp.id = clie.empr_id ) 
                               INNER JOIN temp_est este 
                                       ON ( este.est_id = est.id 
                                            AND este.empcli_id = clie.id )); 