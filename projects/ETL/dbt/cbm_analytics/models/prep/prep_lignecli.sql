{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lignecli
    ============================================================================
    Généré automatiquement le 2025-12-05 15:33:53
    
    Source       : ods.lignecli
    Lignes       : 39,933
    Colonnes ODS : 419
    Colonnes PREP: 104
    Exclues      : 315 (75.2%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 157
      - Constantes      : 145
      - Faible valeur   : 6
    ============================================================================
*/

SELECT
    "cod_cli" AS cod_cli,
    "no_cde" AS no_cde,
    "no_bl" AS no_bl,
    "div_fac" AS div_fac,
    "no_ligne" AS no_ligne,
    "sous_type" AS sous_type,
    "typ_elem" AS typ_elem,
    "cod_pro" AS cod_pro,
    "nom_pro" AS nom_pro,
    "qte" AS qte,
    "qte_rlk" AS qte_rlk,
    "qte_dejl" AS qte_dejl,
    "px_vte" AS px_vte,
    "remise1" AS remise1,
    "rem_app" AS rem_app,
    "refint" AS refint,
    "groupe" AS groupe,
    "famille" AS famille,
    "s_famille" AS s_famille,
    "fam_cpt" AS fam_cpt,
    "lib_univ" AS lib_univ,
    "lib_conv" AS lib_conv,
    "cod_fou" AS cod_fou,
    "refext" AS refext,
    "devise" AS devise,
    "px_ach" AS px_ach,
    "fam_tar" AS fam_tar,
    "poid_bru" AS poid_bru,
    "code_tva" AS code_tva,
    "enc_db" AS enc_db,
    "calcul" AS calcul,
    "ordre" AS ordre,
    "reg_fact" AS reg_fact,
    "px_refv" AS px_refv,
    "px_refa" AS px_refa,
    "px_ach_d" AS px_ach_d,
    "pmp" AS pmp,
    "cde_fou" AS cde_fou,
    "gratuit" AS gratuit,
    "forcer" AS forcer,
    "depot" AS depot,
    "dat_liv" AS dat_liv,
    "dat_livd" AS dat_livd,
    "tot_txt" AS tot_txt,
    "reliquat" AS reliquat,
    "dev_vte" AS dev_vte,
    "qte_res" AS qte_res,
    "consigne" AS consigne,
    "lien_nmc" AS lien_nmc,
    "maj_stk" AS maj_stk,
    "edt_nmc" AS edt_nmc,
    "no_bp" AS no_bp,
    "n_surlig" AS n_surlig,
    "heure" AS heure,
    "decl_fou" AS decl_fou,
    "maj_stat" AS maj_stat,
    "px_rvt" AS px_rvt,
    "sf_tar" AS sf_tar,
    "mt_ht" AS mt_ht,
    "lien_con" AS lien_con,
    "ord_nmc" AS ord_nmc,
    "cod_ori" AS cod_ori,
    "garantie" AS garantie,
    "lig_fou" AS lig_fou,
    "volume" AS volume,
    "gen_auto" AS gen_auto,
    "dat_mad" AS dat_mad,
    "qte_prep" AS qte_prep,
    "reg_deb" AS reg_deb,
    "no_contrat" AS no_contrat,
    "type_prix" AS type_prix,
    "hr_vp" AS hr_vp,
    "dat_vp" AS dat_vp,
    "qui_p" AS qui_p,
    "qte_epre" AS qte_epre,
    "lien_emp" AS lien_emp,
    "ndos" AS ndos,
    "s3_tar" AS s3_tar,
    "typ_gra" AS typ_gra,
    "nb_uv" AS nb_uv,
    "dat_acc" AS dat_acc,
    "dat_rll" AS dat_rll,
    "poid_net" AS poid_net,
    "px_ttc" AS px_ttc,
    "s3_famille" AS s3_famille,
    "s4_famille" AS s4_famille,
    "qui_v" AS qui_v,
    "nom_pr2" AS nom_pr2,
    "no_lot" AS no_lot,
    "no_unique" AS no_unique,
    "MScod_fou" AS mscod_fou,
    "MScde_fou" AS mscde_fou,
    "MSlig_fou" AS mslig_fou,
    "poid_tot" AS poid_tot,
    "ach_ctr" AS ach_ctr,
    "no_tarif" AS no_tarif,
    "uniq_id" AS uniq_id,
    "no_devis" AS no_devis,
    "nb_uv1" AS nb_uv1,
    "qte_ro" AS qte_ro,
    "marque" AS marque,
    "gen_con" AS gen_con,
    "cod_nom" AS cod_nom,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lignecli') }}
