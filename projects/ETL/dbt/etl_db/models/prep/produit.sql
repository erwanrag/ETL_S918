{{ config(
    materialized='incremental',
    unique_key='cod_pro',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS produit_pkey ON {{ this }} USING btree (cod_pro)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_pro NOT IN (SELECT cod_pro FROM {{ source('ods', 'produit') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : produit
    ============================================================================
    Généré automatiquement le 2025-12-12 16:57:31
    
    Source       : ods.produit
    Lignes       : 436,163
    Colonnes ODS : 613
    Colonnes PREP: 156  (+ _prep_loaded_at)
    Exclues      : 458 (74.7%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_pro
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 178
      - Constantes      : 272
      - Faible valeur   : 7
    ============================================================================
*/

SELECT
    "sous_type" AS sous_type,
    "typ_elem" AS typ_elem,
    "cod_pro" AS cod_pro,
    "nom_pro" AS nom_pro,
    "nom_pr2" AS nom_pr2,
    "refint" AS refint,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_5" AS zal_5,
    "znu_1" AS znu_1,
    "znu_2" AS znu_2,
    "znu_3" AS znu_3,
    "znu_5" AS znu_5,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "zta_4" AS zta_4,
    "zda_2" AS zda_2,
    "statut" AS statut,
    "groupe" AS groupe,
    "clas_grp" AS clas_grp,
    "famille" AS famille,
    "clas_fam" AS clas_fam,
    "s_famille" AS s_famille,
    "clas_sf" AS clas_sf,
    "fam_cpt" AS fam_cpt,
    "calcul" AS calcul,
    "ordre" AS ordre,
    "reg_fact" AS reg_fact,
    "val_df" AS val_df,
    "crit_df" AS crit_df,
    "px_refv" AS px_refv,
    "coef_t2" AS coef_t2,
    "coef_t3" AS coef_t3,
    "coef_t4" AS coef_t4,
    "dat_fpxv" AS dat_fpxv,
    "uni_vte" AS uni_vte,
    "lib_univ" AS lib_univ,
    "lib_conv" AS lib_conv,
    "mult-fou" AS mult_fou,
    "refext" AS refext,
    "px_refa" AS px_refa,
    "fam_tar" AS fam_tar,
    "marque" AS marque,
    "nb_point" AS nb_point,
    "art_remp" AS art_remp,
    "dat_remp" AS dat_remp,
    "equiv" AS equiv,
    "classe" AS classe,
    "coef_dep" AS coef_dep,
    "code_tva" AS code_tva,
    "enc_db" AS enc_db,
    "rem_vte" AS rem_vte,
    "pmp" AS pmp,
    "ctrl_qua" AS ctrl_qua,
    "suiv_se" AS suiv_se,
    "cod_for" AS cod_for,
    "qte_for" AS qte_for,
    "cod_nom" AS cod_nom,
    "poid_net" AS poid_net,
    "gencod-v" AS gencod_v,
    "consigne" AS consigne,
    "art_cs_u" AS art_cs_u,
    "poid_brut_1" AS poid_brut_1,
    "poid_brut_2" AS poid_brut_2,
    "largeur_1" AS largeur_1,
    "largeur_2" AS largeur_2,
    "longueur_1" AS longueur_1,
    "longueur_2" AS longueur_2,
    "hauteur_1" AS hauteur_1,
    "hauteur_2" AS hauteur_2,
    "d_pxach" AS d_pxach,
    "dat_ent" AS dat_ent,
    "ges_stk" AS ges_stk,
    "ges_dem" AS ges_dem,
    "px_rvt" AS px_rvt,
    "px_refav" AS px_refav,
    "dpx_rvt" AS dpx_rvt,
    "sf_tar" AS sf_tar,
    "cod_par" AS cod_par,
    "px_poi" AS px_poi,
    "rem_imp" AS rem_imp,
    "zlo_1" AS zlo_1,
    "zlo_2" AS zlo_2,
    "radio_dng" AS radio_dng,
    "cod_cli" AS cod_cli,
    "int_condt" AS int_condt,
    "gar_fab" AS gar_fab,
    "qte_eco" AS qte_eco,
    "magasin" AS magasin,
    "er_for" AS er_for,
    "er_pon" AS er_pon,
    "er_var_1" AS er_var_1,
    "er_var_2" AS er_var_2,
    "mult_cde" AS mult_cde,
    "s3_famille" AS s3_famille,
    "s4_famille" AS s4_famille,
    "qte_serm" AS qte_serm,
    "ach_ctr" AS ach_ctr,
    "nb_ctr" AS nb_ctr,
    "px_sim" AS px_sim,
    "qte_max" AS qte_max,
    "nat_deb" AS nat_deb,
    "survei" AS survei,
    "dep_uniq" AS dep_uniq,
    "cod_conv" AS cod_conv,
    "cod_cal_1" AS cod_cal_1,
    "cod_cal_2" AS cod_cal_2,
    "cod_cal_3" AS cod_cal_3,
    "cod_cal_4" AS cod_cal_4,
    "cod_cal_5" AS cod_cal_5,
    "s3_tar" AS s3_tar,
    "volume_1" AS volume_1,
    "volume_2" AS volume_2,
    "px_blq" AS px_blq,
    "px_mini" AS px_mini,
    "ges_emp" AS ges_emp,
    "mini_vte" AS mini_vte,
    "mult_vte" AS mult_vte,
    "crit_cli_df" AS crit_cli_df,
    "cod_nue" AS cod_nue,
    "nb_uv" AS nb_uv,
    "nb_cv" AS nb_cv,
    "pp_uv" AS pp_uv,
    "pou_poid" AS pou_poid,
    "cod_prx" AS cod_prx,
    "ges_smp" AS ges_smp,
    "px_std" AS px_std,
    "w_produit" AS w_produit,
    "f_pn" AS f_pn,
    "phone" AS phone,
    "fou_fab" AS fou_fab,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "cod_cat" AS cod_cat,
    "px_ttc" AS px_ttc,
    "dat_import" AS dat_import,
    "non_sscc" AS non_sscc,
    "code_bio" AS code_bio,
    "nb_uv1" AS nb_uv1,
    "poid_ul_1" AS poid_ul_1,
    "px_etud" AS px_etud,
    "non_uli" AS non_uli,
    "tare_p_1" AS tare_p_1,
    "tare_p_2" AS tare_p_2,
    "pds_net_1" AS pds_net_1,
    "pds_net_2" AS pds_net_2,
    "pds_net_3" AS pds_net_3,
    "pds_net_ul_1" AS pds_net_ul_1,
    "cod_prev" AS cod_prev,
    "dat_fpxr" AS dat_fpxr,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'produit') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
