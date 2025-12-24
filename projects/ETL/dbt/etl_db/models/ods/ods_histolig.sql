{{
    config(
        materialized='incremental',
        unique_key=['uniq_id'],
        incremental_strategy='merge',
        on_schema_change='fail',
        schema='ods_dbt'
    )
}}

/*
============================================================================
ODS Model: histolig
============================================================================
Description : Historique lignes mouvements
Primary Key : uniq_id
Detect Del. : No
Retention   : 730 days
============================================================================
*/

WITH source_data AS (
    SELECT
        "uniq_id",
        "cod_pro", "cod_dec1", "cod_dec2", "cod_dec3", "cod_dec4", "cod_dec5", "depot", "dat_mvt", "typ_mvt", "ref_mvt", "es_mvt", "qte_unit", "px_unit", "sous_type", "typ_elem", "cod_cf", "no_cde", "no_bl", "div_fac", "no_ligne", "nom_pro", "qte", "px_vte", "remise1", "remise2", "rem_app", "refint", "groupe", "famille", "s_famille", "fam_cpt", "uni_vte", "lib_univ", "lib_conv", "lib_surv", "nb_univ", "nb_conv", "conv_px", "cod_fou", "refext", "devise", "px_ach", "uni_ach", "lib_unia", "lib_cona", "lib_sura", "nb_unia", "nb_cona", "fam_tar", "fam_pan", "code_tva", "enc_db", "calcul", "px_refv", "px_refa", "px_ach_d", "pmp", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "gratuit", "forcer", "dat_liv", "commerc_1", "commerc_2", "app_aff", "total", "tot_txt", "formule", "niv_pha_1", "niv_pha_2", "niv_pha_3", "typ_pha", "conv_sto", "px_vte_i", "dev_vte", "nmc_lie", "lien_imm", "lien_lot", "lien_nmc", "maj_stk", "edt_nmc", "conv_md", "conv_dec", "qte_cde", "n_surlig", "heure", "cout_trs", "longueur", "largeur", "hauteur", "calc_uv", "lib_unic", "qte_uc", "cal_marg", "coef_mar_1", "coef_mar_2", "coef_mar_3", "coef_mar_4", "coef_mar_5", "px_rvt", "coef_dep", "c_promo", "poi_var", "remise3", "poid_tot", "pvc", "maj_stat", "ref_con", "sf_tar", "affaire", "qui", "mt_ht", "lien_con", "master", "ord_nmc", "fac_poi", "cod_ori", "pret", "fin_pret", "dat_ret", "ret_pret", "garantie", "fin_gar", "cde_cf", "lig_fou", "demande", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "typ_rem_1", "typ_rem_2", "typ_rem_3", "qte_rlf", "mt_rlf", "volume", "zon_lib_1", "zon_lib_2", "zon_lib_3", "zon_lib_4", "zon_lib_5", "num_ave", "non_rfa", "tx_com", "conveuro", "no_of", "no_reafab", "no_reacons", "k_var", "no_lance", "nofaca", "dafaca", "md5", "prod_gp", "dat_mad", "no_bl_a", "location", "deb_loc", "fin_loc", "no_info", "motif_rebu", "col_vis", "no_serie", "cde_ori", "conv_pa", "k_opt", "lien_grc", "lien_gar", "lien_emp", "typ_ctr", "reg_deb", "nat_deb", "no_contrat", "type_prix", "hr_vp", "dat_vp", "qui_p", "contrat", "ind_con", "lig_con", "uni_fac", "cod_conv", "ndos", "s3_tar", "s4_tar", "nat_prodad", "cpx_prodad", "remise4", "ord_rem_1", "ord_rem_2", "ord_rem_3", "ord_rem_4", "typ_com", "reg_fact", "crit_cli_df", "typ_gra", "nb_ua", "nb_ca", "nb_uv", "nb_cv", "pp_ua", "pp_uv", "regr", "retr_cess", "retr_gere", "dat_acc", "dat_rll", "lien_frais", "typ_acc", "typ_cc", "dat_cc", "poid_net", "tare", "fret", "s3_famille", "s4_famille", "stk_csg", "list_crit", "qui_v", "num_lot", "num_colis", "no_mvtbois", "no_mvtobois", "no_opebois", "mag_cli", "mag_gen", "etat", "nom_pr2", "no_lot", "mt_ttc", "inc_deb", "rem_val_1", "rem_val_2", "rem_val_3", "rem_val_4", "nb_point", "ope_exc", "asso_lig", "eve_exc", "sous_trait", "niveau1", "vol_ach", "niveau2", "vol_cnv", "niveau3", "vol_mes", "niveau4", "vol_mtach", "niveau5", "px_rvt_bo", "no_unique", "ns_indrev", "mt_trsc", "num_prix", "rendement", "duree", "prc_perte", "motif", "no_ligct", "no_grp_c", "mtp_ach_p", "mtp_rvt_p", "mtp_ach_r", "mtp_rvt_r", "mtp_fac", "no_ctbois", "typ_fich", "materiel", "no_ordre", "px_franco", "px_std", "ach_ctr", "px_ree", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "px_trs", "consigne", "det_frais", "gen_csg", "lien_cona", "cpt_acc", "hr_mod", "no_tarif", "hr_crt", "lien_sscc", "no_batch", "ordre_deb", "derog_fou", "no_devis", "mt_pa", "mt_pr", "mt_pm", "ref_lig_bud", "grille_rem", "lib_ua1", "lib_ua2", "lib_ua3", "lib_uv1", "lib_uv2", "lib_uv3", "nb_ua1", "nb_ua2", "nb_ca3", "nb_uv1", "nb_uv2", "nb_cv3", "cod_op", "MScod_fou", "MScde_fou", "MSlig_fou", "crit_param", "crit_val", "pays_ori", "sous_total", "no_u_perte", "df_tax_1", "df_tax_2", "df_tax_3", "df_tax_4", "df_tax_5", "df_vte_1", "df_vte_2", "df_vte_3", "df_vte_4", "df_vte_5", "df_rvt_1", "df_rvt_2", "df_rvt_3", "df_rvt_4", "df_rvt_5", "df_mul_1", "df_mul_2", "df_mul_3", "df_mul_4", "df_mul_5", "motif_rem", "lien_valor", "vc_numlig", "vc_rang", "rei_deb", "nai_deb", "dci_deb", "px_deb", "ret_stk", "vpm", "vpr", "hr_mvt", "noedit", "tickbal", "rayon", "lib_bar", "uni_pv", "uni_pa", "qte_por", "com_port", "mt_frais", "px_rvt_vte", "cod_fou_df", "cod_prolie", "famillefl", "s_famillefl", "s3_famillefl", "s4_famillefl", "marquefl", "cod_rvt_ach", "cod_rvt_vte", "px_up", "nom_pr_web", "qte_up", "qte_ro", "uni_uc", "qte_rc", "qte_rb", "lissch", "marque", "lib_uc", "lib_pv", "TabPart_histolig", "flag_repli", "tare_p_1", "tare_p_2", "tare_p_3", "tare_ul_1", "tare_ul_2", "tare_ul_3", "valodffl", "prioritefl", "colis", "pal_sol", "lien_dffl", "apeser", "ssfraisfl", "typ_pal", "typ_col", "frai_prod", "lib_pa", "lien_facfo", "niveau6", "niveau7", "niveau8", "no_deb", "lig_deb", "ligsce", "sup_cons", "cond_rem_1", "cond_rem_2", "cond_rem_3", "cond_rem_4", "list_rem_1", "list_rem_2", "list_rem_3", "list_rem_4", "cod_nom", "coef_mar6", "intitule", "arrondi_pvdev", "mt_titresto",
        "_etl_hashdiff",
        "_etl_valid_from"
    FROM {{ source('staging', 'stg_histolig') }}
    
    {% if is_incremental() %}
    WHERE (_etl_valid_from, _etl_hashdiff) NOT IN (
        SELECT _etl_valid_from, _etl_hashdiff
        FROM {{ this }}
    )
    {% endif %}
),

{% if is_incremental() %}

current_records AS (
    SELECT
        "uniq_id",
        "_etl_hashdiff",
        "_etl_valid_from",
        "last_seen_date"
    FROM {{ this }}
    WHERE "_etl_is_current" = TRUE
      AND "_etl_is_deleted" = FALSE
),

new_records AS (
    SELECT stg.*
    FROM source_data stg
    LEFT JOIN current_records ods USING ("uniq_id")
    WHERE ods."uniq_id" IS NULL
),

changed_records AS (
    SELECT 
        stg.*,
        ods."_etl_valid_from" AS previous_valid_from
    FROM source_data stg
    INNER JOIN current_records ods USING ("uniq_id")
    WHERE stg."_etl_hashdiff" != ods."_etl_hashdiff"
),

unchanged_records AS (
    SELECT
        "uniq_id",
        stg."_etl_valid_from" AS new_last_seen
    FROM current_records ods
    INNER JOIN source_data stg USING ("uniq_id")
    WHERE stg."_etl_hashdiff" = ods."_etl_hashdiff"
),

final AS (
    
    -- Clôturer anciennes versions
    SELECT
        ods.*,
        cr."_etl_valid_from" AS "_etl_valid_to",
        FALSE AS "_etl_is_current",
        ods."last_seen_date"
    FROM {{ this }} ods
    INNER JOIN changed_records cr USING ("uniq_id")
    WHERE ods."_etl_valid_from" = cr.previous_valid_from
      AND ods."_etl_is_current" = TRUE
    
    UNION ALL
    
    -- Nouvelles versions
    SELECT
        "uniq_id", "cod_pro", "cod_dec1", "cod_dec2", "cod_dec3", "cod_dec4", "cod_dec5", "depot", "dat_mvt", "typ_mvt", "ref_mvt", "es_mvt", "qte_unit", "px_unit", "sous_type", "typ_elem", "cod_cf", "no_cde", "no_bl", "div_fac", "no_ligne", "nom_pro", "qte", "px_vte", "remise1", "remise2", "rem_app", "refint", "groupe", "famille", "s_famille", "fam_cpt", "uni_vte", "lib_univ", "lib_conv", "lib_surv", "nb_univ", "nb_conv", "conv_px", "cod_fou", "refext", "devise", "px_ach", "uni_ach", "lib_unia", "lib_cona", "lib_sura", "nb_unia", "nb_cona", "fam_tar", "fam_pan", "code_tva", "enc_db", "calcul", "px_refv", "px_refa", "px_ach_d", "pmp", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "gratuit", "forcer", "dat_liv", "commerc_1", "commerc_2", "app_aff", "total", "tot_txt", "formule", "niv_pha_1", "niv_pha_2", "niv_pha_3", "typ_pha", "conv_sto", "px_vte_i", "dev_vte", "nmc_lie", "lien_imm", "lien_lot", "lien_nmc", "maj_stk", "edt_nmc", "conv_md", "conv_dec", "qte_cde", "n_surlig", "heure", "cout_trs", "longueur", "largeur", "hauteur", "calc_uv", "lib_unic", "qte_uc", "cal_marg", "coef_mar_1", "coef_mar_2", "coef_mar_3", "coef_mar_4", "coef_mar_5", "px_rvt", "coef_dep", "c_promo", "poi_var", "remise3", "poid_tot", "pvc", "maj_stat", "ref_con", "sf_tar", "affaire", "qui", "mt_ht", "lien_con", "master", "ord_nmc", "fac_poi", "cod_ori", "pret", "fin_pret", "dat_ret", "ret_pret", "garantie", "fin_gar", "cde_cf", "lig_fou", "demande", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "typ_rem_1", "typ_rem_2", "typ_rem_3", "qte_rlf", "mt_rlf", "volume", "zon_lib_1", "zon_lib_2", "zon_lib_3", "zon_lib_4", "zon_lib_5", "num_ave", "non_rfa", "tx_com", "conveuro", "no_of", "no_reafab", "no_reacons", "k_var", "no_lance", "nofaca", "dafaca", "md5", "prod_gp", "dat_mad", "no_bl_a", "location", "deb_loc", "fin_loc", "no_info", "motif_rebu", "col_vis", "no_serie", "cde_ori", "conv_pa", "k_opt", "lien_grc", "lien_gar", "lien_emp", "typ_ctr", "reg_deb", "nat_deb", "no_contrat", "type_prix", "hr_vp", "dat_vp", "qui_p", "contrat", "ind_con", "lig_con", "uni_fac", "cod_conv", "ndos", "s3_tar", "s4_tar", "nat_prodad", "cpx_prodad", "remise4", "ord_rem_1", "ord_rem_2", "ord_rem_3", "ord_rem_4", "typ_com", "reg_fact", "crit_cli_df", "typ_gra", "nb_ua", "nb_ca", "nb_uv", "nb_cv", "pp_ua", "pp_uv", "regr", "retr_cess", "retr_gere", "dat_acc", "dat_rll", "lien_frais", "typ_acc", "typ_cc", "dat_cc", "poid_net", "tare", "fret", "s3_famille", "s4_famille", "stk_csg", "list_crit", "qui_v", "num_lot", "num_colis", "no_mvtbois", "no_mvtobois", "no_opebois", "mag_cli", "mag_gen", "etat", "nom_pr2", "no_lot", "mt_ttc", "inc_deb", "rem_val_1", "rem_val_2", "rem_val_3", "rem_val_4", "nb_point", "ope_exc", "asso_lig", "eve_exc", "sous_trait", "niveau1", "vol_ach", "niveau2", "vol_cnv", "niveau3", "vol_mes", "niveau4", "vol_mtach", "niveau5", "px_rvt_bo", "no_unique", "ns_indrev", "mt_trsc", "num_prix", "rendement", "duree", "prc_perte", "motif", "no_ligct", "no_grp_c", "mtp_ach_p", "mtp_rvt_p", "mtp_ach_r", "mtp_rvt_r", "mtp_fac", "no_ctbois", "typ_fich", "materiel", "no_ordre", "px_franco", "px_std", "ach_ctr", "px_ree", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "px_trs", "consigne", "det_frais", "gen_csg", "lien_cona", "cpt_acc", "hr_mod", "no_tarif", "hr_crt", "lien_sscc", "no_batch", "ordre_deb", "derog_fou", "no_devis", "mt_pa", "mt_pr", "mt_pm", "ref_lig_bud", "grille_rem", "lib_ua1", "lib_ua2", "lib_ua3", "lib_uv1", "lib_uv2", "lib_uv3", "nb_ua1", "nb_ua2", "nb_ca3", "nb_uv1", "nb_uv2", "nb_cv3", "cod_op", "MScod_fou", "MScde_fou", "MSlig_fou", "crit_param", "crit_val", "pays_ori", "sous_total", "no_u_perte", "df_tax_1", "df_tax_2", "df_tax_3", "df_tax_4", "df_tax_5", "df_vte_1", "df_vte_2", "df_vte_3", "df_vte_4", "df_vte_5", "df_rvt_1", "df_rvt_2", "df_rvt_3", "df_rvt_4", "df_rvt_5", "df_mul_1", "df_mul_2", "df_mul_3", "df_mul_4", "df_mul_5", "motif_rem", "lien_valor", "vc_numlig", "vc_rang", "rei_deb", "nai_deb", "dci_deb", "px_deb", "ret_stk", "vpm", "vpr", "hr_mvt", "noedit", "tickbal", "rayon", "lib_bar", "uni_pv", "uni_pa", "qte_por", "com_port", "mt_frais", "px_rvt_vte", "cod_fou_df", "cod_prolie", "famillefl", "s_famillefl", "s3_famillefl", "s4_famillefl", "marquefl", "cod_rvt_ach", "cod_rvt_vte", "px_up", "nom_pr_web", "qte_up", "qte_ro", "uni_uc", "qte_rc", "qte_rb", "lissch", "marque", "lib_uc", "lib_pv", "TabPart_histolig", "flag_repli", "tare_p_1", "tare_p_2", "tare_p_3", "tare_ul_1", "tare_ul_2", "tare_ul_3", "valodffl", "prioritefl", "colis", "pal_sol", "lien_dffl", "apeser", "ssfraisfl", "typ_pal", "typ_col", "frai_prod", "lib_pa", "lien_facfo", "niveau6", "niveau7", "niveau8", "no_deb", "lig_deb", "ligsce", "sup_cons", "cond_rem_1", "cond_rem_2", "cond_rem_3", "cond_rem_4", "list_rem_1", "list_rem_2", "list_rem_3", "list_rem_4", "cod_nom", "coef_mar6", "intitule", "arrondi_pvdev", "mt_titresto",
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM changed_records
    
    UNION ALL
    
    -- Nouveaux enregistrements
    SELECT
        "uniq_id", "cod_pro", "cod_dec1", "cod_dec2", "cod_dec3", "cod_dec4", "cod_dec5", "depot", "dat_mvt", "typ_mvt", "ref_mvt", "es_mvt", "qte_unit", "px_unit", "sous_type", "typ_elem", "cod_cf", "no_cde", "no_bl", "div_fac", "no_ligne", "nom_pro", "qte", "px_vte", "remise1", "remise2", "rem_app", "refint", "groupe", "famille", "s_famille", "fam_cpt", "uni_vte", "lib_univ", "lib_conv", "lib_surv", "nb_univ", "nb_conv", "conv_px", "cod_fou", "refext", "devise", "px_ach", "uni_ach", "lib_unia", "lib_cona", "lib_sura", "nb_unia", "nb_cona", "fam_tar", "fam_pan", "code_tva", "enc_db", "calcul", "px_refv", "px_refa", "px_ach_d", "pmp", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "gratuit", "forcer", "dat_liv", "commerc_1", "commerc_2", "app_aff", "total", "tot_txt", "formule", "niv_pha_1", "niv_pha_2", "niv_pha_3", "typ_pha", "conv_sto", "px_vte_i", "dev_vte", "nmc_lie", "lien_imm", "lien_lot", "lien_nmc", "maj_stk", "edt_nmc", "conv_md", "conv_dec", "qte_cde", "n_surlig", "heure", "cout_trs", "longueur", "largeur", "hauteur", "calc_uv", "lib_unic", "qte_uc", "cal_marg", "coef_mar_1", "coef_mar_2", "coef_mar_3", "coef_mar_4", "coef_mar_5", "px_rvt", "coef_dep", "c_promo", "poi_var", "remise3", "poid_tot", "pvc", "maj_stat", "ref_con", "sf_tar", "affaire", "qui", "mt_ht", "lien_con", "master", "ord_nmc", "fac_poi", "cod_ori", "pret", "fin_pret", "dat_ret", "ret_pret", "garantie", "fin_gar", "cde_cf", "lig_fou", "demande", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "typ_rem_1", "typ_rem_2", "typ_rem_3", "qte_rlf", "mt_rlf", "volume", "zon_lib_1", "zon_lib_2", "zon_lib_3", "zon_lib_4", "zon_lib_5", "num_ave", "non_rfa", "tx_com", "conveuro", "no_of", "no_reafab", "no_reacons", "k_var", "no_lance", "nofaca", "dafaca", "md5", "prod_gp", "dat_mad", "no_bl_a", "location", "deb_loc", "fin_loc", "no_info", "motif_rebu", "col_vis", "no_serie", "cde_ori", "conv_pa", "k_opt", "lien_grc", "lien_gar", "lien_emp", "typ_ctr", "reg_deb", "nat_deb", "no_contrat", "type_prix", "hr_vp", "dat_vp", "qui_p", "contrat", "ind_con", "lig_con", "uni_fac", "cod_conv", "ndos", "s3_tar", "s4_tar", "nat_prodad", "cpx_prodad", "remise4", "ord_rem_1", "ord_rem_2", "ord_rem_3", "ord_rem_4", "typ_com", "reg_fact", "crit_cli_df", "typ_gra", "nb_ua", "nb_ca", "nb_uv", "nb_cv", "pp_ua", "pp_uv", "regr", "retr_cess", "retr_gere", "dat_acc", "dat_rll", "lien_frais", "typ_acc", "typ_cc", "dat_cc", "poid_net", "tare", "fret", "s3_famille", "s4_famille", "stk_csg", "list_crit", "qui_v", "num_lot", "num_colis", "no_mvtbois", "no_mvtobois", "no_opebois", "mag_cli", "mag_gen", "etat", "nom_pr2", "no_lot", "mt_ttc", "inc_deb", "rem_val_1", "rem_val_2", "rem_val_3", "rem_val_4", "nb_point", "ope_exc", "asso_lig", "eve_exc", "sous_trait", "niveau1", "vol_ach", "niveau2", "vol_cnv", "niveau3", "vol_mes", "niveau4", "vol_mtach", "niveau5", "px_rvt_bo", "no_unique", "ns_indrev", "mt_trsc", "num_prix", "rendement", "duree", "prc_perte", "motif", "no_ligct", "no_grp_c", "mtp_ach_p", "mtp_rvt_p", "mtp_ach_r", "mtp_rvt_r", "mtp_fac", "no_ctbois", "typ_fich", "materiel", "no_ordre", "px_franco", "px_std", "ach_ctr", "px_ree", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "px_trs", "consigne", "det_frais", "gen_csg", "lien_cona", "cpt_acc", "hr_mod", "no_tarif", "hr_crt", "lien_sscc", "no_batch", "ordre_deb", "derog_fou", "no_devis", "mt_pa", "mt_pr", "mt_pm", "ref_lig_bud", "grille_rem", "lib_ua1", "lib_ua2", "lib_ua3", "lib_uv1", "lib_uv2", "lib_uv3", "nb_ua1", "nb_ua2", "nb_ca3", "nb_uv1", "nb_uv2", "nb_cv3", "cod_op", "MScod_fou", "MScde_fou", "MSlig_fou", "crit_param", "crit_val", "pays_ori", "sous_total", "no_u_perte", "df_tax_1", "df_tax_2", "df_tax_3", "df_tax_4", "df_tax_5", "df_vte_1", "df_vte_2", "df_vte_3", "df_vte_4", "df_vte_5", "df_rvt_1", "df_rvt_2", "df_rvt_3", "df_rvt_4", "df_rvt_5", "df_mul_1", "df_mul_2", "df_mul_3", "df_mul_4", "df_mul_5", "motif_rem", "lien_valor", "vc_numlig", "vc_rang", "rei_deb", "nai_deb", "dci_deb", "px_deb", "ret_stk", "vpm", "vpr", "hr_mvt", "noedit", "tickbal", "rayon", "lib_bar", "uni_pv", "uni_pa", "qte_por", "com_port", "mt_frais", "px_rvt_vte", "cod_fou_df", "cod_prolie", "famillefl", "s_famillefl", "s3_famillefl", "s4_famillefl", "marquefl", "cod_rvt_ach", "cod_rvt_vte", "px_up", "nom_pr_web", "qte_up", "qte_ro", "uni_uc", "qte_rc", "qte_rb", "lissch", "marque", "lib_uc", "lib_pv", "TabPart_histolig", "flag_repli", "tare_p_1", "tare_p_2", "tare_p_3", "tare_ul_1", "tare_ul_2", "tare_ul_3", "valodffl", "prioritefl", "colis", "pal_sol", "lien_dffl", "apeser", "ssfraisfl", "typ_pal", "typ_col", "frai_prod", "lib_pa", "lien_facfo", "niveau6", "niveau7", "niveau8", "no_deb", "lig_deb", "ligsce", "sup_cons", "cond_rem_1", "cond_rem_2", "cond_rem_3", "cond_rem_4", "list_rem_1", "list_rem_2", "list_rem_3", "list_rem_4", "cod_nom", "coef_mar6", "intitule", "arrondi_pvdev", "mt_titresto",
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM new_records
    
    UNION ALL
    
    -- Mise à jour last_seen_date
    SELECT
        ods."uniq_id", ods."cod_pro", ods."cod_dec1", ods."cod_dec2", ods."cod_dec3", ods."cod_dec4", ods."cod_dec5", ods."depot", ods."dat_mvt", ods."typ_mvt", ods."ref_mvt", ods."es_mvt", ods."qte_unit", ods."px_unit", ods."sous_type", ods."typ_elem", ods."cod_cf", ods."no_cde", ods."no_bl", ods."div_fac", ods."no_ligne", ods."nom_pro", ods."qte", ods."px_vte", ods."remise1", ods."remise2", ods."rem_app", ods."refint", ods."groupe", ods."famille", ods."s_famille", ods."fam_cpt", ods."uni_vte", ods."lib_univ", ods."lib_conv", ods."lib_surv", ods."nb_univ", ods."nb_conv", ods."conv_px", ods."cod_fou", ods."refext", ods."devise", ods."px_ach", ods."uni_ach", ods."lib_unia", ods."lib_cona", ods."lib_sura", ods."nb_unia", ods."nb_cona", ods."fam_tar", ods."fam_pan", ods."code_tva", ods."enc_db", ods."calcul", ods."px_refv", ods."px_refa", ods."px_ach_d", ods."pmp", ods."zal_1", ods."zal_2", ods."zal_3", ods."zal_4", ods."zal_5", ods."znu_1", ods."znu_2", ods."znu_3", ods."znu_4", ods."znu_5", ods."zta_1", ods."zta_2", ods."zta_3", ods."zta_4", ods."zta_5", ods."zda_1", ods."zda_2", ods."zda_3", ods."zda_4", ods."zda_5", ods."gratuit", ods."forcer", ods."dat_liv", ods."commerc_1", ods."commerc_2", ods."app_aff", ods."total", ods."tot_txt", ods."formule", ods."niv_pha_1", ods."niv_pha_2", ods."niv_pha_3", ods."typ_pha", ods."conv_sto", ods."px_vte_i", ods."dev_vte", ods."nmc_lie", ods."lien_imm", ods."lien_lot", ods."lien_nmc", ods."maj_stk", ods."edt_nmc", ods."conv_md", ods."conv_dec", ods."qte_cde", ods."n_surlig", ods."heure", ods."cout_trs", ods."longueur", ods."largeur", ods."hauteur", ods."calc_uv", ods."lib_unic", ods."qte_uc", ods."cal_marg", ods."coef_mar_1", ods."coef_mar_2", ods."coef_mar_3", ods."coef_mar_4", ods."coef_mar_5", ods."px_rvt", ods."coef_dep", ods."c_promo", ods."poi_var", ods."remise3", ods."poid_tot", ods."pvc", ods."maj_stat", ods."ref_con", ods."sf_tar", ods."affaire", ods."qui", ods."mt_ht", ods."lien_con", ods."master", ods."ord_nmc", ods."fac_poi", ods."cod_ori", ods."pret", ods."fin_pret", ods."dat_ret", ods."ret_pret", ods."garantie", ods."fin_gar", ods."cde_cf", ods."lig_fou", ods."demande", ods."zlo_1", ods."zlo_2", ods."zlo_3", ods."zlo_4", ods."zlo_5", ods."typ_rem_1", ods."typ_rem_2", ods."typ_rem_3", ods."qte_rlf", ods."mt_rlf", ods."volume", ods."zon_lib_1", ods."zon_lib_2", ods."zon_lib_3", ods."zon_lib_4", ods."zon_lib_5", ods."num_ave", ods."non_rfa", ods."tx_com", ods."conveuro", ods."no_of", ods."no_reafab", ods."no_reacons", ods."k_var", ods."no_lance", ods."nofaca", ods."dafaca", ods."md5", ods."prod_gp", ods."dat_mad", ods."no_bl_a", ods."location", ods."deb_loc", ods."fin_loc", ods."no_info", ods."motif_rebu", ods."col_vis", ods."no_serie", ods."cde_ori", ods."conv_pa", ods."k_opt", ods."lien_grc", ods."lien_gar", ods."lien_emp", ods."typ_ctr", ods."reg_deb", ods."nat_deb", ods."no_contrat", ods."type_prix", ods."hr_vp", ods."dat_vp", ods."qui_p", ods."contrat", ods."ind_con", ods."lig_con", ods."uni_fac", ods."cod_conv", ods."ndos", ods."s3_tar", ods."s4_tar", ods."nat_prodad", ods."cpx_prodad", ods."remise4", ods."ord_rem_1", ods."ord_rem_2", ods."ord_rem_3", ods."ord_rem_4", ods."typ_com", ods."reg_fact", ods."crit_cli_df", ods."typ_gra", ods."nb_ua", ods."nb_ca", ods."nb_uv", ods."nb_cv", ods."pp_ua", ods."pp_uv", ods."regr", ods."retr_cess", ods."retr_gere", ods."dat_acc", ods."dat_rll", ods."lien_frais", ods."typ_acc", ods."typ_cc", ods."dat_cc", ods."poid_net", ods."tare", ods."fret", ods."s3_famille", ods."s4_famille", ods."stk_csg", ods."list_crit", ods."qui_v", ods."num_lot", ods."num_colis", ods."no_mvtbois", ods."no_mvtobois", ods."no_opebois", ods."mag_cli", ods."mag_gen", ods."etat", ods."nom_pr2", ods."no_lot", ods."mt_ttc", ods."inc_deb", ods."rem_val_1", ods."rem_val_2", ods."rem_val_3", ods."rem_val_4", ods."nb_point", ods."ope_exc", ods."asso_lig", ods."eve_exc", ods."sous_trait", ods."niveau1", ods."vol_ach", ods."niveau2", ods."vol_cnv", ods."niveau3", ods."vol_mes", ods."niveau4", ods."vol_mtach", ods."niveau5", ods."px_rvt_bo", ods."no_unique", ods."ns_indrev", ods."mt_trsc", ods."num_prix", ods."rendement", ods."duree", ods."prc_perte", ods."motif", ods."no_ligct", ods."no_grp_c", ods."mtp_ach_p", ods."mtp_rvt_p", ods."mtp_ach_r", ods."mtp_rvt_r", ods."mtp_fac", ods."no_ctbois", ods."typ_fich", ods."materiel", ods."no_ordre", ods."px_franco", ods."px_std", ods."ach_ctr", ods."px_ree", ods."usr_crt", ods."dat_crt", ods."usr_mod", ods."dat_mod", ods."px_trs", ods."consigne", ods."det_frais", ods."gen_csg", ods."lien_cona", ods."cpt_acc", ods."hr_mod", ods."no_tarif", ods."hr_crt", ods."lien_sscc", ods."no_batch", ods."ordre_deb", ods."derog_fou", ods."no_devis", ods."mt_pa", ods."mt_pr", ods."mt_pm", ods."ref_lig_bud", ods."grille_rem", ods."lib_ua1", ods."lib_ua2", ods."lib_ua3", ods."lib_uv1", ods."lib_uv2", ods."lib_uv3", ods."nb_ua1", ods."nb_ua2", ods."nb_ca3", ods."nb_uv1", ods."nb_uv2", ods."nb_cv3", ods."cod_op", ods."MScod_fou", ods."MScde_fou", ods."MSlig_fou", ods."crit_param", ods."crit_val", ods."pays_ori", ods."sous_total", ods."no_u_perte", ods."df_tax_1", ods."df_tax_2", ods."df_tax_3", ods."df_tax_4", ods."df_tax_5", ods."df_vte_1", ods."df_vte_2", ods."df_vte_3", ods."df_vte_4", ods."df_vte_5", ods."df_rvt_1", ods."df_rvt_2", ods."df_rvt_3", ods."df_rvt_4", ods."df_rvt_5", ods."df_mul_1", ods."df_mul_2", ods."df_mul_3", ods."df_mul_4", ods."df_mul_5", ods."motif_rem", ods."lien_valor", ods."vc_numlig", ods."vc_rang", ods."rei_deb", ods."nai_deb", ods."dci_deb", ods."px_deb", ods."ret_stk", ods."vpm", ods."vpr", ods."hr_mvt", ods."noedit", ods."tickbal", ods."rayon", ods."lib_bar", ods."uni_pv", ods."uni_pa", ods."qte_por", ods."com_port", ods."mt_frais", ods."px_rvt_vte", ods."cod_fou_df", ods."cod_prolie", ods."famillefl", ods."s_famillefl", ods."s3_famillefl", ods."s4_famillefl", ods."marquefl", ods."cod_rvt_ach", ods."cod_rvt_vte", ods."px_up", ods."nom_pr_web", ods."qte_up", ods."qte_ro", ods."uni_uc", ods."qte_rc", ods."qte_rb", ods."lissch", ods."marque", ods."lib_uc", ods."lib_pv", ods."TabPart_histolig", ods."flag_repli", ods."tare_p_1", ods."tare_p_2", ods."tare_p_3", ods."tare_ul_1", ods."tare_ul_2", ods."tare_ul_3", ods."valodffl", ods."prioritefl", ods."colis", ods."pal_sol", ods."lien_dffl", ods."apeser", ods."ssfraisfl", ods."typ_pal", ods."typ_col", ods."frai_prod", ods."lib_pa", ods."lien_facfo", ods."niveau6", ods."niveau7", ods."niveau8", ods."no_deb", ods."lig_deb", ods."ligsce", ods."sup_cons", ods."cond_rem_1", ods."cond_rem_2", ods."cond_rem_3", ods."cond_rem_4", ods."list_rem_1", ods."list_rem_2", ods."list_rem_3", ods."list_rem_4", ods."cod_nom", ods."coef_mar6", ods."intitule", ods."arrondi_pvdev", ods."mt_titresto",
        ods."_etl_hashdiff",
        ods."_etl_valid_from",
        ods."_etl_valid_to",
        ods."_etl_is_current",
        ods."_etl_is_deleted",
        ur."new_last_seen" AS "last_seen_date"
    FROM {{ this }} ods
    INNER JOIN unchanged_records ur USING ("uniq_id")
    WHERE ods."_etl_is_current" = TRUE
    
    UNION ALL
    
    -- Historique ancien
    SELECT * FROM {{ this }}
    WHERE "_etl_is_current" = FALSE
)

{% else %}

-- Initial load
final AS (
    SELECT
        "uniq_id", "cod_pro", "cod_dec1", "cod_dec2", "cod_dec3", "cod_dec4", "cod_dec5", "depot", "dat_mvt", "typ_mvt", "ref_mvt", "es_mvt", "qte_unit", "px_unit", "sous_type", "typ_elem", "cod_cf", "no_cde", "no_bl", "div_fac", "no_ligne", "nom_pro", "qte", "px_vte", "remise1", "remise2", "rem_app", "refint", "groupe", "famille", "s_famille", "fam_cpt", "uni_vte", "lib_univ", "lib_conv", "lib_surv", "nb_univ", "nb_conv", "conv_px", "cod_fou", "refext", "devise", "px_ach", "uni_ach", "lib_unia", "lib_cona", "lib_sura", "nb_unia", "nb_cona", "fam_tar", "fam_pan", "code_tva", "enc_db", "calcul", "px_refv", "px_refa", "px_ach_d", "pmp", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "gratuit", "forcer", "dat_liv", "commerc_1", "commerc_2", "app_aff", "total", "tot_txt", "formule", "niv_pha_1", "niv_pha_2", "niv_pha_3", "typ_pha", "conv_sto", "px_vte_i", "dev_vte", "nmc_lie", "lien_imm", "lien_lot", "lien_nmc", "maj_stk", "edt_nmc", "conv_md", "conv_dec", "qte_cde", "n_surlig", "heure", "cout_trs", "longueur", "largeur", "hauteur", "calc_uv", "lib_unic", "qte_uc", "cal_marg", "coef_mar_1", "coef_mar_2", "coef_mar_3", "coef_mar_4", "coef_mar_5", "px_rvt", "coef_dep", "c_promo", "poi_var", "remise3", "poid_tot", "pvc", "maj_stat", "ref_con", "sf_tar", "affaire", "qui", "mt_ht", "lien_con", "master", "ord_nmc", "fac_poi", "cod_ori", "pret", "fin_pret", "dat_ret", "ret_pret", "garantie", "fin_gar", "cde_cf", "lig_fou", "demande", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "typ_rem_1", "typ_rem_2", "typ_rem_3", "qte_rlf", "mt_rlf", "volume", "zon_lib_1", "zon_lib_2", "zon_lib_3", "zon_lib_4", "zon_lib_5", "num_ave", "non_rfa", "tx_com", "conveuro", "no_of", "no_reafab", "no_reacons", "k_var", "no_lance", "nofaca", "dafaca", "md5", "prod_gp", "dat_mad", "no_bl_a", "location", "deb_loc", "fin_loc", "no_info", "motif_rebu", "col_vis", "no_serie", "cde_ori", "conv_pa", "k_opt", "lien_grc", "lien_gar", "lien_emp", "typ_ctr", "reg_deb", "nat_deb", "no_contrat", "type_prix", "hr_vp", "dat_vp", "qui_p", "contrat", "ind_con", "lig_con", "uni_fac", "cod_conv", "ndos", "s3_tar", "s4_tar", "nat_prodad", "cpx_prodad", "remise4", "ord_rem_1", "ord_rem_2", "ord_rem_3", "ord_rem_4", "typ_com", "reg_fact", "crit_cli_df", "typ_gra", "nb_ua", "nb_ca", "nb_uv", "nb_cv", "pp_ua", "pp_uv", "regr", "retr_cess", "retr_gere", "dat_acc", "dat_rll", "lien_frais", "typ_acc", "typ_cc", "dat_cc", "poid_net", "tare", "fret", "s3_famille", "s4_famille", "stk_csg", "list_crit", "qui_v", "num_lot", "num_colis", "no_mvtbois", "no_mvtobois", "no_opebois", "mag_cli", "mag_gen", "etat", "nom_pr2", "no_lot", "mt_ttc", "inc_deb", "rem_val_1", "rem_val_2", "rem_val_3", "rem_val_4", "nb_point", "ope_exc", "asso_lig", "eve_exc", "sous_trait", "niveau1", "vol_ach", "niveau2", "vol_cnv", "niveau3", "vol_mes", "niveau4", "vol_mtach", "niveau5", "px_rvt_bo", "no_unique", "ns_indrev", "mt_trsc", "num_prix", "rendement", "duree", "prc_perte", "motif", "no_ligct", "no_grp_c", "mtp_ach_p", "mtp_rvt_p", "mtp_ach_r", "mtp_rvt_r", "mtp_fac", "no_ctbois", "typ_fich", "materiel", "no_ordre", "px_franco", "px_std", "ach_ctr", "px_ree", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "px_trs", "consigne", "det_frais", "gen_csg", "lien_cona", "cpt_acc", "hr_mod", "no_tarif", "hr_crt", "lien_sscc", "no_batch", "ordre_deb", "derog_fou", "no_devis", "mt_pa", "mt_pr", "mt_pm", "ref_lig_bud", "grille_rem", "lib_ua1", "lib_ua2", "lib_ua3", "lib_uv1", "lib_uv2", "lib_uv3", "nb_ua1", "nb_ua2", "nb_ca3", "nb_uv1", "nb_uv2", "nb_cv3", "cod_op", "MScod_fou", "MScde_fou", "MSlig_fou", "crit_param", "crit_val", "pays_ori", "sous_total", "no_u_perte", "df_tax_1", "df_tax_2", "df_tax_3", "df_tax_4", "df_tax_5", "df_vte_1", "df_vte_2", "df_vte_3", "df_vte_4", "df_vte_5", "df_rvt_1", "df_rvt_2", "df_rvt_3", "df_rvt_4", "df_rvt_5", "df_mul_1", "df_mul_2", "df_mul_3", "df_mul_4", "df_mul_5", "motif_rem", "lien_valor", "vc_numlig", "vc_rang", "rei_deb", "nai_deb", "dci_deb", "px_deb", "ret_stk", "vpm", "vpr", "hr_mvt", "noedit", "tickbal", "rayon", "lib_bar", "uni_pv", "uni_pa", "qte_por", "com_port", "mt_frais", "px_rvt_vte", "cod_fou_df", "cod_prolie", "famillefl", "s_famillefl", "s3_famillefl", "s4_famillefl", "marquefl", "cod_rvt_ach", "cod_rvt_vte", "px_up", "nom_pr_web", "qte_up", "qte_ro", "uni_uc", "qte_rc", "qte_rb", "lissch", "marque", "lib_uc", "lib_pv", "TabPart_histolig", "flag_repli", "tare_p_1", "tare_p_2", "tare_p_3", "tare_ul_1", "tare_ul_2", "tare_ul_3", "valodffl", "prioritefl", "colis", "pal_sol", "lien_dffl", "apeser", "ssfraisfl", "typ_pal", "typ_col", "frai_prod", "lib_pa", "lien_facfo", "niveau6", "niveau7", "niveau8", "no_deb", "lig_deb", "ligsce", "sup_cons", "cond_rem_1", "cond_rem_2", "cond_rem_3", "cond_rem_4", "list_rem_1", "list_rem_2", "list_rem_3", "list_rem_4", "cod_nom", "coef_mar6", "intitule", "arrondi_pvdev", "mt_titresto",
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM source_data
)

{% endif %}

SELECT * FROM final

{% if is_incremental() %}
WHERE "_etl_valid_to" IS NULL 
   OR "_etl_valid_to" >= CURRENT_DATE - INTERVAL '730 days'
{% endif %}
