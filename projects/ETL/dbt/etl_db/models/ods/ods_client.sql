{{
    config(
        materialized='incremental',
        unique_key=['cod_cli'],
        incremental_strategy='merge',
        on_schema_change='fail',
        schema='ods_dbt'
    )
}}

/*
============================================================================
ODS Model: client
============================================================================
Description : Table des clients
Primary Key : cod_cli
Detect Del. : Yes
Retention   : 730 days
============================================================================
*/

WITH source_data AS (
    SELECT
        "cod_cli",
        "typ_elem", "nom_cli", "ville", "k_post", "regime", "type_fac", "form_jur", "capital", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "statut", "qui_sta", "com_sta", "dat_sta", "chif_bl", "fac_dvs", "fac_ttc", "fac_pxa", "fra_app", "liasse", "div_fac_1", "div_fac_2", "div_fac_3", "val_fac_1", "val_fac_2", "val_fac_3", "fam_art_1", "fam_art_2", "fam_art_3", "fam_art_4", "fam_art_5", "depot", "transpor", "mod_liv", "edt_bp", "edt_arc", "mnt_rlk_1", "mnt_rlk_2", "mnt_rlk_3", "mnt_rlk_4", "ref_cde", "proforma", "televen", "cal_cde", "cal_liv", "heure", "com_tel", "jou_dec", "cl_grp", "cl_livre", "cl_fact", "cl_paye", "tarif", "cl_stat", "region", "famille", "cat_tar", "commerc_1", "commerc_2", "app_aff", "tel_j1", "tel_j2", "tel_j3", "tel_j4", "tel_j5", "tel_j6", "tel_j7", "repert", "no_tarif", "borne", "tx_ech_d", "vte_spe", "gencod", "cl_plnat", "cl_plreg", "maj_ach", "pays", "internet", "tva_ech", "mot_cle", "zon_geo", "zon_lib", "notel", "rem_glo_1", "rem_glo_2", "cde_cpl", "fact_con", "coeff_pvc", "liv_cpl", "arr_bi_1", "arr_bi_2", "arr_bi_3", "arr_bi_4", "arr_bi_5", "arr_bs_1", "arr_bs_2", "arr_bs_3", "arr_bs_4", "arr_bs_5", "arr_r_1", "arr_r_2", "arr_r_3", "arr_r_4", "arr_r_5", "fac_poi", "c_factor", "transit", "grp_ps", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "k_post2", "blok_enc", "effectif", "ca_client", "ca_pot", "login", "log_mdp", "gencod_det", "ord_affec", "priorite", "jalon_tot", "int_rrr_1", "int_rrr_2", "int_rrr_3", "int_rrr_4", "int_rrr_5", "int_rrr_6", "ben_rrr_1", "ben_rrr_2", "ben_rrr_3", "ben_rrr_4", "ben_rrr_5", "ben_rrr_6", "fvte", "cod_adh", "cadencier", "interv_1", "interv_2", "interv_3", "interv_4", "interv_5", "mt_mini", "plus_bpbl", "bl_regr", "tx_com", "inc_deb", "trs_deb", "dev_cap", "mt_franco", "fr_cde", "s2_famille", "s3_famille", "s4_famille", "promo", "rem_lig", "cod_tlv", "frq_a_nul", "frq_v_nul", "mod_a", "mod_v", "cal_v", "hr_v", "vis_j1", "vis_j2", "vis_j3", "vis_j4", "vis_j5", "vis_j6", "vis_j7", "prep_isol", "commercial", "com_v", "f_verifcde_1", "f_verifcde_2", "f_verifcde_3", "enc_imp", "plan_rrr", "szon_geo", "origine", "s2_cat", "s3_cat", "s4_cat", "qui_stn_1", "qui_stn_2", "qui_stn_3", "qui_stn_4", "qui_stn_5", "qui_stn_6", "qui_stn_7", "qui_stn_8", "qui_stn_9", "qui_ver", "rq_pan", "typ_con", "typ_pxa", "c_retr", "cl_prc", "ffc_prc", "rfa_soc", "interloc_1", "interloc_2", "adresse_1", "adresse_2", "adresse_3", "zone_exp", "cpt_web", "f_prc", "demat_fac", "non_bloc", "phone", "pref_ref", "dgc", "no_tar_loc", "meth_rf", "siret", "ty_mini", "bl_mini", "prc_rep", "retour_date", "cat_cum", "tar_fil_1", "tar_fil_2", "tar_fil_3", "tar_fil_4", "tar_fil_5", "tar_fil_6", "tar_cum_1", "tar_cum_2", "tar_cum_3", "tar_cum_4", "tar_cum_5", "tar_cum_6", "typ_cad", "dep_unq", "lias_bl", "canal", "notation", "no_ctrl", "typ_floc", "contrat_date", "date_px", "or_ces", "typ_cam", "marge_p", "typ_veh", "prix_franco", "rlk_mnt", "no_port", "maqf_etq", "typ_sup", "groupe", "dep_uniq", "sscc_sor", "refrec", "refrec2", "vc_subst_g", "uni_ref", "bloq_prep", "cl_plcon", "cl_pere", "rayon", "tar_fi2_1", "tar_fi2_2", "tar_fi2_3", "tar_fi2_4", "tar_cu2_1", "tar_cu2_2", "tar_cu2_3", "tar_cu2_4", "cal_enc", "prc_aco", "mod_aco", "TabPart_client", "flag_repli", "jamais_peser", "hr_app_1", "hr_app_2", "hr_app_3", "hr_app_4", "hr_app_5", "hr_app_6", "hr_app_7", "multitrp", "adresse5", "or_spereg", "fac_liv", "conso_exc",
        "_etl_hashdiff",
        "_etl_valid_from"
    FROM {{ source('staging', 'stg_client') }}
    
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
        "cod_cli",
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
    LEFT JOIN current_records ods USING ("cod_cli")
    WHERE ods."cod_cli" IS NULL
),

changed_records AS (
    SELECT 
        stg.*,
        ods."_etl_valid_from" AS previous_valid_from
    FROM source_data stg
    INNER JOIN current_records ods USING ("cod_cli")
    WHERE stg."_etl_hashdiff" != ods."_etl_hashdiff"
),

unchanged_records AS (
    SELECT
        "cod_cli",
        stg."_etl_valid_from" AS new_last_seen
    FROM current_records ods
    INNER JOIN source_data stg USING ("cod_cli")
    WHERE stg."_etl_hashdiff" = ods."_etl_hashdiff"
),

{% if var('load_mode', 'INCREMENTAL') == 'FULL' %}
deleted_records AS (
    SELECT "cod_cli"
    FROM current_records ods
    LEFT JOIN source_data stg USING ("cod_cli")
    WHERE stg.cod_cli IS NULL
),
{% endif %}

final AS (
    
    -- Clôturer anciennes versions
    SELECT
        ods.*,
        cr."_etl_valid_from" AS "_etl_valid_to",
        FALSE AS "_etl_is_current",
        ods."last_seen_date"
    FROM {{ this }} ods
    INNER JOIN changed_records cr USING ("cod_cli")
    WHERE ods."_etl_valid_from" = cr.previous_valid_from
      AND ods."_etl_is_current" = TRUE
    
    UNION ALL
    
    -- Nouvelles versions
    SELECT
        "cod_cli", "typ_elem", "nom_cli", "ville", "k_post", "regime", "type_fac", "form_jur", "capital", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "statut", "qui_sta", "com_sta", "dat_sta", "chif_bl", "fac_dvs", "fac_ttc", "fac_pxa", "fra_app", "liasse", "div_fac_1", "div_fac_2", "div_fac_3", "val_fac_1", "val_fac_2", "val_fac_3", "fam_art_1", "fam_art_2", "fam_art_3", "fam_art_4", "fam_art_5", "depot", "transpor", "mod_liv", "edt_bp", "edt_arc", "mnt_rlk_1", "mnt_rlk_2", "mnt_rlk_3", "mnt_rlk_4", "ref_cde", "proforma", "televen", "cal_cde", "cal_liv", "heure", "com_tel", "jou_dec", "cl_grp", "cl_livre", "cl_fact", "cl_paye", "tarif", "cl_stat", "region", "famille", "cat_tar", "commerc_1", "commerc_2", "app_aff", "tel_j1", "tel_j2", "tel_j3", "tel_j4", "tel_j5", "tel_j6", "tel_j7", "repert", "no_tarif", "borne", "tx_ech_d", "vte_spe", "gencod", "cl_plnat", "cl_plreg", "maj_ach", "pays", "internet", "tva_ech", "mot_cle", "zon_geo", "zon_lib", "notel", "rem_glo_1", "rem_glo_2", "cde_cpl", "fact_con", "coeff_pvc", "liv_cpl", "arr_bi_1", "arr_bi_2", "arr_bi_3", "arr_bi_4", "arr_bi_5", "arr_bs_1", "arr_bs_2", "arr_bs_3", "arr_bs_4", "arr_bs_5", "arr_r_1", "arr_r_2", "arr_r_3", "arr_r_4", "arr_r_5", "fac_poi", "c_factor", "transit", "grp_ps", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "k_post2", "blok_enc", "effectif", "ca_client", "ca_pot", "login", "log_mdp", "gencod_det", "ord_affec", "priorite", "jalon_tot", "int_rrr_1", "int_rrr_2", "int_rrr_3", "int_rrr_4", "int_rrr_5", "int_rrr_6", "ben_rrr_1", "ben_rrr_2", "ben_rrr_3", "ben_rrr_4", "ben_rrr_5", "ben_rrr_6", "fvte", "cod_adh", "cadencier", "interv_1", "interv_2", "interv_3", "interv_4", "interv_5", "mt_mini", "plus_bpbl", "bl_regr", "tx_com", "inc_deb", "trs_deb", "dev_cap", "mt_franco", "fr_cde", "s2_famille", "s3_famille", "s4_famille", "promo", "rem_lig", "cod_tlv", "frq_a_nul", "frq_v_nul", "mod_a", "mod_v", "cal_v", "hr_v", "vis_j1", "vis_j2", "vis_j3", "vis_j4", "vis_j5", "vis_j6", "vis_j7", "prep_isol", "commercial", "com_v", "f_verifcde_1", "f_verifcde_2", "f_verifcde_3", "enc_imp", "plan_rrr", "szon_geo", "origine", "s2_cat", "s3_cat", "s4_cat", "qui_stn_1", "qui_stn_2", "qui_stn_3", "qui_stn_4", "qui_stn_5", "qui_stn_6", "qui_stn_7", "qui_stn_8", "qui_stn_9", "qui_ver", "rq_pan", "typ_con", "typ_pxa", "c_retr", "cl_prc", "ffc_prc", "rfa_soc", "interloc_1", "interloc_2", "adresse_1", "adresse_2", "adresse_3", "zone_exp", "cpt_web", "f_prc", "demat_fac", "non_bloc", "phone", "pref_ref", "dgc", "no_tar_loc", "meth_rf", "siret", "ty_mini", "bl_mini", "prc_rep", "retour_date", "cat_cum", "tar_fil_1", "tar_fil_2", "tar_fil_3", "tar_fil_4", "tar_fil_5", "tar_fil_6", "tar_cum_1", "tar_cum_2", "tar_cum_3", "tar_cum_4", "tar_cum_5", "tar_cum_6", "typ_cad", "dep_unq", "lias_bl", "canal", "notation", "no_ctrl", "typ_floc", "contrat_date", "date_px", "or_ces", "typ_cam", "marge_p", "typ_veh", "prix_franco", "rlk_mnt", "no_port", "maqf_etq", "typ_sup", "groupe", "dep_uniq", "sscc_sor", "refrec", "refrec2", "vc_subst_g", "uni_ref", "bloq_prep", "cl_plcon", "cl_pere", "rayon", "tar_fi2_1", "tar_fi2_2", "tar_fi2_3", "tar_fi2_4", "tar_cu2_1", "tar_cu2_2", "tar_cu2_3", "tar_cu2_4", "cal_enc", "prc_aco", "mod_aco", "TabPart_client", "flag_repli", "jamais_peser", "hr_app_1", "hr_app_2", "hr_app_3", "hr_app_4", "hr_app_5", "hr_app_6", "hr_app_7", "multitrp", "adresse5", "or_spereg", "fac_liv", "conso_exc",
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
        "cod_cli", "typ_elem", "nom_cli", "ville", "k_post", "regime", "type_fac", "form_jur", "capital", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "statut", "qui_sta", "com_sta", "dat_sta", "chif_bl", "fac_dvs", "fac_ttc", "fac_pxa", "fra_app", "liasse", "div_fac_1", "div_fac_2", "div_fac_3", "val_fac_1", "val_fac_2", "val_fac_3", "fam_art_1", "fam_art_2", "fam_art_3", "fam_art_4", "fam_art_5", "depot", "transpor", "mod_liv", "edt_bp", "edt_arc", "mnt_rlk_1", "mnt_rlk_2", "mnt_rlk_3", "mnt_rlk_4", "ref_cde", "proforma", "televen", "cal_cde", "cal_liv", "heure", "com_tel", "jou_dec", "cl_grp", "cl_livre", "cl_fact", "cl_paye", "tarif", "cl_stat", "region", "famille", "cat_tar", "commerc_1", "commerc_2", "app_aff", "tel_j1", "tel_j2", "tel_j3", "tel_j4", "tel_j5", "tel_j6", "tel_j7", "repert", "no_tarif", "borne", "tx_ech_d", "vte_spe", "gencod", "cl_plnat", "cl_plreg", "maj_ach", "pays", "internet", "tva_ech", "mot_cle", "zon_geo", "zon_lib", "notel", "rem_glo_1", "rem_glo_2", "cde_cpl", "fact_con", "coeff_pvc", "liv_cpl", "arr_bi_1", "arr_bi_2", "arr_bi_3", "arr_bi_4", "arr_bi_5", "arr_bs_1", "arr_bs_2", "arr_bs_3", "arr_bs_4", "arr_bs_5", "arr_r_1", "arr_r_2", "arr_r_3", "arr_r_4", "arr_r_5", "fac_poi", "c_factor", "transit", "grp_ps", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "k_post2", "blok_enc", "effectif", "ca_client", "ca_pot", "login", "log_mdp", "gencod_det", "ord_affec", "priorite", "jalon_tot", "int_rrr_1", "int_rrr_2", "int_rrr_3", "int_rrr_4", "int_rrr_5", "int_rrr_6", "ben_rrr_1", "ben_rrr_2", "ben_rrr_3", "ben_rrr_4", "ben_rrr_5", "ben_rrr_6", "fvte", "cod_adh", "cadencier", "interv_1", "interv_2", "interv_3", "interv_4", "interv_5", "mt_mini", "plus_bpbl", "bl_regr", "tx_com", "inc_deb", "trs_deb", "dev_cap", "mt_franco", "fr_cde", "s2_famille", "s3_famille", "s4_famille", "promo", "rem_lig", "cod_tlv", "frq_a_nul", "frq_v_nul", "mod_a", "mod_v", "cal_v", "hr_v", "vis_j1", "vis_j2", "vis_j3", "vis_j4", "vis_j5", "vis_j6", "vis_j7", "prep_isol", "commercial", "com_v", "f_verifcde_1", "f_verifcde_2", "f_verifcde_3", "enc_imp", "plan_rrr", "szon_geo", "origine", "s2_cat", "s3_cat", "s4_cat", "qui_stn_1", "qui_stn_2", "qui_stn_3", "qui_stn_4", "qui_stn_5", "qui_stn_6", "qui_stn_7", "qui_stn_8", "qui_stn_9", "qui_ver", "rq_pan", "typ_con", "typ_pxa", "c_retr", "cl_prc", "ffc_prc", "rfa_soc", "interloc_1", "interloc_2", "adresse_1", "adresse_2", "adresse_3", "zone_exp", "cpt_web", "f_prc", "demat_fac", "non_bloc", "phone", "pref_ref", "dgc", "no_tar_loc", "meth_rf", "siret", "ty_mini", "bl_mini", "prc_rep", "retour_date", "cat_cum", "tar_fil_1", "tar_fil_2", "tar_fil_3", "tar_fil_4", "tar_fil_5", "tar_fil_6", "tar_cum_1", "tar_cum_2", "tar_cum_3", "tar_cum_4", "tar_cum_5", "tar_cum_6", "typ_cad", "dep_unq", "lias_bl", "canal", "notation", "no_ctrl", "typ_floc", "contrat_date", "date_px", "or_ces", "typ_cam", "marge_p", "typ_veh", "prix_franco", "rlk_mnt", "no_port", "maqf_etq", "typ_sup", "groupe", "dep_uniq", "sscc_sor", "refrec", "refrec2", "vc_subst_g", "uni_ref", "bloq_prep", "cl_plcon", "cl_pere", "rayon", "tar_fi2_1", "tar_fi2_2", "tar_fi2_3", "tar_fi2_4", "tar_cu2_1", "tar_cu2_2", "tar_cu2_3", "tar_cu2_4", "cal_enc", "prc_aco", "mod_aco", "TabPart_client", "flag_repli", "jamais_peser", "hr_app_1", "hr_app_2", "hr_app_3", "hr_app_4", "hr_app_5", "hr_app_6", "hr_app_7", "multitrp", "adresse5", "or_spereg", "fac_liv", "conso_exc",
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
        ods."cod_cli", ods."typ_elem", ods."nom_cli", ods."ville", ods."k_post", ods."regime", ods."type_fac", ods."form_jur", ods."capital", ods."usr_crt", ods."dat_crt", ods."usr_mod", ods."dat_mod", ods."zal_1", ods."zal_2", ods."zal_3", ods."zal_4", ods."zal_5", ods."znu_1", ods."znu_2", ods."znu_3", ods."znu_4", ods."znu_5", ods."zta_1", ods."zta_2", ods."zta_3", ods."zta_4", ods."zta_5", ods."zda_1", ods."zda_2", ods."zda_3", ods."zda_4", ods."zda_5", ods."statut", ods."qui_sta", ods."com_sta", ods."dat_sta", ods."chif_bl", ods."fac_dvs", ods."fac_ttc", ods."fac_pxa", ods."fra_app", ods."liasse", ods."div_fac_1", ods."div_fac_2", ods."div_fac_3", ods."val_fac_1", ods."val_fac_2", ods."val_fac_3", ods."fam_art_1", ods."fam_art_2", ods."fam_art_3", ods."fam_art_4", ods."fam_art_5", ods."depot", ods."transpor", ods."mod_liv", ods."edt_bp", ods."edt_arc", ods."mnt_rlk_1", ods."mnt_rlk_2", ods."mnt_rlk_3", ods."mnt_rlk_4", ods."ref_cde", ods."proforma", ods."televen", ods."cal_cde", ods."cal_liv", ods."heure", ods."com_tel", ods."jou_dec", ods."cl_grp", ods."cl_livre", ods."cl_fact", ods."cl_paye", ods."tarif", ods."cl_stat", ods."region", ods."famille", ods."cat_tar", ods."commerc_1", ods."commerc_2", ods."app_aff", ods."tel_j1", ods."tel_j2", ods."tel_j3", ods."tel_j4", ods."tel_j5", ods."tel_j6", ods."tel_j7", ods."repert", ods."no_tarif", ods."borne", ods."tx_ech_d", ods."vte_spe", ods."gencod", ods."cl_plnat", ods."cl_plreg", ods."maj_ach", ods."pays", ods."internet", ods."tva_ech", ods."mot_cle", ods."zon_geo", ods."zon_lib", ods."notel", ods."rem_glo_1", ods."rem_glo_2", ods."cde_cpl", ods."fact_con", ods."coeff_pvc", ods."liv_cpl", ods."arr_bi_1", ods."arr_bi_2", ods."arr_bi_3", ods."arr_bi_4", ods."arr_bi_5", ods."arr_bs_1", ods."arr_bs_2", ods."arr_bs_3", ods."arr_bs_4", ods."arr_bs_5", ods."arr_r_1", ods."arr_r_2", ods."arr_r_3", ods."arr_r_4", ods."arr_r_5", ods."fac_poi", ods."c_factor", ods."transit", ods."grp_ps", ods."zlo_1", ods."zlo_2", ods."zlo_3", ods."zlo_4", ods."zlo_5", ods."k_post2", ods."blok_enc", ods."effectif", ods."ca_client", ods."ca_pot", ods."login", ods."log_mdp", ods."gencod_det", ods."ord_affec", ods."priorite", ods."jalon_tot", ods."int_rrr_1", ods."int_rrr_2", ods."int_rrr_3", ods."int_rrr_4", ods."int_rrr_5", ods."int_rrr_6", ods."ben_rrr_1", ods."ben_rrr_2", ods."ben_rrr_3", ods."ben_rrr_4", ods."ben_rrr_5", ods."ben_rrr_6", ods."fvte", ods."cod_adh", ods."cadencier", ods."interv_1", ods."interv_2", ods."interv_3", ods."interv_4", ods."interv_5", ods."mt_mini", ods."plus_bpbl", ods."bl_regr", ods."tx_com", ods."inc_deb", ods."trs_deb", ods."dev_cap", ods."mt_franco", ods."fr_cde", ods."s2_famille", ods."s3_famille", ods."s4_famille", ods."promo", ods."rem_lig", ods."cod_tlv", ods."frq_a_nul", ods."frq_v_nul", ods."mod_a", ods."mod_v", ods."cal_v", ods."hr_v", ods."vis_j1", ods."vis_j2", ods."vis_j3", ods."vis_j4", ods."vis_j5", ods."vis_j6", ods."vis_j7", ods."prep_isol", ods."commercial", ods."com_v", ods."f_verifcde_1", ods."f_verifcde_2", ods."f_verifcde_3", ods."enc_imp", ods."plan_rrr", ods."szon_geo", ods."origine", ods."s2_cat", ods."s3_cat", ods."s4_cat", ods."qui_stn_1", ods."qui_stn_2", ods."qui_stn_3", ods."qui_stn_4", ods."qui_stn_5", ods."qui_stn_6", ods."qui_stn_7", ods."qui_stn_8", ods."qui_stn_9", ods."qui_ver", ods."rq_pan", ods."typ_con", ods."typ_pxa", ods."c_retr", ods."cl_prc", ods."ffc_prc", ods."rfa_soc", ods."interloc_1", ods."interloc_2", ods."adresse_1", ods."adresse_2", ods."adresse_3", ods."zone_exp", ods."cpt_web", ods."f_prc", ods."demat_fac", ods."non_bloc", ods."phone", ods."pref_ref", ods."dgc", ods."no_tar_loc", ods."meth_rf", ods."siret", ods."ty_mini", ods."bl_mini", ods."prc_rep", ods."retour_date", ods."cat_cum", ods."tar_fil_1", ods."tar_fil_2", ods."tar_fil_3", ods."tar_fil_4", ods."tar_fil_5", ods."tar_fil_6", ods."tar_cum_1", ods."tar_cum_2", ods."tar_cum_3", ods."tar_cum_4", ods."tar_cum_5", ods."tar_cum_6", ods."typ_cad", ods."dep_unq", ods."lias_bl", ods."canal", ods."notation", ods."no_ctrl", ods."typ_floc", ods."contrat_date", ods."date_px", ods."or_ces", ods."typ_cam", ods."marge_p", ods."typ_veh", ods."prix_franco", ods."rlk_mnt", ods."no_port", ods."maqf_etq", ods."typ_sup", ods."groupe", ods."dep_uniq", ods."sscc_sor", ods."refrec", ods."refrec2", ods."vc_subst_g", ods."uni_ref", ods."bloq_prep", ods."cl_plcon", ods."cl_pere", ods."rayon", ods."tar_fi2_1", ods."tar_fi2_2", ods."tar_fi2_3", ods."tar_fi2_4", ods."tar_cu2_1", ods."tar_cu2_2", ods."tar_cu2_3", ods."tar_cu2_4", ods."cal_enc", ods."prc_aco", ods."mod_aco", ods."TabPart_client", ods."flag_repli", ods."jamais_peser", ods."hr_app_1", ods."hr_app_2", ods."hr_app_3", ods."hr_app_4", ods."hr_app_5", ods."hr_app_6", ods."hr_app_7", ods."multitrp", ods."adresse5", ods."or_spereg", ods."fac_liv", ods."conso_exc",
        ods."_etl_hashdiff",
        ods."_etl_valid_from",
        ods."_etl_valid_to",
        ods."_etl_is_current",
        ods."_etl_is_deleted",
        ur."new_last_seen" AS "last_seen_date"
    FROM {{ this }} ods
    INNER JOIN unchanged_records ur USING ("cod_cli")
    WHERE ods."_etl_is_current" = TRUE
    
    {% if var('load_mode', 'INCREMENTAL') == 'FULL' %}
    UNION ALL
    
    -- Marquer suppressions
    SELECT
        ods."cod_cli", ods."typ_elem", ods."nom_cli", ods."ville", ods."k_post", ods."regime", ods."type_fac", ods."form_jur", ods."capital", ods."usr_crt", ods."dat_crt", ods."usr_mod", ods."dat_mod", ods."zal_1", ods."zal_2", ods."zal_3", ods."zal_4", ods."zal_5", ods."znu_1", ods."znu_2", ods."znu_3", ods."znu_4", ods."znu_5", ods."zta_1", ods."zta_2", ods."zta_3", ods."zta_4", ods."zta_5", ods."zda_1", ods."zda_2", ods."zda_3", ods."zda_4", ods."zda_5", ods."statut", ods."qui_sta", ods."com_sta", ods."dat_sta", ods."chif_bl", ods."fac_dvs", ods."fac_ttc", ods."fac_pxa", ods."fra_app", ods."liasse", ods."div_fac_1", ods."div_fac_2", ods."div_fac_3", ods."val_fac_1", ods."val_fac_2", ods."val_fac_3", ods."fam_art_1", ods."fam_art_2", ods."fam_art_3", ods."fam_art_4", ods."fam_art_5", ods."depot", ods."transpor", ods."mod_liv", ods."edt_bp", ods."edt_arc", ods."mnt_rlk_1", ods."mnt_rlk_2", ods."mnt_rlk_3", ods."mnt_rlk_4", ods."ref_cde", ods."proforma", ods."televen", ods."cal_cde", ods."cal_liv", ods."heure", ods."com_tel", ods."jou_dec", ods."cl_grp", ods."cl_livre", ods."cl_fact", ods."cl_paye", ods."tarif", ods."cl_stat", ods."region", ods."famille", ods."cat_tar", ods."commerc_1", ods."commerc_2", ods."app_aff", ods."tel_j1", ods."tel_j2", ods."tel_j3", ods."tel_j4", ods."tel_j5", ods."tel_j6", ods."tel_j7", ods."repert", ods."no_tarif", ods."borne", ods."tx_ech_d", ods."vte_spe", ods."gencod", ods."cl_plnat", ods."cl_plreg", ods."maj_ach", ods."pays", ods."internet", ods."tva_ech", ods."mot_cle", ods."zon_geo", ods."zon_lib", ods."notel", ods."rem_glo_1", ods."rem_glo_2", ods."cde_cpl", ods."fact_con", ods."coeff_pvc", ods."liv_cpl", ods."arr_bi_1", ods."arr_bi_2", ods."arr_bi_3", ods."arr_bi_4", ods."arr_bi_5", ods."arr_bs_1", ods."arr_bs_2", ods."arr_bs_3", ods."arr_bs_4", ods."arr_bs_5", ods."arr_r_1", ods."arr_r_2", ods."arr_r_3", ods."arr_r_4", ods."arr_r_5", ods."fac_poi", ods."c_factor", ods."transit", ods."grp_ps", ods."zlo_1", ods."zlo_2", ods."zlo_3", ods."zlo_4", ods."zlo_5", ods."k_post2", ods."blok_enc", ods."effectif", ods."ca_client", ods."ca_pot", ods."login", ods."log_mdp", ods."gencod_det", ods."ord_affec", ods."priorite", ods."jalon_tot", ods."int_rrr_1", ods."int_rrr_2", ods."int_rrr_3", ods."int_rrr_4", ods."int_rrr_5", ods."int_rrr_6", ods."ben_rrr_1", ods."ben_rrr_2", ods."ben_rrr_3", ods."ben_rrr_4", ods."ben_rrr_5", ods."ben_rrr_6", ods."fvte", ods."cod_adh", ods."cadencier", ods."interv_1", ods."interv_2", ods."interv_3", ods."interv_4", ods."interv_5", ods."mt_mini", ods."plus_bpbl", ods."bl_regr", ods."tx_com", ods."inc_deb", ods."trs_deb", ods."dev_cap", ods."mt_franco", ods."fr_cde", ods."s2_famille", ods."s3_famille", ods."s4_famille", ods."promo", ods."rem_lig", ods."cod_tlv", ods."frq_a_nul", ods."frq_v_nul", ods."mod_a", ods."mod_v", ods."cal_v", ods."hr_v", ods."vis_j1", ods."vis_j2", ods."vis_j3", ods."vis_j4", ods."vis_j5", ods."vis_j6", ods."vis_j7", ods."prep_isol", ods."commercial", ods."com_v", ods."f_verifcde_1", ods."f_verifcde_2", ods."f_verifcde_3", ods."enc_imp", ods."plan_rrr", ods."szon_geo", ods."origine", ods."s2_cat", ods."s3_cat", ods."s4_cat", ods."qui_stn_1", ods."qui_stn_2", ods."qui_stn_3", ods."qui_stn_4", ods."qui_stn_5", ods."qui_stn_6", ods."qui_stn_7", ods."qui_stn_8", ods."qui_stn_9", ods."qui_ver", ods."rq_pan", ods."typ_con", ods."typ_pxa", ods."c_retr", ods."cl_prc", ods."ffc_prc", ods."rfa_soc", ods."interloc_1", ods."interloc_2", ods."adresse_1", ods."adresse_2", ods."adresse_3", ods."zone_exp", ods."cpt_web", ods."f_prc", ods."demat_fac", ods."non_bloc", ods."phone", ods."pref_ref", ods."dgc", ods."no_tar_loc", ods."meth_rf", ods."siret", ods."ty_mini", ods."bl_mini", ods."prc_rep", ods."retour_date", ods."cat_cum", ods."tar_fil_1", ods."tar_fil_2", ods."tar_fil_3", ods."tar_fil_4", ods."tar_fil_5", ods."tar_fil_6", ods."tar_cum_1", ods."tar_cum_2", ods."tar_cum_3", ods."tar_cum_4", ods."tar_cum_5", ods."tar_cum_6", ods."typ_cad", ods."dep_unq", ods."lias_bl", ods."canal", ods."notation", ods."no_ctrl", ods."typ_floc", ods."contrat_date", ods."date_px", ods."or_ces", ods."typ_cam", ods."marge_p", ods."typ_veh", ods."prix_franco", ods."rlk_mnt", ods."no_port", ods."maqf_etq", ods."typ_sup", ods."groupe", ods."dep_uniq", ods."sscc_sor", ods."refrec", ods."refrec2", ods."vc_subst_g", ods."uni_ref", ods."bloq_prep", ods."cl_plcon", ods."cl_pere", ods."rayon", ods."tar_fi2_1", ods."tar_fi2_2", ods."tar_fi2_3", ods."tar_fi2_4", ods."tar_cu2_1", ods."tar_cu2_2", ods."tar_cu2_3", ods."tar_cu2_4", ods."cal_enc", ods."prc_aco", ods."mod_aco", ods."TabPart_client", ods."flag_repli", ods."jamais_peser", ods."hr_app_1", ods."hr_app_2", ods."hr_app_3", ods."hr_app_4", ods."hr_app_5", ods."hr_app_6", ods."hr_app_7", ods."multitrp", ods."adresse5", ods."or_spereg", ods."fac_liv", ods."conso_exc",
        ods."_etl_hashdiff",
        ods."_etl_valid_from",
        CURRENT_TIMESTAMP AS "_etl_valid_to",
        FALSE AS "_etl_is_current",
        TRUE AS "_etl_is_deleted",
        ods."last_seen_date"
    FROM {{ this }} ods
    INNER JOIN deleted_records dr USING ("cod_cli")
    WHERE ods."_etl_is_current" = TRUE
    {% endif %}
    
    UNION ALL
    
    -- Historique ancien
    SELECT * FROM {{ this }}
    WHERE "_etl_is_current" = FALSE
)

{% else %}

-- Initial load
final AS (
    SELECT
        "cod_cli", "typ_elem", "nom_cli", "ville", "k_post", "regime", "type_fac", "form_jur", "capital", "usr_crt", "dat_crt", "usr_mod", "dat_mod", "zal_1", "zal_2", "zal_3", "zal_4", "zal_5", "znu_1", "znu_2", "znu_3", "znu_4", "znu_5", "zta_1", "zta_2", "zta_3", "zta_4", "zta_5", "zda_1", "zda_2", "zda_3", "zda_4", "zda_5", "statut", "qui_sta", "com_sta", "dat_sta", "chif_bl", "fac_dvs", "fac_ttc", "fac_pxa", "fra_app", "liasse", "div_fac_1", "div_fac_2", "div_fac_3", "val_fac_1", "val_fac_2", "val_fac_3", "fam_art_1", "fam_art_2", "fam_art_3", "fam_art_4", "fam_art_5", "depot", "transpor", "mod_liv", "edt_bp", "edt_arc", "mnt_rlk_1", "mnt_rlk_2", "mnt_rlk_3", "mnt_rlk_4", "ref_cde", "proforma", "televen", "cal_cde", "cal_liv", "heure", "com_tel", "jou_dec", "cl_grp", "cl_livre", "cl_fact", "cl_paye", "tarif", "cl_stat", "region", "famille", "cat_tar", "commerc_1", "commerc_2", "app_aff", "tel_j1", "tel_j2", "tel_j3", "tel_j4", "tel_j5", "tel_j6", "tel_j7", "repert", "no_tarif", "borne", "tx_ech_d", "vte_spe", "gencod", "cl_plnat", "cl_plreg", "maj_ach", "pays", "internet", "tva_ech", "mot_cle", "zon_geo", "zon_lib", "notel", "rem_glo_1", "rem_glo_2", "cde_cpl", "fact_con", "coeff_pvc", "liv_cpl", "arr_bi_1", "arr_bi_2", "arr_bi_3", "arr_bi_4", "arr_bi_5", "arr_bs_1", "arr_bs_2", "arr_bs_3", "arr_bs_4", "arr_bs_5", "arr_r_1", "arr_r_2", "arr_r_3", "arr_r_4", "arr_r_5", "fac_poi", "c_factor", "transit", "grp_ps", "zlo_1", "zlo_2", "zlo_3", "zlo_4", "zlo_5", "k_post2", "blok_enc", "effectif", "ca_client", "ca_pot", "login", "log_mdp", "gencod_det", "ord_affec", "priorite", "jalon_tot", "int_rrr_1", "int_rrr_2", "int_rrr_3", "int_rrr_4", "int_rrr_5", "int_rrr_6", "ben_rrr_1", "ben_rrr_2", "ben_rrr_3", "ben_rrr_4", "ben_rrr_5", "ben_rrr_6", "fvte", "cod_adh", "cadencier", "interv_1", "interv_2", "interv_3", "interv_4", "interv_5", "mt_mini", "plus_bpbl", "bl_regr", "tx_com", "inc_deb", "trs_deb", "dev_cap", "mt_franco", "fr_cde", "s2_famille", "s3_famille", "s4_famille", "promo", "rem_lig", "cod_tlv", "frq_a_nul", "frq_v_nul", "mod_a", "mod_v", "cal_v", "hr_v", "vis_j1", "vis_j2", "vis_j3", "vis_j4", "vis_j5", "vis_j6", "vis_j7", "prep_isol", "commercial", "com_v", "f_verifcde_1", "f_verifcde_2", "f_verifcde_3", "enc_imp", "plan_rrr", "szon_geo", "origine", "s2_cat", "s3_cat", "s4_cat", "qui_stn_1", "qui_stn_2", "qui_stn_3", "qui_stn_4", "qui_stn_5", "qui_stn_6", "qui_stn_7", "qui_stn_8", "qui_stn_9", "qui_ver", "rq_pan", "typ_con", "typ_pxa", "c_retr", "cl_prc", "ffc_prc", "rfa_soc", "interloc_1", "interloc_2", "adresse_1", "adresse_2", "adresse_3", "zone_exp", "cpt_web", "f_prc", "demat_fac", "non_bloc", "phone", "pref_ref", "dgc", "no_tar_loc", "meth_rf", "siret", "ty_mini", "bl_mini", "prc_rep", "retour_date", "cat_cum", "tar_fil_1", "tar_fil_2", "tar_fil_3", "tar_fil_4", "tar_fil_5", "tar_fil_6", "tar_cum_1", "tar_cum_2", "tar_cum_3", "tar_cum_4", "tar_cum_5", "tar_cum_6", "typ_cad", "dep_unq", "lias_bl", "canal", "notation", "no_ctrl", "typ_floc", "contrat_date", "date_px", "or_ces", "typ_cam", "marge_p", "typ_veh", "prix_franco", "rlk_mnt", "no_port", "maqf_etq", "typ_sup", "groupe", "dep_uniq", "sscc_sor", "refrec", "refrec2", "vc_subst_g", "uni_ref", "bloq_prep", "cl_plcon", "cl_pere", "rayon", "tar_fi2_1", "tar_fi2_2", "tar_fi2_3", "tar_fi2_4", "tar_cu2_1", "tar_cu2_2", "tar_cu2_3", "tar_cu2_4", "cal_enc", "prc_aco", "mod_aco", "TabPart_client", "flag_repli", "jamais_peser", "hr_app_1", "hr_app_2", "hr_app_3", "hr_app_4", "hr_app_5", "hr_app_6", "hr_app_7", "multitrp", "adresse5", "or_spereg", "fac_liv", "conso_exc",
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
