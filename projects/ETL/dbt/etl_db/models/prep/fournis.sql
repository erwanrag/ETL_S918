{{ config(
    materialized='incremental',
    unique_key='cod_fou',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'fournis') }} s WHERE s.cod_fou = t.cod_fou){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS fournis_pkey ON {{ this }} USING btree (cod_fou)",
        "CREATE INDEX IF NOT EXISTS idx_fournis_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : fournis
============================================================================
Generated : 2025-12-21 05:00:09
Source    : ods.fournis
Rows ODS  : 8,431
Cols ODS  : 216
Cols PREP : 91 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "typ_elem" AS typ_elem,
    "cod_fou" AS cod_fou,
    "nom_fou" AS nom_fou,
    "ville" AS ville,
    "mt_franco" AS mt_franco,
    "mt_mini" AS mt_mini,
    "regime" AS regime,
    "cde_valo" AS cde_valo,
    "verif_ach" AS verif_ach,
    "type_fac" AS type_fac,
    "frais_app" AS frais_app,
    "coef_ach_1" AS coef_ach_1,
    "coef_vte_1" AS coef_vte_1,
    "coef_vte_2" AS coef_vte_2,
    "coef_vte_3" AS coef_vte_3,
    "coef_vte_4" AS coef_vte_4,
    "del_liv_1" AS del_liv_1,
    "h_lim_cde_1" AS h_lim_cde_1,
    "del_appro_1" AS del_appro_1,
    "del_appro_2" AS del_appro_2,
    "relik_1" AS relik_1,
    "relik_2" AS relik_2,
    "relik_3" AS relik_3,
    "relik_4" AS relik_4,
    "edt_cde" AS edt_cde,
    "edt_rec" AS edt_rec,
    "siret" AS siret,
    "ape" AS ape,
    "form_jur" AS form_jur,
    "capital" AS capital,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_1" AS zal_1,
    "zal_2" AS zal_2,
    "znu_1" AS znu_1,
    "znu_2" AS znu_2,
    "znu_3" AS znu_3,
    "znu_4" AS znu_4,
    "zta_2" AS zta_2,
    "zta_4" AS zta_4,
    "statut" AS statut,
    "reg_dvs" AS reg_dvs,
    "franco" AS franco,
    "mod_liv" AS mod_liv,
    "txt_com" AS txt_com,
    "txt_rec" AS txt_rec,
    "gencod" AS gencod,
    "pays" AS pays,
    "internet" AS internet,
    "mot_cle" AS mot_cle,
    "transpor" AS transpor,
    "zon_geo" AS zon_geo,
    "notel" AS notel,
    "zlo_1" AS zlo_1,
    "zlo_3" AS zlo_3,
    "famille" AS famille,
    "arc" AS arc,
    "k_post2" AS k_post2,
    "cod_for" AS cod_for,
    "acheteur" AS acheteur,
    "jalon" AS jalon,
    "notation" AS notation,
    "certifie" AS certifie,
    "inc_deb" AS inc_deb,
    "trs_deb" AS trs_deb,
    "jr_app_2" AS jr_app_2,
    "jr_app_3" AS jr_app_3,
    "jr_app_4" AS jr_app_4,
    "jr_app_5" AS jr_app_5,
    "interloc_1" AS interloc_1,
    "interloc_2" AS interloc_2,
    "adresse_1" AS adresse_1,
    "adresse_2" AS adresse_2,
    "adresse_3" AS adresse_3,
    "phone" AS phone,
    "date_px" AS date_px,
    "tx_ech_d" AS tx_ech_d,
    "typ_con" AS typ_con,
    "fact_con" AS fact_con,
    "note_calc" AS note_calc,
    "rem_app_1" AS rem_app_1,
    "rem_app_2" AS rem_app_2,
    "rem_app_3" AS rem_app_3,
    "rem_app_4" AS rem_app_4,
    "ges_golda" AS ges_golda,
    "cod_fop" AS cod_fop,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'fournis') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
