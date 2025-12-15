{{ config(
    materialized='incremental',
    unique_key='cod_cli',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS client_pkey ON {{ this }} USING btree (cod_cli)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_cli NOT IN (SELECT cod_cli FROM {{ source('ods', 'client') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : client
    ============================================================================
    Généré automatiquement le 2025-12-12 16:39:44
    
    Source       : ods.client
    Lignes       : 15,409
    Colonnes ODS : 316
    Colonnes PREP: 122  (+ _prep_loaded_at)
    Exclues      : 195 (61.7%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_cli
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 50
      - Constantes      : 140
      - Faible valeur   : 4
    ============================================================================
*/

SELECT
    "typ_elem" AS typ_elem,
    "cod_cli" AS cod_cli,
    "nom_cli" AS nom_cli,
    "ville" AS ville,
    "regime" AS regime,
    "type_fac" AS type_fac,
    "form_jur" AS form_jur,
    "capital" AS capital,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_1" AS zal_1,
    "znu_1" AS znu_1,
    "znu_2" AS znu_2,
    "znu_3" AS znu_3,
    "zta_1" AS zta_1,
    "zta_4" AS zta_4,
    "statut" AS statut,
    "qui_sta" AS qui_sta,
    "com_sta" AS com_sta,
    "dat_sta" AS dat_sta,
    "chif_bl" AS chif_bl,
    "fac_dvs" AS fac_dvs,
    "fac_pxa" AS fac_pxa,
    "liasse" AS liasse,
    "div_fac_1" AS div_fac_1,
    "div_fac_2" AS div_fac_2,
    "val_fac_1" AS val_fac_1,
    "val_fac_2" AS val_fac_2,
    "depot" AS depot,
    "transpor" AS transpor,
    "mod_liv" AS mod_liv,
    "edt_bp" AS edt_bp,
    "edt_arc" AS edt_arc,
    "mnt_rlk_1" AS mnt_rlk_1,
    "mnt_rlk_2" AS mnt_rlk_2,
    "mnt_rlk_3" AS mnt_rlk_3,
    "mnt_rlk_4" AS mnt_rlk_4,
    "ref_cde" AS ref_cde,
    "proforma" AS proforma,
    "cl_grp" AS cl_grp,
    "cl_fact" AS cl_fact,
    "cl_paye" AS cl_paye,
    "tarif" AS tarif,
    "region" AS region,
    "famille" AS famille,
    "cat_tar" AS cat_tar,
    "commerc_1" AS commerc_1,
    "commerc_2" AS commerc_2,
    "no_tarif" AS no_tarif,
    "borne" AS borne,
    "tx_ech_d" AS tx_ech_d,
    "vte_spe" AS vte_spe,
    "gencod" AS gencod,
    "maj_ach" AS maj_ach,
    "pays" AS pays,
    "internet" AS internet,
    "mot_cle" AS mot_cle,
    "zon_geo" AS zon_geo,
    "notel" AS notel,
    "rem_glo_1" AS rem_glo_1,
    "cde_cpl" AS cde_cpl,
    "fact_con" AS fact_con,
    "coeff_pvc" AS coeff_pvc,
    "liv_cpl" AS liv_cpl,
    "zlo_2" AS zlo_2,
    "zlo_3" AS zlo_3,
    "k_post2" AS k_post2,
    "effectif" AS effectif,
    "fvte" AS fvte,
    "cod_adh" AS cod_adh,
    "mt_mini" AS mt_mini,
    "inc_deb" AS inc_deb,
    "trs_deb" AS trs_deb,
    "dev_cap" AS dev_cap,
    "mt_franco" AS mt_franco,
    "s2_famille" AS s2_famille,
    "promo" AS promo,
    "cod_tlv" AS cod_tlv,
    "frq_a_nul" AS frq_a_nul,
    "frq_v_nul" AS frq_v_nul,
    "vis_j1" AS vis_j1,
    "commercial" AS commercial,
    "qui_stn_1" AS qui_stn_1,
    "qui_stn_2" AS qui_stn_2,
    "qui_stn_3" AS qui_stn_3,
    "qui_stn_4" AS qui_stn_4,
    "qui_stn_5" AS qui_stn_5,
    "qui_stn_6" AS qui_stn_6,
    "qui_stn_7" AS qui_stn_7,
    "qui_stn_8" AS qui_stn_8,
    "qui_stn_9" AS qui_stn_9,
    "qui_ver" AS qui_ver,
    "typ_con" AS typ_con,
    "c_retr" AS c_retr,
    "interloc_1" AS interloc_1,
    "interloc_2" AS interloc_2,
    "adresse_1" AS adresse_1,
    "adresse_2" AS adresse_2,
    "adresse_3" AS adresse_3,
    "demat_fac" AS demat_fac,
    "non_bloc" AS non_bloc,
    "phone" AS phone,
    "no_tar_loc" AS no_tar_loc,
    "siret" AS siret,
    "ty_mini" AS ty_mini,
    "bl_mini" AS bl_mini,
    "cat_cum" AS cat_cum,
    "tar_fil_1" AS tar_fil_1,
    "tar_cum_1" AS tar_cum_1,
    "lias_bl" AS lias_bl,
    "no_ctrl" AS no_ctrl,
    "date_px" AS date_px,
    "or_ces" AS or_ces,
    "typ_veh" AS typ_veh,
    "prix_franco" AS prix_franco,
    "no_port" AS no_port,
    "fac_liv" AS fac_liv,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'client') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
