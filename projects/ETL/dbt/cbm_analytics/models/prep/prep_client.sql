{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : client
    ============================================================================
    Généré automatiquement le 2025-12-05 15:19:49
    
    Source       : ods.client
    Lignes       : 43
    Colonnes ODS : 321
    Colonnes PREP: 47
    Exclues      : 274 (85.4%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 159
      - Constantes      : 99
      - Faible valeur   : 9
    ============================================================================
*/

SELECT
    "typ_elem" AS typ_elem,
    "cod_cli" AS cod_cli,
    "nom_cli" AS nom_cli,
    "ville" AS ville,
    "regime" AS regime,
    "type_fac" AS type_fac,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "com_sta" AS com_sta,
    "dat_sta" AS dat_sta,
    "chif_bl" AS chif_bl,
    "fac_dvs" AS fac_dvs,
    "liasse" AS liasse,
    "depot" AS depot,
    "mod_liv" AS mod_liv,
    "edt_bp" AS edt_bp,
    "edt_arc" AS edt_arc,
    "ref_cde" AS ref_cde,
    "proforma" AS proforma,
    "cl_grp" AS cl_grp,
    "tarif" AS tarif,
    "region" AS region,
    "famille" AS famille,
    "cat_tar" AS cat_tar,
    "no_tarif" AS no_tarif,
    "vte_spe" AS vte_spe,
    "pays" AS pays,
    "internet" AS internet,
    "mot_cle" AS mot_cle,
    "zon_geo" AS zon_geo,
    "notel" AS notel,
    "fact_con" AS fact_con,
    "k_post2" AS k_post2,
    "dev_cap" AS dev_cap,
    "s2_famille" AS s2_famille,
    "promo" AS promo,
    "commercial" AS commercial,
    "qui_ver" AS qui_ver,
    "demat_fac" AS demat_fac,
    "non_bloc" AS non_bloc,
    "phone" AS phone,
    "siret" AS siret,
    "bl_mini" AS bl_mini,
    "fac_liv" AS fac_liv,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'client') }}
