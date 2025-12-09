{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : entetfou
    ============================================================================
    Généré automatiquement le 2025-12-05 15:23:44
    
    Source       : ods.entetfou
    Lignes       : 6,506
    Colonnes ODS : 270
    Colonnes PREP: 77
    Exclues      : 193 (71.5%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 143
      - Constantes      : 37
      - Faible valeur   : 6
    ============================================================================
*/

SELECT
    "cod_fou" AS cod_fou,
    "no_cde" AS no_cde,
    "achev" AS achev,
    "dat_cde" AS dat_cde,
    "dat_px" AS dat_px,
    "qui" AS qui,
    "ref_cde" AS ref_cde,
    "dat_liv" AS dat_liv,
    "depot" AS depot,
    "civ_fac" AS civ_fac,
    "villef" AS villef,
    "paysf" AS paysf,
    "num_tel" AS num_tel,
    "num_fax" AS num_fax,
    "villel" AS villel,
    "paysl" AS paysl,
    "devise" AS devise,
    "code_reg" AS code_reg,
    "code_ech" AS code_ech,
    "langue" AS langue,
    "regime" AS regime,
    "mod_liv" AS mod_liv,
    "mt_cde" AS mt_cde,
    "dat_edi" AS dat_edi,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "txt_com" AS txt_com,
    "txt_rec" AS txt_rec,
    "cde_valo" AS cde_valo,
    "verif_ach" AS verif_ach,
    "type_fac" AS type_fac,
    "blocage" AS blocage,
    "sta_cde" AS sta_cde,
    "edt_cde" AS edt_cde,
    "edt_rec" AS edt_rec,
    "mnt_rlk" AS mnt_rlk,
    "zon_geo" AS zon_geo,
    "transpor" AS transpor,
    "famille" AS famille,
    "arc" AS arc,
    "mt_ht_dev" AS mt_ht_dev,
    "k_post2f" AS k_post2f,
    "k_post2l" AS k_post2l,
    "ref_arc" AS ref_arc,
    "sta_pce" AS sta_pce,
    "regr" AS regr,
    "inc_deb" AS inc_deb,
    "semaine" AS semaine,
    "adrfac4" AS adrfac4,
    "adrliv4" AS adrliv4,
    "ndos" AS ndos,
    "st_interne" AS st_interne,
    "dat_livd" AS dat_livd,
    "no_info" AS no_info,
    "dat_val" AS dat_val,
    "qui_val" AS qui_val,
    "poids" AS poids,
    "colis" AS colis,
    "palette" AS palette,
    "typ_con" AS typ_con,
    "transit" AS transit,
    "volume" AS volume,
    "arc_fax" AS arc_fax,
    "arc_mail" AS arc_mail,
    "mt_fact" AS mt_fact,
    "dat_acc" AS dat_acc,
    "com_golda" AS com_golda,
    "uniq_id" AS uniq_id,
    "metre_lin" AS metre_lin,
    "creerparcli" AS creerparcli,
    "informat" AS informat,
    "acq_intra" AS acq_intra,
    "pal_sol" AS pal_sol,
    "annonce" AS annonce,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'entetfou') }}
