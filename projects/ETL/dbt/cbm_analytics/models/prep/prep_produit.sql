{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : produit
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:16
    
    Source       : ods.produit
    Lignes       : 1,617
    Colonnes ODS : 618
    Colonnes PREP: 54
    Exclues      : 564 (91.3%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 358
      - Constantes      : 193
      - Faible valeur   : 6
    ============================================================================
*/

SELECT
    "typ_elem" AS typ_elem,
    "cod_pro" AS cod_pro,
    "nom_pro" AS nom_pro,
    "nom_pr2" AS nom_pr2,
    "refint" AS refint,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "statut" AS statut,
    "groupe" AS groupe,
    "famille" AS famille,
    "s_famille" AS s_famille,
    "fam_cpt" AS fam_cpt,
    "ordre" AS ordre,
    "px_refv" AS px_refv,
    "lib_univ" AS lib_univ,
    "mult-fou" AS mult_fou,
    "refext" AS refext,
    "px_refa" AS px_refa,
    "fam_tar" AS fam_tar,
    "marque" AS marque,
    "art_remp" AS art_remp,
    "dat_remp" AS dat_remp,
    "classe" AS classe,
    "coef_dep" AS coef_dep,
    "rem_vte" AS rem_vte,
    "pmp" AS pmp,
    "cod_nom" AS cod_nom,
    "poid_net" AS poid_net,
    "gencod-v" AS gencod_v,
    "consigne" AS consigne,
    "art_cs_u" AS art_cs_u,
    "d_pxach" AS d_pxach,
    "dat_ent" AS dat_ent,
    "ges_stk" AS ges_stk,
    "ges_dem" AS ges_dem,
    "dpx_rvt" AS dpx_rvt,
    "sf_tar" AS sf_tar,
    "cod_cli" AS cod_cli,
    "s3_famille" AS s3_famille,
    "s4_famille" AS s4_famille,
    "ach_ctr" AS ach_ctr,
    "nb_ctr" AS nb_ctr,
    "s3_tar" AS s3_tar,
    "ges_emp" AS ges_emp,
    "mult_vte" AS mult_vte,
    "nb_uv" AS nb_uv,
    "w_produit" AS w_produit,
    "phone" AS phone,
    "dat_import" AS dat_import,
    "non_sscc" AS non_sscc,
    "non_uli" AS non_uli,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'produit') }}
