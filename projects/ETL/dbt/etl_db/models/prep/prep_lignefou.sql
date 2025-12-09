{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lignefou
    ============================================================================
    Généré automatiquement le 2025-12-05 15:33:58
    
    Source       : ods.lignefou
    Lignes       : 18,249
    Colonnes ODS : 348
    Colonnes PREP: 78
    Exclues      : 270 (77.6%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 162
      - Constantes      : 92
      - Faible valeur   : 9
    ============================================================================
*/

SELECT
    "cod_fou" AS cod_fou,
    "no_cde" AS no_cde,
    "no_ligne" AS no_ligne,
    "sous_type" AS sous_type,
    "typ_elem" AS typ_elem,
    "cod_pro" AS cod_pro,
    "nom_pro" AS nom_pro,
    "qte" AS qte,
    "px_ach" AS px_ach,
    "remise1" AS remise1,
    "rem_app" AS rem_app,
    "groupe" AS groupe,
    "famille" AS famille,
    "s_famille" AS s_famille,
    "fam_cpt" AS fam_cpt,
    "conv_px" AS conv_px,
    "refext" AS refext,
    "devise" AS devise,
    "lib_unia" AS lib_unia,
    "lib_cona" AS lib_cona,
    "fam_tar" AS fam_tar,
    "code_tva" AS code_tva,
    "px_refa" AS px_refa,
    "pmp" AS pmp,
    "depot" AS depot,
    "dat_liv" AS dat_liv,
    "forcer" AS forcer,
    "delai" AS delai,
    "refint" AS refint,
    "cod_cli" AS cod_cli,
    "longueur" AS longueur,
    "largeur" AS largeur,
    "hauteur" AS hauteur,
    "calc_uv" AS calc_uv,
    "sf_tar" AS sf_tar,
    "mt_ht" AS mt_ht,
    "cde_cli" AS cde_cli,
    "lien_nmc" AS lien_nmc,
    "edt_nmc" AS edt_nmc,
    "ord_nmc" AS ord_nmc,
    "qte_rec" AS qte_rec,
    "poids" AS poids,
    "reliquat" AS reliquat,
    "type_prix" AS type_prix,
    "ndos" AS ndos,
    "er_sr" AS er_sr,
    "es_sp" AS es_sp,
    "er_ep" AS er_ep,
    "er_sf" AS er_sf,
    "s3_tar" AS s3_tar,
    "s4_tar" AS s4_tar,
    "er_couv" AS er_couv,
    "confirme" AS confirme,
    "calcul" AS calcul,
    "nb_ua" AS nb_ua,
    "pp_ua" AS pp_ua,
    "poid_net" AS poid_net,
    "s3_famille" AS s3_famille,
    "s4_famille" AS s4_famille,
    "nom_pr2" AS nom_pr2,
    "no_unique" AS no_unique,
    "er_cdfr" AS er_cdfr,
    "poid_tot" AS poid_tot,
    "volume" AS volume,
    "num_ave" AS num_ave,
    "consigne" AS consigne,
    "dat_acc" AS dat_acc,
    "dat_livd" AS dat_livd,
    "maj_stk" AS maj_stk,
    "gen_csg" AS gen_csg,
    "qte_a_rec" AS qte_a_rec,
    "uniq_id" AS uniq_id,
    "pays_ori" AS pays_ori,
    "px_rvt" AS px_rvt,
    "marque" AS marque,
    "gratuit" AS gratuit,
    "cod_nom" AS cod_nom,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lignefou') }}
