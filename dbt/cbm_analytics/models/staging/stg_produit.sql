{{ 
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'produit', 'product']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_produit') }}
),

cleaned AS (
    SELECT
        -- ============================================
        -- Clé primaire
        -- ============================================
        "COD_PRO" as code_produit,
        
        -- ============================================
        -- Informations générales
        -- ============================================
        "NOM_PRO" as nom_produit,
        UPPER(TRIM("NOM_PRO")) as nom_produit_normalized,
        "NOM_PR2" as nom_produit_2,
        "LIB_CONV" as libelle_conventionne,
        "LIB_UNIV" as libelle_universel,
        
        -- ============================================
        -- Classification
        -- ============================================
        "FAMILLE" as famille,
        "S_FAMILLE" as sous_famille,
        "S3_FAMILLE" as sous_famille_3,
        "S4_FAMILLE" as sous_famille_4,
        "FAM_TAR" as famille_tarif,
        "SF_TAR" as sous_famille_tarif,
        "S3_TAR" as sous_famille_3_tarif,
        "GROUPE" as groupe,
        "CLASSE" as classe,
        "MARQUE" as marque,
        "SOUS_TYPE" as sous_type,
        
        -- ============================================
        -- Codes et références
        -- ============================================
        "REFEXT" as reference_externe,
        "REFINT" as reference_interne,
        "GENCOD_V" as code_ean,
        "COD_NOM" as code_nomenclature,
        
        -- ============================================
        -- Pricing et tarifs (DÉJÀ NUMERIC - pas de split)
        -- ============================================
        "PX_RVT"::numeric as prix_revient,
        "PX_REFV"::numeric as prix_reference_vente,
        "PX_REFA"::numeric as prix_reference_achat,
        "PMP"::numeric as prix_moyen_pondere,
        "D_PXACH" as date_prix_achat,
        "DPX_RVT" as date_prix_revient,
        "REM_VTE" as remise_vente,
        
        -- ============================================
        -- Coefficients (DÉJÀ NUMERIC - pas de split)
        -- ============================================
        "COEF_DEP"::numeric as coefficient_depot,
        "COEF_T2"::numeric as coefficient_tarif_2,
        "COEF_T3"::numeric as coefficient_tarif_3,
        "COEF_T4"::numeric as coefficient_tarif_4,
        
        -- ============================================
        -- Stock et gestion
        -- ============================================
        "GES_STK" as gestion_stock,
        "GES_SMP" as gestion_smp,
        "GES_EMP" as gestion_emplacement,
        "GES_DEM" as gestion_demande,
        "MINI_VTE"::numeric as minimum_vente,
        "MULT_VTE"::numeric as multiple_vente,
        "MULT_FOU" as multiple_fournisseur,
        
        -- ============================================
        -- Unités et conditionnements
        -- ============================================
        "UNI_VTE" as unite_vente,
        "NB_UV"::integer as nombre_unite_vente,
        "NB_CTR"::integer as nombre_colis,
        "INT_CONDT"::integer as intitule_conditionnement,
        "ART_CS_U" as article_csu,
        
        -- ============================================
        -- Dimensions et poids (TEXT - AVEC split)
        -- ============================================
        {{ split_multivalue_numeric('"LONGUEUR"', 1) }} as longueur,
        {{ split_multivalue_numeric('"LARGEUR"', 1) }} as largeur,
        {{ split_multivalue_numeric('"HAUTEUR"', 1) }} as hauteur,
        {{ split_multivalue_numeric('"VOLUME"', 1) }} as volume,
        {{ split_multivalue_numeric('"POID_NET"', 1) }} as poids_net,
        {{ split_multivalue_numeric('"POID_BRUT"', 1) }} as poids_brut,
        {{ split_multivalue_numeric('"PDS_NET"', 1) }} as pds_net,
        
        -- ============================================
        -- Fiscalité
        -- ============================================
        "CODE_TVA" as code_tva,
        "FAM_CPT" as famille_comptable,
        "CONSIGNE" as consigne,
        
        -- ============================================
        -- Gestion particulière
        -- ============================================
        "STATUT"::text as statut,
        "CALCUL" as calcul,
        "GAR_FAB" as garantie_fabricant,
        "PHONE" as telephone,
        "W_PRODUIT" as web_produit,
        "NON_SSCC" as non_sscc,
        "NON_ULI" as non_uli,
        "ORDRE"::integer as ordre,
        
        -- ============================================
        -- Articles de remplacement
        -- ============================================
        "ART_REMP" as article_remplacement,
        "DAT_REMP" as date_remplacement,
        
        -- ============================================
        -- Achats
        -- ============================================
        "ACH_CTR" as achat_controlé,
        
        -- ============================================
        -- Zones
        -- ============================================
        "ZAL" as zone_al,
        "ZDA" as zone_da,
        "ZLO" as zone_lo,
        "ZNU" as zone_nu,
        "ZTA" as zone_ta,
        
        -- ============================================
        -- Flags et type
        -- ============================================
        "TYP_ELEM" as type_element,
        
        -- ============================================
        -- Dates
        -- ============================================
        "DAT_CRT" as date_creation,
        "DAT_MOD" as date_modification,
        "DAT_ENT" as date_entree,
        "DAT_IMPORT" as date_import,
        
        -- ============================================
        -- Utilisateurs
        -- ============================================
        "USR_CRT" as utilisateur_creation,
        "USR_MOD" as utilisateur_modification,
        
        -- ============================================
        -- Metadata ETL (Phase 1)
        -- ============================================
        "hashdiff" as hashdiff_source,
        "ts_source"::timestamp as timestamp_source,
        "load_ts"::timestamp as load_timestamp_phase1,
        
        -- ============================================
        -- Metadata ETL (Phase 2)
        -- ============================================
        "_loaded_at"::timestamp as loaded_at,
        "_source_file" as source_file,
        "_sftp_log_id" as sftp_log_id,
        CURRENT_TIMESTAMP as processed_at
        
    FROM source
    WHERE "COD_PRO" IS NOT NULL
)

SELECT * FROM cleaned