{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'client']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_client') }}
),

cleaned AS (
    SELECT
        "COD_CLI"                 AS code_client,
        "NOM_CLI"                 AS nom_client,
        UPPER(TRIM("NOM_CLI"))    AS nom_client_normalized,
        "INTERLOC"                AS interlocuteur,
        "NOTEL"                   AS telephone,
        "PHONE"                   AS phone,
        "VILLE"                   AS ville,
        UPPER(TRIM("VILLE"))      AS ville_normalized,
        "K_POST2"                 AS code_postal,
        "PAYS"                    AS pays,
        "REGION"                  AS region,
        "FAMILLE"                 AS famille,
        
        -- S2_FAMILLE = "AAA;;;;" - Split multi-valeurs
        {{ split_multivalue_text('"S2_FAMILLE"', 1) }} as sous_famille,
        
        "TYPE_FAC"                AS type_facture,
        "STATUT"                  AS statut,
        "REGIME"                  AS regime,
        "CAT_TAR"                 AS categorie_tarif,
        "COMMERCIAL"              AS code_commercial,
        "COMMERCIAL"              AS nom_commercial,
        "NO_TARIF"                AS numero_tarif,
        "COEFF_PVC"   ::numeric   AS coefficient_pvc,
        "CAPITAL"     ::numeric   AS capital,
        "CL_PAYE"                 AS classe_paiement,
        "CL_GRP"                  AS classe_groupe,
        "CHIF_BL"                 AS chiffrage_bl,
        "MT_MINI"     ::numeric   AS montant_minimum,
        CASE WHEN "BL_MINI" = true THEN 1 ELSE 0 END::numeric AS bl_minimum,
        
        -- MNT_RLK = "1;1;0;0" - Prendre première valeur
        {{ split_multivalue_numeric('"MNT_RLK"', 1) }} as montant_reliquat,
        
        "MOD_LIV"                 AS mode_livraison,
        "TRANSPOR"                AS transporteur,
        
        -- TRS_DEB = "0;0;0;0;0;0" - Prendre première valeur
        {{ split_multivalue_numeric('"TRS_DEB"', 1) }} as frais_transport,
        
        -- DIV_FAC = "0;0;0" - Prendre première valeur
        {{ split_multivalue_numeric('"DIV_FAC"', 1) }} as division_facture,
        
        "FAC_DVS"                 AS facture_diverse,
        "FAC_LIV"                 AS facture_livraison,
        "FAC_PXA"                 AS facture_prix_achat,
        "FACT_CON"                AS facture_consolidee,
        "DEMAT_FAC"               AS dematerialisation_facture,
        "PROFORMA"                AS proforma,
        "PROMO"                   AS promotion,
        "DEPOT"                   AS depot,
        "SIRET"                   AS siret,
        "GENCOD"                  AS code_ean,
        "LIASSE"                  AS liasse,
        "INTERNET"                AS internet,
        "REF_CDE"                 AS reference_commande,
        "MOT_CLE"                 AS mot_cle,
        "ZON_GEO"                 AS zone_geographique,
        
        -- ZAL = "0;0;0;0;0" - Prendre première valeur
        {{ split_multivalue_numeric('"ZAL"', 1) }} as zone_al,
        
        -- ZLO = "1;0;0;0;0" - Prendre première valeur
        {{ split_multivalue_integer('"ZLO"', 1) }} as zone_lo,
        
        -- ZNU = ";;;;" - Prendre première valeur (si existe)
        {{ split_multivalue_text('"ZNU"', 1) }} as zone_nu,
        
        "ZTA"                     AS zone_ta,
        "EDT_ARC"                 AS edition_archive,
        "EDT_BP"                  AS edition_bp,
        "MAJ_ACH"                 AS maj_achat,
        "VTE_SPE"                 AS vente_speciale,
        "TYP_CON"                 AS type_contrat,
        "TYP_ELEM"                AS type_element,
        "TAR_FIL"                 AS tarif_filiale,
        "DAT_CRT"                 AS date_creation,
        "DAT_MOD"                 AS date_modification,
        "DAT_STA"                 AS date_statut,
        "USR_CRT"                 AS utilisateur_creation,
        "USR_MOD"                 AS utilisateur_modification,
        "QUI_STA"                 AS qui_statut,
        "QUI_VER"                 AS qui_verification,
        
        -- ETL metadata
        "hashdiff"                AS hashdiff_source,
        "ts_source"  ::timestamp  AS timestamp_source,
        "load_ts"    ::timestamp  AS load_timestamp_phase1,
        "_loaded_at" ::timestamp  AS loaded_at,
        "_source_file"            AS source_file,
        "_sftp_log_id"            AS sftp_log_id,
        CURRENT_TIMESTAMP         AS processed_at

    FROM source
    WHERE "COD_CLI" IS NOT NULL
)

SELECT * FROM cleaned