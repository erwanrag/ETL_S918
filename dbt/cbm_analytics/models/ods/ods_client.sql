{{
    config(
        materialized='table',
        schema='ods',
        unique_key='code_client',
        tags=['ods', 'client']
    )
}}

/*
=================================================================
Modèle : ods_client
Description : Table ODS finale des clients (sélection depuis staging)
Source : staging.stg_client
Stratégie : UPSERT sur code_client
=================================================================
*/

WITH staging AS (
    SELECT * FROM {{ ref('stg_client') }}
),

final AS (
    SELECT
        -- Toutes les colonnes depuis staging
        code_client,
        nom_client,
        nom_client_normalized,
        interlocuteur,
        telephone,
        phone,
        ville,
        ville_normalized,
        code_postal,
        pays,
        region,
        famille,
        sous_famille,
        type_facture,
        statut,
        regime,
        categorie_tarif,
        code_commercial,
        nom_commercial,
        numero_tarif,
        coefficient_pvc,
        capital,
        classe_paiement,
        classe_groupe,
        chiffrage_bl,
        montant_minimum,
        bl_minimum,
        montant_reliquat,
        mode_livraison,
        transporteur,
        frais_transport,
        division_facture,
        facture_diverse,
        facture_livraison,
        facture_prix_achat,
        facture_consolidee,
        dematerialisation_facture,
        proforma,
        promotion,
        depot,
        siret,
        code_ean,
        liasse,
        internet,
        reference_commande,
        mot_cle,
        zone_geographique,
        zone_al,
        zone_lo,
        zone_nu,
        zone_ta,
        edition_archive,
        edition_bp,
        maj_achat,
        vente_speciale,
        type_contrat,
        type_element,
        tarif_filiale,
        date_creation,
        date_modification,
        date_statut,
        utilisateur_creation,
        utilisateur_modification,
        qui_statut,
        qui_verification,
        
        -- ============================================
        -- Metadata ETL (depuis Phase 1)
        -- ============================================
        hashdiff_source,
        timestamp_source,
        load_timestamp_phase1,
        
        -- ============================================
        -- Metadata ETL (Phase 2 - PostgreSQL)
        -- ============================================
        loaded_at as source_loaded_at,
        source_file,
        sftp_log_id,
        processed_at as staging_processed_at,
        CURRENT_TIMESTAMP as ods_updated_at
        
    FROM staging
)

SELECT * FROM final