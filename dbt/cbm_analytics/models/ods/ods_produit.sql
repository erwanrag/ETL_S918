{{
    config(
        materialized='table',
        schema='ods',
        unique_key='code_produit',
        tags=['ods', 'produit', 'product']
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_produit') }}
),

final AS (
    SELECT
        code_produit,
        nom_produit,
        nom_produit_normalized,
        nom_produit_2,
        libelle_conventionne,
        libelle_universel,
        famille,
        sous_famille,
        sous_famille_3,
        sous_famille_4,
        famille_tarif,
        sous_famille_tarif,
        sous_famille_3_tarif,
        groupe,
        classe,
        marque,
        sous_type,
        reference_externe,
        reference_interne,
        code_ean,
        code_nomenclature,
        prix_revient,
        prix_reference_vente,
        prix_reference_achat,
        prix_moyen_pondere,
        date_prix_achat,
        date_prix_revient,
        remise_vente,
        coefficient_depot,
        coefficient_tarif_2,
        coefficient_tarif_3,
        coefficient_tarif_4,
        gestion_stock,
        gestion_smp,
        gestion_emplacement,
        gestion_demande,
        minimum_vente,
        multiple_vente,
        multiple_fournisseur,
        unite_vente,
        nombre_unite_vente,
        nombre_colis,
        intitule_conditionnement,
        article_csu,
        longueur,
        largeur,
        hauteur,
        volume,
        poids_net,
        poids_brut,
        pds_net,
        code_tva,
        famille_comptable,
        consigne,
        statut,
        calcul,
        garantie_fabricant,
        telephone,
        web_produit,
        non_sscc,
        non_uli,
        ordre,
        article_remplacement,
        date_remplacement,
        achat_controlé,
        zone_al,
        zone_da,
        zone_lo,
        zone_nu,
        zone_ta,
        type_element,
        date_creation,
        date_modification,
        date_entree,
        date_import,
        utilisateur_creation,
        utilisateur_modification,
        
        -- Metadata Phase 1
        hashdiff_source,
        timestamp_source,
        load_timestamp_phase1,
        
        -- Metadata Phase 2
        loaded_at as source_loaded_at,
        source_file,
        sftp_log_id,
        processed_at as staging_processed_at,
        CURRENT_TIMESTAMP as ods_updated_at
        
    FROM staging
)

SELECT * FROM final