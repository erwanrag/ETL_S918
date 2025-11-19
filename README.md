# 🏭 ETL CBM Analytics - Prefect + dbt + PostgreSQL

Pipeline ETL industriel pour migration Progress/Proginov → PostgreSQL avec transformation dbt.

## 📋 Architecture

```
CBM_DATA01 (Progress)
    ↓ SFTP Export
E:\SFTP_Mirroring (S918)
    ↓ Prefect Ingestion
PostgreSQL RAW
    ↓ dbt Transformations
PostgreSQL STAGING → ODS → MARTS
```

## 🗂️ Structure Projet

```
E:\Prefect\projects\ETL\
├── dbt/
│   └── cbm_analytics/          # Projet dbt (models, macros, tests)
├── flows/
│   ├── config/
│   │   └── pg_config.py        # Config PostgreSQL (NON VERSIONNÉ)
│   ├── ingestion/
│   │   ├── db_metadata_import.py   # Import metadata Progress
│   │   └── sftp_to_raw.py          # Ingestion SFTP → RAW
│   └── transformations/
│       └── dbt_runner.py           # Orchestration dbt
├── logs/                       # Logs Prefect
└── scripts/                    # Utilitaires PowerShell
```

## 🚀 Setup Initial

### 1. Configuration PostgreSQL

```bash
cd E:\Prefect\projects\ETL\flows\config
cp pg_config_template.py pg_config.py
# Éditer pg_config.py avec vos credentials
```

### 2. Installation dépendances Python

```bash
pip install prefect psycopg2-binary pandas pyarrow sqlalchemy dbt-postgres
```

### 3. Configuration dbt

```bash
cd E:\Prefect\projects\ETL\dbt\cbm_analytics
dbt debug  # Vérifier connexion PostgreSQL
```

## 🔄 Exécution Flows

### Ingestion SFTP → RAW
```bash
python flows/ingestion/sftp_to_raw.py
```

### Transformation dbt (RAW → ODS)
```bash
python flows/transformations/dbt_runner.py
```

### Pipeline Complet
```python
from flows.transformations.dbt_runner import full_etl_pipeline
full_etl_pipeline()
```

## 📊 Performance Target

- **Source**: 700 lignes/sec (SQL Server legacy)
- **Cible**: 50,000 - 100,000 lignes/sec (PostgreSQL COPY)

## 🔐 Sécurité

⚠️ **JAMAIS commiter `pg_config.py`** (contient credentials)
- Utiliser variables d'environnement en production
- Template disponible: `pg_config_template.py`

## 📈 Monitoring

- Logs Prefect: `E:\Prefect\projects\ETL\logs\`
- Tables monitoring: `etl_logs.*`, `sftp_monitoring.*`
- dbt docs: `dbt docs serve` (dans dbt/cbm_analytics)

## 🏗️ Schémas PostgreSQL

- `raw`: Données brutes SFTP
- `staging`: Transformations intermédiaires
- `ods`: Operational Data Store (business logic)
- `marts`: Data marts métier
- `etl_logs`: Logs exécution ETL
- `sftp_monitoring`: Monitoring fichiers SFTP

## 🔧 Maintenance

### Nettoyage archives SFTP
```powershell
# Archives > 90 jours dans E:\SFTP_Mirroring\Processed\
```

### Mise à jour dbt models
```bash
cd dbt/cbm_analytics
dbt run --models staging.*
dbt test
```

## 📞 Support

- **Équipe**: CBM Analytics
- **Environnement**: Windows Server S918_ETL
- **PostgreSQL**: Version 17 avec pgAgent