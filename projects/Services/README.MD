# 🌍 Services - Données de Référence

Flows Prefect pour charger et maintenir les données de référence analytics :
- **Devises** (codes ISO 4217 + taux de change quotidiens)
- **Dimension temporelle** (calendrier 2015-2035)

---

## 📋 Vue d'ensemble

| Flow | Fréquence | Description |
|------|-----------|-------------|
| `load-currency-codes` | 1x/mois | Codes devises ISO 4217 |
| `load-exchange-rates` | 1x/jour | Taux de change (base EUR) |
| `build-time-dimension` | Manuel | Dimension temporelle complète |

---

## 🗂️ Structure Projet

```
projects/Services/
├── config/
│   ├── __init__.py
│   └── pg_config.py              # Configuration PostgreSQL
├── flows/
│   ├── __init__.py
│   ├── currency_rates.py         # Flows devises
│   └── time_dimension.py         # Flow dimension temporelle
├── sql/
│   └── create_tables.sql         # DDL tables PostgreSQL
├── tests/
│   └── test_flows.py             # Tests unitaires
├── prefect.yaml                  # Configuration déploiements
└── README.md                     # Ce fichier

```

---

## 🚀 Installation & Setup

### 1. Pré-requis

```bash
# Python 3.11+
pip install prefect psycopg2-binary requests

# PostgreSQL 17+ avec accès lecture/écriture
```

### 2. Configuration PostgreSQL

Le module Services **réutilise la configuration ETL** pour éviter duplication :

```python
# Utilise automatiquement : projects/ETL/flows/config/pg_config.py
# Variables d'environnement : ETL_PG_HOST, ETL_PG_PORT, etc.
```

### 3. Créer les tables PostgreSQL

```bash
# Exécuter le DDL
psql -U postgres -d etl_db -f sql/create_tables.sql

# Vérifier
psql -U postgres -d etl_db -c "\dt reference.*"
```

**Tables créées** :
- `reference.currencies` (codes ISO)
- `reference.currency_rates` (historique)
- `reference.currency_rates_today` (snapshot)
- `reference.time_dimension` (calendrier)

---

## 🔄 Exécution

### Standalone (test local)

```bash
cd E:/Prefect/projects/Services

# Charger codes ISO
python flows/currency_rates.py --mode codes

# Charger taux de change
python flows/currency_rates.py --mode rates

# Charger les deux
python flows/currency_rates.py --mode both

# Construire time_dimension
python flows/time_dimension.py
```

### Via Prefect (production)

```bash
# 1. Déployer les flows
prefect deploy --all

# 2. Exécution manuelle
prefect deployment run services/load-currency-codes
prefect deployment run services/load-exchange-rates
prefect deployment run services/build-time-dimension

# 3. Monitoring
prefect server start  # UI : http://127.0.0.1:4200
```

---

## 📊 Données Produites

### 1. Devises (currencies)

```sql
SELECT * FROM reference.currencies LIMIT 5;

currency_code | currency_name        | last_updated
--------------|----------------------|------------------
EUR           | Euro                 | 2025-12-09 ...
USD           | United States Dollar | 2025-12-09 ...
GBP           | British Pound        | 2025-12-09 ...
```

**Volume** : ~170 codes devises

### 2. Taux de Change (currency_rates)

```sql
SELECT * FROM reference.currency_rates 
WHERE date = CURRENT_DATE 
LIMIT 5;

date       | currency | rate
-----------|----------|----------
2025-12-09 | USD      | 1.0542
2025-12-09 | GBP      | 0.8321
2025-12-09 | JPY      | 156.23
```

**Volume** : ~170 devises × nombre de jours

### 3. Snapshot Taux du Jour

```sql
SELECT * FROM reference.currency_rates_today LIMIT 5;

currency | rate    | updated_at
---------|---------|------------------
USD      | 1.0542  | 2025-12-09 08:00
GBP      | 0.8321  | 2025-12-09 08:00
```

### 4. Dimension Temporelle

```sql
SELECT * FROM reference.time_dimension 
WHERE date_id BETWEEN '2025-12-01' AND '2025-12-10';

date_id    | day_name | week | month_name | quarter | is_weekend
-----------|----------|------|------------|---------|------------
2025-12-01 | Monday   | 49   | December   | 4       | false
2025-12-09 | Tuesday  | 50   | December   | 4       | false
```

**Volume** : 7,671 jours (2015-01-01 → 2035-12-31)

---

## 🔧 Configuration Schedules

### Production (prefect.yaml)

| Flow | Schedule | Timezone |
|------|----------|----------|
| currency-codes | 1er du mois à 2h | Europe/Paris |
| exchange-rates | Quotidien à 8h | Europe/Paris |
| time-dimension | Manuel | - |

### Modifier les schedules

```yaml
# prefect.yaml
deployments:
  - name: load-exchange-rates
    schedule:
      cron: "0 8 * * *"  # Modifier ici
      timezone: "Europe/Paris"
```

---

## 📈 Monitoring

### Vérifier dernières exécutions

```sql
-- Dernière mise à jour codes devises
SELECT MAX(last_updated) FROM reference.currencies;

-- Dernière date de taux disponible
SELECT MAX(date) FROM reference.currency_rates;

-- Snapshot du jour
SELECT updated_at FROM reference.currency_rates_today LIMIT 1;
```

### Logs Prefect

```bash
# Via UI
prefect server start
# → http://127.0.0.1:4200

# Via CLI
prefect deployment ls
prefect flow-run ls --limit 10
```

---

## 🛠️ APIs Externes Utilisées

### 1. Codes Devises ISO 4217
- **URL** : `https://open.er-api.com/v6/codes`
- **Limite** : Aucune
- **Format** : JSON
- **Gratuit** : Oui

### 2. Taux de Change
- **URL** : `https://api.exchangerate.host/latest?base=EUR`
- **Limite** : 1,500 requêtes/mois (gratuit)
- **Format** : JSON
- **Base** : EUR
- **Gratuit** : Oui

---

## 🧪 Tests

```bash
cd E:/Prefect/projects/Services

# Exécuter tests
pytest tests/ -v

# Test connexion PostgreSQL
python -c "from config.pg_config import config; print(config.get_connection_string())"

# Test API devises
curl https://open.er-api.com/v6/codes

# Test API taux
curl "https://api.exchangerate.host/latest?base=EUR"
```

---

## 🔐 Sécurité

### Credentials PostgreSQL

**Configuration partagée avec ETL** :
```bash
# Variables d'environnement (PowerShell Admin)
$env:ETL_PG_HOST = "localhost"
$env:ETL_PG_PORT = "5432"
$env:ETL_PG_DATABASE = "etl_db"
$env:ETL_PG_USER = "postgres"
$env:ETL_PG_PASSWORD = "votre_password"
```

### APIs Externes

Aucun token requis (APIs publiques gratuites)

---

## 📞 Support

| Sujet | Contact |
|-------|---------|
| Bugs/Questions | CBM Analytics Team |
| Infra PostgreSQL | DBA Team |
| Prefect | Data Engineering |

---

## 🔄 Maintenance

### Annuelle (Janvier)

```bash
# Étendre time_dimension si nécessaire
# Modifier ligne 61 dans flows/time_dimension.py :
end = date(2036, 12, 31)  # +1 an

# Réexécuter
python flows/time_dimension.py
```

### Mensuelle

- Vérifier logs Prefect
- Valider taux de change du mois
- Monitoring espace disque PostgreSQL

---

## 📝 Changelog

### v1.0.0 (2025-12-09)
- ✅ Initial release
- ✅ Flows devises (codes + taux)
- ✅ Flow time_dimension
- ✅ DDL PostgreSQL complet
- ✅ Documentation complète

---

## 🎯 Roadmap

- [ ] Tests unitaires complets
- [ ] Alerting email sur échec
- [ ] Tableau de bord Grafana
- [ ] Support multi-bases (EUR, USD)
- [ ] Historique API (backup local)

---

## 📄 License

Propriété de CBM - Usage interne uniquement