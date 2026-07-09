# DataLake IoT Industriel

Data lake pour la supervision de lignes de production industrielles (capteurs température / pression). Pipeline orchestré avec Apache Airflow, stockage objet MinIO (S3-compatible), catalogage avec OpenMetadata.

## Architecture

Le pipeline suit une architecture en couches (medallion), documentée en détail dans [`archi_couche.md`](archi_couche.md) :

| Couche | Rôle | Format | Partitionnement | Rétention |
| :--- | :--- | :--- | :--- | :--- |
| **raw** | Source de vérité immuable, copie conforme des CSV producteurs (hash MD5 + `ingestion_ts`) | CSV | `year=/month=/line=/` | 180j puis ILM → GLACIER |
| **staging** | Données harmonisées (snake_case), typées, schéma validé | Parquet | `year=/month=/line=/` | 30-90j (régénérable depuis raw) |
| **curated** | Table modélisée pour la détection d'anomalies multi-lignes (z-score par ligne) | Parquet | `year=/month=/` (cross-lignes) | 1-2 ans |
| **archive** | Conservation froide, compliance | Parquet+zstd | héritée de raw | 2 ans → suppression |

### Flux de données (DAGs Airflow)

1. **`ingest_raw`** (`dags/raw_dag.py`) — Upload des 5 CSV producteurs (`data/`) vers `raw/`, partitionnement Hive par ligne/année/mois, vérification d'intégrité MD5/ETag. La ligne A est chunkée par jour pour simuler un flux temps réel.
2. **`transform_to_staging`** (`dags/staging_dag.py`) — Lecture de `raw/`, harmonisation des noms de colonnes (référentiel dans [`explore.md`](explore.md)), typage, ajout de `line_id`/`ingestion_ts`, validation de schéma fail-fast, écriture Parquet vers `staging/`.
3. **`build_curated`** (`dags/build_curated_dag.py`) — Agrégation de tout `staging/`, calcul des stats par ligne (moyenne/écart-type température & pression), z-score et score d'anomalie composite, écriture vers `curated/`.
4. **`catalog_ingest_staging`** (`dags/openmetadata_ingest_staging.py`) — Scan de `staging/` et ingestion des métadonnées dans OpenMetadata.

## Stack technique

- **Orchestration** : Apache Airflow 2.10.3 (LocalExecutor, backend Postgres dédié)
- **Stockage objet** : MinIO (S3-compatible), chiffrement SSE-S3 via KES/KMS
- **Catalogue de données** : OpenMetadata (backend Postgres + Elasticsearch dédiés)
- **Traitement** : pandas / pyarrow (Parquet, compression snappy)
- **Journalisation d'audit** : webhook HTTP MinIO → `infrastructure/minio/audit-logs/`

## Prérequis

- Docker + Docker Compose
- Python 3.11+ (pour lancer les scripts hors conteneur, ex. `scriptboto.py`)

## Installation

1. Copier le fichier d'environnement d'exemple et renseigner les valeurs :

   ```bash
   cp .env.example .env
   ```

   Variables à définir dans `.env` :

   | Variable | Rôle |
   | :--- | :--- |
   | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Credentials MinIO |
   | `MINIO_ENDPOINT` | Endpoint MinIO (scripts hors conteneur) |
   | `AIRFLOW_UID` | UID hôte pour les volumes montés dans les conteneurs Airflow |
   | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Compte admin créé au démarrage d'Airflow |
   | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Chaîne de connexion Postgres pour Airflow |
   | `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Base Postgres dédiée à Airflow |
   | `OPENMETA_POSTGRES_USER` / `OPENMETA_POSTGRES_PASSWORD` | Base Postgres dédiée à OpenMetadata |
   | `OPENMETA_DB_USER` / `OPENMETA_DB_USER_PASSWORD` / `OPENMETA_DATABASE` | Utilisateur applicatif de la base OpenMetadata |
   | `OPENMETA_FERNET_KEY` | Clé de chiffrement du serveur OpenMetadata (générer avec `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`) |
   | `OPENMETADATA_JWT_TOKEN` | Token du bot d'ingestion OpenMetadata (`Settings > Bots > ingestion-bot` dans l'UI, disponible seulement après le premier démarrage du serveur) |

2. Placer les clés KES pour le chiffrement MinIO à la racine du projet : `root.key` et `root.cert` (non versionnées, cf. `.gitignore`).

3. Démarrer la stack :

   ```bash
   docker compose up -d
   ```

4. Accès aux interfaces :

   | Service | URL |
   | :--- | :--- |
   | Airflow | http://localhost:8080 |
   | MinIO Console | http://localhost:9001 |
   | OpenMetadata | http://localhost:8585 |

5. Déclencher les DAGs dans l'ordre : `ingest_raw` → `transform_to_staging` → `build_curated` → `catalog_ingest_staging` (le dernier nécessite `OPENMETADATA_JWT_TOKEN` renseigné après le premier démarrage d'OpenMetadata).

## Structure du projet

```
dags/                      DAGs Airflow (ingestion, transformation, curation, catalogue)
data/, raw/                Jeux de données CSV d'exemple (5 lignes de production)
infrastructure/minio/      Policies IAM MinIO, webhook d'audit
minio_archive/             Règle de cycle de vie (ILM) MinIO
minio_client/              Script d'exemple client `mc`
scriptboto.py              Upload manuel raw/ (hors Airflow)
teststaging.py             Vérification manuelle d'un fichier staging/
verif_curated.py           Vérification manuelle d'un fichier curated/
archi_couche.md            Détail des couches du data lake
explore.md                 Référentiel d'harmonisation des colonnes source → staging
```

## Sécurité

- Aucun credential n'est en dur dans le code ou `docker-compose.yml` : tout passe par `.env` (non versionné).
- `root.cert` / `root.key` (clés KES) et les logs d'audit sont exclus du dépôt via `.gitignore`.
- Le token du bot d'ingestion OpenMetadata doit être régénéré via l'UI (`Settings > Bots > ingestion-bot`) après chaque nouveau déploiement — ne jamais le commiter.
