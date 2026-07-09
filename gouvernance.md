# Politique de gouvernance — DataLake IoT Industriel

## 1. Objet

Ce document définit qui accède à quelles données, sous quelles conditions, et les responsabilités associées à chaque rôle dans le data lake de supervision des 5 lignes de production (A-E).

## 2. Classification des données

| Donnée | Sensibilité | Justification |
|---|---|---|
| Capteurs temp/pression (raw, staging, curated) | Interne — non critique | Données de process industriel, pas de données personnelles ni financières |
| Métadonnées catalogue (OpenMetadata) | Interne | Descriptions techniques, propriétaires internes |
| Credentials (`.env`, clés KES) | Confidentiel | Accès système, jamais versionné |

Aucune donnée personnelle (RGPD) n'est présente dans ce data lake — uniquement des séries temporelles de capteurs industriels.

## 3. Rôles et périmètre d'accès

Trois comptes de service MinIO distincts, chacun avec une policy IAM dédiée (principe du moindre privilège).

| Rôle | Compte | Accès | Justification |
|---|---|---|---|
| **data-analyst** | `analyst-prod` | Lecture seule sur `curated/` | Consulte les données modélisées prêtes à l'analyse (z-score, anomaly_score) ; n'a pas besoin d'accéder aux couches techniques amont |
| **data-engineer** | `engineer-prod` | Lecture/écriture sur `raw/` et `staging/` | Opère les pipelines d'ingestion et de transformation ; n'a pas d'accès direct à `curated/` (produit uniquement par le DAG `build_curated`, pas par un humain) |
| **admin** | `admin-prod` | Tous droits (policy `consoleAdmin`) | Administration de la plateforme : buckets, IAM, ILM, chiffrement |

### Matrice d'accès par bucket

| Bucket | data-analyst | data-engineer | admin |
|---|---|---|---|
| `raw/` | ❌ | ✅ R/W | ✅ |
| `staging/` | ❌ | ✅ R/W | ✅ |
| `curated/` | ✅ R | ❌ | ✅ |
| `archive/` | ❌ | ❌ | ✅ |

Aucun rôle applicatif n'a d'accès direct à `archive/` — cette couche n'est manipulée que par les règles ILM automatiques et par un accès administrateur en cas d'audit/replay exceptionnel.

### Accès par ligne de production

Le data lake ne différencie **pas** l'accès par ligne (A-E) — toutes les lignes sont logiquement équivalentes en termes de sensibilité (mêmes types de capteurs, même criticité). La ségrégation se fait uniquement par **couche** (raw/staging/curated/archive), pas par ligne. Si une ligne devenait plus sensible (ex : produit sous NDA), une policy IAM supplémentaire scoperait l'accès via le préfixe `production_lines/.../line=X/`.

## 4. Conditions d'accès

- **Authentification** : chaque compte de service utilise une paire access key / secret key dédiée (pas de partage de credentials entre rôles)
- **Chiffrement en transit** : accès via endpoint MinIO (HTTP en développement local ; HTTPS obligatoire en production)
- **Chiffrement au repos** : SSE-S3 activé sur `raw/` (bucket contenant les données brutes les plus sensibles industriellement), via KES connecté à un KMS externe
- **Traçabilité** : tous les accès sont journalisés via le webhook d'audit MinIO (cf. section 6)
- **Principe du moindre privilège** : chaque policy IAM n'autorise que les actions strictement nécessaires au rôle (`GetObject`/`ListBucket` pour analyst ; ajout de `PutObject`/`DeleteObject` pour engineer)

## 5. Cycle de vie des données (ILM)

| Règle | Bucket cible | Déclencheur | Action |
|---|---|---|---|
| Expiration raw | `raw/production_lines/` | 730 jours | Suppression automatique |
| Transition froide | `raw/` | 180 jours | Non implémentée (nécessite un tier de stockage distant — cf. limitation section 8) |

La rétention de 2 ans couvre les besoins d'audit opérationnel courant sans accumulation indéfinie de données brutes.

## 6. Audit et traçabilité

Chaque requête S3 (lecture, écriture, suppression) est interceptée par un webhook d'audit MinIO et journalisée avec :
- Horodatage
- Compte à l'origine de la requête (`accessKey`)
- Action effectuée (`api.name`)
- Résultat (`statusCode` : 200 = autorisé, 403 = refusé)

**Exemple de scénario tracé** : une tentative de `analyst-prod` d'accéder à `raw/` est journalisée avec `statusCode: 403`, prouvant que la policy IAM restrictive s'applique effectivement (pas seulement en théorie).

Les logs sont stockés dans `infrastructure/minio/audit-logs/minio-audit.log`. Rétention recommandée en production : 1 an minimum pour les besoins de conformité.

## 7. Responsabilités par rôle

| Rôle | Responsabilités |
|---|---|
| **data-engineer** | Maintenance des DAGs Airflow (ingestion, transformation) ; garantit l'intégrité raw→staging (validation de schéma fail-fast) ; supervise l'exécution des pipelines |
| **data-analyst** | Consommation des données curated pour analyse/reporting ; ne modifie jamais les données sources ; signale les anomalies de qualité au data-engineer |
| **admin** | Gestion des comptes IAM et policies ; supervision du chiffrement et de l'ILM ; réponse aux incidents de sécurité ; revue périodique des logs d'audit |
| **Propriétaires de ligne** (référencés dans OpenMetadata) | Point de contact métier pour chaque ligne de production (A-E) ; validation de la pertinence des seuils d'anomalie documentés dans le catalogue |

## 8. Limitations connues et axes d'amélioration

- **Transition ILM vers stockage froid (180j)** : non implémentée — nécessiterait un tier de stockage distant (`mc admin tier add`), absent de cette infrastructure mono-nœud de formation. Seule l'expiration à 2 ans est active.
- **SSE-S3 via sandbox KES public** (`play.min.io:7373`) : fonctionnel pour la démonstration, mais explicitement non adapté à la production (clés accessibles publiquement). Un déploiement réel nécessiterait un KES/Vault privé.
- **Pas de rotation automatique des credentials** : les secrets des comptes de service sont statiques pour ce projet. En production, une rotation périodique (ex : via un secret manager) serait requise.
- **Granularité d'accès par ligne** : non implémentée (cf. section 3) faute de besoin identifié à ce stade ; l'architecture IAM le permettrait facilement si un besoin métier émergeait.

## 9. Références techniques

- Policies IAM : `infrastructure/minio/policies/`
- Règles ILM : `infrastructure/minio/ilm.json`
- Configuration audit : `infrastructure/minio/script_logs.py`
- Catalogue de métadonnées : OpenMetadata, service `minio-datalake-staging`