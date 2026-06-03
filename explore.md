### TABLEAU MARKDOWN GÉNÉRÉ :

| Colonne logique | LineA | LineB | LineC | LineD | LineE | nom_cible_staging |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| timestamp | `timestamp` | `timestamp` | `timestamp` | `timestamp` | `timestamp` | `timestamp (datetime)` |
| temperature | `Temperature` | `temperature` | `Temperature` | `temperature` | `Temperature` | `temperature` |
| pressure | `pressure` | `pressure` | `pressure` | `Pressure` | `pressure` | `pressure` |
| elapsed_time | `elapsed_time` | `Elapsed_time` | ❌ | ❌ | ❌ | `elapsed_time (nullable)` |
| label | `label` | `label` | `label` | `label` | `label` | `label` |
| *(ajouté staging)* | — | — | — | — | — | `line_id` |
| *(ajouté staging)* | — | — | — | — | — | `ingestion_ts` |