import pandas as pd

liste_fichiers = ['LineA_Stable_10K.csv', 'LineB_Flux.csv', 'LineC_Turbulent.csv',
                  'LineD_SpikeControl.csv', 'LineE_SmoothRun.csv']
colonnes_visuelles = ['LineA', 'LineB', 'LineC', 'LineD', 'LineE']

# Règle d'harmonisation FIXE (décidée par toi, pas inférée)
NOMS_CIBLE = {
    'timestamp':    'timestamp (datetime)',
    'temperature':  'temperature',
    'pressure':     'pressure',
    'elapsed_time': 'elapsed_time (nullable)',
    'label':        'label',
}

# Cartographie : par ligne, le nom source réel trouvé dans le CSV
cartographie = {ligne: {col: '❌' for col in NOMS_CIBLE} for ligne in colonnes_visuelles}

for file in liste_fichiers:
    print(f"\nExploration de {file} :")
    df = pd.read_csv(f"raw/production_lines/{file}")
    print(df.shape)
    print(df.dtypes)
    print(df.head(3))
    print(df.isna().sum())
    print(df.describe())
    if 'label' in df.columns:
        print(df['label'].value_counts())
    if 'timestamp' in df.columns:
        print(df['timestamp'].min(), df['timestamp'].max())

    # --- Check cadence (manquait) ---
    if 'timestamp' in df.columns:
        ts = pd.to_datetime(df['timestamp'])
        deltas = ts.diff().value_counts().head(3)
        print(f"Cadence (top 3 deltas) :\n{deltas}")

    print("-" * 50)

    # --- Mapping source -> cible (insensible à la casse) ---
    nom_ligne = file.split('_')[0]
    if nom_ligne in cartographie:
        for col in df.columns:
            cle = col.lower()  # 'Temperature' -> 'temperature', etc.
            if cle in cartographie[nom_ligne]:
                cartographie[nom_ligne][cle] = f"`{col}`"

# --- Génération tableau Markdown ---
# nom_cible_staging = COLONNE à droite (pas ligne)
header = "| Colonne logique | " + " | ".join(colonnes_visuelles) + " | nom_cible_staging |"
sep    = "| :--- | " + " | ".join([":---"] * len(colonnes_visuelles)) + " | :--- |"

lignes_md = [header, sep]
for col_logique, cible in NOMS_CIBLE.items():
    sources = " | ".join(cartographie[l][col_logique] for l in colonnes_visuelles)
    lignes_md.append(f"| {col_logique} | {sources} | `{cible}` |")

# Lignes ajoutées en staging (pas de source, cible seulement)
lignes_md.append(f"| *(ajouté staging)* | — | — | — | — | — | `line_id` |")
lignes_md.append(f"| *(ajouté staging)* | — | — | — | — | — | `ingestion_ts` |")

print("\n### TABLEAU MARKDOWN GÉNÉRÉ :\n")
print("\n".join(lignes_md))