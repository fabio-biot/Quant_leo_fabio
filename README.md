# Quant Professional Stack

Projet de recherche quantitative pour construire un dataset financier, entraîner un modèle de scoring d'alpha, backtester les signaux et produire un rapport quotidien exploitable.

Le système combine des données de prix, des indicateurs techniques, des informations macro, une normalisation cross-sectionnelle, une détection de régimes de marché et un ensemble de modèles XGBoost + CatBoost.

## Objectifs

- Identifier des actifs avec une probabilité élevée de surperformance à horizon 20 jours.
- Réduire les biais classiques de backtest avec une validation walk-forward et des jointures temporelles point-in-time.
- Produire des signaux lisibles : `TOP BUY`, `WATCH (Extended)`, `EXIT / TAKE PROFIT` ou `SCANNING`.
- Tester les signaux nets de coûts de transaction.

## Architecture

```text
.
├── pipeline.py                  # Ingestion principale des données
├── engine/                      # Calculs quant, features, backtest, production
│   ├── indicators.py            # RSI, moyennes mobiles, Bollinger, ADX, volatilité...
│   ├── engineer.py              # Dataset final, ranking, neutralisation, macro join
│   ├── backtester.py            # Backtest portefeuille net de coûts
│   └── production.py            # Génération du rapport quotidien
├── models/
│   ├── ml_pipeline.py           # Entraînement walk-forward XGBoost/CatBoost
│   ├── dl_pipeline.py           # Pipeline deep learning expérimental
│   └── trained/                 # Modèles persistés
├── src/
│   ├── config.py                # Paramètres projet
│   └── database.py              # Schéma SQLite et upserts
├── reports/                     # Graphiques, backtests, rapports journaliers
├── tables_csv/                  # Exports CSV intermédiaires
├── tests/                       # Tests unitaires
└── contexte.md                  # Notes d'audit et contexte quant
```

## Installation

Prérequis : Python 3.11+ recommandé.

```bash
make install
```

Cette commande crée un environnement virtuel `venv` et installe les dépendances de `requirements.txt`.

Si vous préférez le faire manuellement :

```bash
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Les paramètres principaux sont dans `src/config.py` :

- `TICKERS` : univers de production.
- `TARGET_HORIZON` : cible utilisée par défaut (`target_20d`).
- `TRANSACTION_COSTS` : coûts appliqués au backtest.
- `MIN_PROBABILITY_BUY` : seuil minimal pour classer un actif en signal d'achat.
- `MAX_DAILY_POSITIONS` : nombre maximal de positions théoriques.

La base SQLite est définie dans `config.py` via `DB_PATH`.

## Workflow Standard

### 1. Mettre a jour les donnees

```bash
make update-data
```

Cette étape :

- initialise la base SQLite ;
- télécharge les prix via `yfinance` ;
- calcule les indicateurs techniques ;
- récupère les dimensions actifs, secteurs, industries et marchés ;
- sauvegarde les tables dans `database/database.db` et les exports dans `tables_csv/`.

### 2. Construire le dataset de recherche

```bash
make build-dataset
```

Cette étape construit `dataset_signals.csv` avec :

- features techniques ;
- neutralisation sectorielle ;
- ranking cross-sectionnel ;
- jointure macro point-in-time via `merge_asof` ;
- targets futures ;
- filtres de sur-extension (`is_extended`).

### 3. Entrainer les modeles

```bash
make train-ml
```

Cette étape entraîne XGBoost et CatBoost avec validation walk-forward, calibration des probabilités et sauvegarde :

- `models/trained/XGBoost.joblib`
- `models/trained/CatBoost.joblib`
- `models/trained/feature_list.joblib`

### 4. Generer le rapport quotidien

```bash
make today
```

Le rapport est écrit dans `reports/daily/report_YYYYMMDD.csv`.

Colonnes importantes :

- `ticker` : actif analysé.
- `Alpha Score` : probabilité moyenne XGBoost/CatBoost.
- `signal_explanation` : lecture technique du signal.
- `Status` : décision opérationnelle.
- `rsi_14`, `bb_position`, `volatility_20` : métriques de contrôle.

### 5. Lancer tout le pipeline

```bash
make run-all
```

Cette commande enchaîne ingestion, dataset, entraînement et rapport quotidien.

## Backtest et Visualisations

Backtest sur un ticker :

```bash
make backtest TICKER=AAPL
```

Graphiques IA :

```bash
. venv/bin/activate
python visualize_ai.py
```

Les résultats sont sauvegardés dans `reports/`.

## Tests

```bash
. venv/bin/activate
python -m pytest -q
```

Les tests actuels couvrent les indicateurs de base. Les prochaines priorités de test sont :

- stabilité de `build_final_dataset` ;
- alignement des features entre entraînement et production ;
- métriques du backtester sur un dataset synthétique ;
- absence de fuite temporelle sur les jointures macro.

## Notes Quant

Le projet applique plusieurs garde-fous :

- validation walk-forward au lieu d'un split aléatoire ;
- calibration des probabilités avant production ;
- transaction costs dans le backtest ;
- neutralisation sectorielle ;
- ranking cross-sectionnel ;
- protection contre l'achat d'actifs trop étendus via `is_extended`.

Limites actuelles :

- l'univers utilise les constituants actuels, donc le survivorship bias n'est pas totalement supprimé ;
- les données viennent principalement de sources gratuites, avec des risques de trous ou corrections historiques ;
- les signaux ne constituent pas un conseil financier et doivent être validés avant toute exécution réelle.

## Commandes Utiles

```bash
make install         # Installe l'environnement
make update-data     # Recharge les donnees
make build-dataset   # Reconstruit dataset_signals.csv
make train-ml        # Entraine et sauvegarde les modeles
make today           # Produit le rapport quotidien
make run-all         # Lance toute la chaine
make clean           # Nettoie caches Python et dataset genere
```
