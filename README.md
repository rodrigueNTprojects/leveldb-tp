# TP LevelDB - Registres Locaux

## Objectifs pédagogiques

Ce TP se concentre sur les **concepts de LevelDB** (base clé-valeur
locale) en comparaison avec CouchDB (base distribuée).

**Vous allez apprendre:**

- Architecture LSM-tree vs B-tree
- Opérations clé-valeur atomiques
- Indexation secondaire manuelle
- Réplication sans mécanisme intégré
- Trade-offs performance vs distribution

**Durée**: 1 semaine (3-4h de travail effectif)\
**Modalités**: minimalement sous forme de Binômes

## Installation et compilation

### 1. Récupérer le code fourni

    # Cloner le repo avec le code pré-écrit
    git clone [url-leveldb-tp]
    cd leveldb-tp

    # Installer dépendances
    go mod download

### 2. Compiler tous les outils

**Linux/macOS:**

    # Utiliser le Makefile
    make build

    # Ou manuellement:
    go build -o bin/setup ./cmd/setup
    go build -o bin/loader ./cmd/loader
    go build -o bin/query ./cmd/query
    go build -o bin/replicator ./cmd/replicator
    go build -o bin/compare ./cmd/compare

**Windows:**

    # Compilation manuelle
    # Si le dossier n'existe pas il faut le créer
    mkdir bin

    # Ensuite, exécuter les commandes suivantes
    go build -o bin\setup.exe .\cmd\setup
    go build -o bin\loader.exe .\cmd\loader
    go build -o bin\query.exe .\cmd\query
    go build -o bin\replicator.exe .\cmd\replicator
    go build -o bin\compare.exe .\cmd\compare

### 3. Dataset pour les tests

Le dossier `data/` contient des fichiers CSV avec des données de test
(par exemple un maximum de 18 000 documents):

    data/
    ├── orders.csv          
    ├── products.csv        
    ├── sellers.csv         
    ├── leads_qualified.csv
    └── ...

**Note:** Vous pouvez limiter le nombre de documents à charger avec
l\'option `-``limit` :

    # Linux/macOS:
    ./bin/loader -csv ./data -limit 1000

    # Windows:
    .\bin\loader.exe -csv .\data -limit 1000
