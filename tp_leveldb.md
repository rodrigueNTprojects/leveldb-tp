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
    git clone https://github.com/rodrigueNTprojects/tp_levelDB_IND500.git
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

## Étape 0 : Découverte et Opérations de Base (25 points)

### Mission 0.1 : Comprendre l\'architecture LevelDB (5 points)

**Objectif**: Observer comment LevelDB stocke les données (LSM-tree).

**Action:**

    # 1. Initialiser les nœuds

    # Linux/macOS:
    ./bin/setup

    # Windows:
    .\bin\setup.exe

    # 2. Observer la structure créée

    # Linux/macOS:
    ls -la leveldb-stores/node1/

    # Windows (PowerShell):
    Get-ChildItem -Force leveldb-stores\node1

    # Windows (cmd):
    dir /S leveldb-stores\node1

    # Exemple de sortie attendue:
    # leveldb-stores/node1/
    # ├── 000005.ldb          # SSTable (fichier trié)
    # ├── 000006.log          # Write-Ahead Log (WAL)
    # ├── CURRENT             # Pointeur version actuelle
    # ├── LOCK                # Verrou
    # └── MANIFEST-000004     # Métadonnées

**Questions à répondre (livrable 0.1):**

1.  **Comparez avec CouchDB**: Combien de fichiers CouchDB crée-t-il
    pour une base?
2.  **Architecture LSM-tree**: À quoi sert le fichier `.log` vs
    `.``ldb`?
3.  **Compaction**: Que se passe-t-il quand il y a trop de fichiers
    `.``ldb`?

### Mission 0.2 : PUT/GET avec intégrité (5 points)

**Objectif**: Insérer et lire une donnée avec hash SHA-256.

**Fichier à compléter**: `missions/mission_0_``2.go`

**Test:**

    # Linux/macOS:
    go run missions/mission_0_2.go

    # Windows:
    go run missions\mission_0_2.go

**Livrable 0.2**: Code complété + copie de la sortie du programme.

### Mission 0.3 : Batch atomique (5 points)

**Objectif**: Comprendre les transactions atomiques (tout ou rien).

**Fichier à compléter**: `missions/mission_0_``3.go`

**Test:**

    # Linux/macOS:
    go run missions/mission_0_3.go

    # Windows:
    go run missions\mission_0_3.go

**Questions (livrable 0.3):**

1.  Temps d\'insertion de 100 docs en batch?
2.  Que se passe-t-il si une des 100 insertions échoue? (tout annulé ou
    partiel?)
3.  Comparez avec CouchDB `_``bulk_docs`: quelle différence?

### Mission 0.4 : Snapshot et restauration (10 points)

**Objectif**: Comprendre comment LevelDB peut être sauvegardé et
restauré.

**Action:**

    # 1. Charger quelques données de test

    # Linux/macOS:
    ./bin/loader -csv ./data -limit 1000

    # Windows:
    .\bin\loader.exe -csv .\data -limit 1000

    # 2. Vérifier le nombre de documents

    # Linux/macOS:
    ./bin/query -count

    # Windows:
    .\bin\query.exe -count

    # 3. Créer un snapshot (copie complète)

    # Linux/macOS:
    ./bin/setup -snapshot node1

    # Windows:
    .\bin\setup.exe -snapshot node1

**À ce stade, vous devriez voir un dossier** `snapshots` **avec un
sous-dossier contenant votre copie.**

Maintenant, simulons un changement de données: Exécuter le script du
fichier mission_0_4_modify.go qui a été fournit

    # Exécuter la modification

    # Linux/macOS:
    go run missions/mission_0_4_modify.go

    # Windows:
    go run missions\mission_0_4_modify.go

    # Vérifier qu'un document a bien été modifié

    # Linux/macOS:
    ./bin/query -get order:00001

    # Windows:
    .\bin\query.exe -get order:00001

    # Maintenant restaurer depuis le snapshot

    # Linux/macOS:
    ./bin/setup -restore node1,snapshots/node1_XXXXXXXXX

    # Windows:
    .\bin\setup.exe -restore node1,snapshots\node1_XXXXXXXXX
    # (Remplacez XXXXXXXXX par le timestamp dans votre dossier)

    # Vérifier que le document est revenu à son état initial

    # Linux/macOS:
    ./bin/query -get order:00001

    # Windows:
    .\bin\query.exe -get order:00001

**Livrable 0.4:**

- Capture d\'écran du dossier `snapshots/`
- Log de la commande restore
- Question: **En quoi les snapshots LevelDB diffèrent-ils des révisions
  CouchDB?**

## Étape 1 : Concepts Avancés et Comparaison (25 points)

### Mission 1.1 : Indexation secondaire (10 points)

**Objectif**: Comprendre comment implémenter un index secondaire dans
LevelDB.

**Contexte**: LevelDB n\'a que des clés primaires. Vous devez créer un
index secondaire manuel pour chercher des commandes par région (NA, EU,
AP).

**Fichier à compléter**: `missions/mission_1_``1.go`

**Test:**

    # Linux/macOS:
    go run missions/mission_1_1.go

    # Windows:
    go run missions\mission_1_1.go

**Après avoir créé les index, testez la recherche avec l\'outil query:**

    # Linux/macOS:
    ./bin/query -index region -value NA

    # Windows:
    .\bin\query.exe -index region -value NA

**Questions (livrable 1.1):**

1.  Combien de commandes NA trouvées?
2.  Quel est le format exact de la clé d\'index créée dans LevelDB?
3.  En CouchDB, comment fait-on la même chose? (Comparez les techniques
    d\'indexation)

### Mission 1.2 : Réplication manuelle (5 points)

**Objectif**: Comprendre pourquoi la réplication distribuée est
complexe.

**Expérience 1: Réplication simple**

    # 1. Export node1 → fichier JSON

    # Linux/macOS:
    ./bin/replicator -action export -source node1 -output export.json

    # Windows:
    .\bin\replicator.exe -action export -source node1 -output export.json

    # 2. Import du fichier JSON → node2

    # Linux/macOS:
    ./bin/replicator -action import -target node2 -input export.json

    # Windows:
    .\bin\replicator.exe -action import -target node2 -input export.json

    # 3. Valider la cohérence

    # Linux/macOS:
    ./bin/replicator -action validate -node1 node1 -node2 node2

    # Windows:
    .\bin\replicator.exe -action validate -node1 node1 -node2 node2

**Expérience 2: Simulation de conflit**

**Trouver le fichier** `missions/``test_``conflict.go`**:**

**Exécuter le script du fichier test_conflict.go qui a été fournit**

    # Linux/macOS:
    go run missions/test_conflict.go
    # Puis suivez les instructions à l'écran

    # Windows:
    go run missions\test_conflict.go
    # Puis suivez les instructions à l'écran

**Questions (livrable 1.2):**

1.  Combien de temps faut-il pour répliquer 1000 documents?
2.  Que se passe-t-il avec le document en conflit après la réplication?
3.  En quoi cette approche diffère-t-elle de la réplication CouchDB?
4.  Comment CouchDB gère-t-il les conflits de réplication?

### Mission 1.3 : Compaction manuelle (5 points)

**Objectif**: Observer comment LevelDB fusionne ses fichiers SSTable.

**Action:**

    # 1. Nettoyer les nœuds existants

    # Linux/macOS:
    ./bin/setup -clean
    ./bin/setup
    ./bin/loader -csv ./data -limit 1000

    # Windows:
    .\bin\setup.exe -clean
    .\bin\setup.exe
    .\bin\loader.exe -csv .\data -limit 1000

    # 2. Ajouter des données par petits lots pour créer plusieurs fichiers
    # Exécuter un des scripts de chargement par lot de 500 fichiers avec offset suivant: scripts/load_batches.sh, scripts/load_batches.ps1, scripts/load_batches.bat

    # Linux/macOS:
    # Rendre le script exécutable
    chmod +x /scripts/load_batches.sh
    # Exécuter le script
    ./scripts/load_batches.sh


    # Windows (PowerShell):
    .\scripts\load_batches.ps1

    # Windows (cmd):
    .\scripts\load_batches.bat

    # 3. Observer les fichiers créés

    # Linux/macOS:
    ls -lh leveldb-stores/node1/*.ldb

    # Windows (PowerShell):
    Get-ChildItem leveldb-stores\node1\*.ldb | Format-Table Name,Length

    # Windows (cmd):
    dir leveldb-stores\node1\*.ldb

**Trouvez le fichier** `missions/``compact.go`**:**

    # Exécuter la compaction

    # Linux/macOS:
    go run missions/compact.go node1

    # Windows:
    go run missions\compact.go node1

    # Observer les fichiers après compaction

    # Linux/macOS:
    ls -lh leveldb-stores/node1/*.ldb

    # Windows (PowerShell):
    Get-ChildItem leveldb-stores\node1\*.ldb | Format-Table Name,Length

    # Windows (cmd):
    dir leveldb-stores\node1\*.ldb

**Livrable 1.3:**

- Capture AVANT compaction (nombre et taille des fichiers)
- Capture APRÈS compaction
- Explication: **Pourquoi la compaction améliore-t-elle les
  performances?**

### Mission 1.4 : Benchmark et analyse (5 points)

**Objectif**: Comparer quantitativement LevelDB vs CouchDB.

**Préparation:** Assurez-vous que CouchDB est installé et accessible sur
<http://localhost:5987>

- url:<http://localhost:5987>
- username: admin
- Password: ecommerce2024
- Base de données de test: benchmark_test
- **Notes:** Si vous avez déjà fait le tp de couchDB vous n\'avez rien à
  faire. Le script va créer automatiquement la base de données, charger
  les documents et comparer avec levelDB

<!-- -->

    # Exécuter le benchmark

    # Linux/macOS:
    ./bin/compare -dataset 5000

    # Windows:
    .\bin\compare.exe -dataset 5000

Le script compare.exe effectue:

1.  Insertion séquentielle de 5000 documents
2.  Lecture aléatoire de 1000 documents
3.  Insertion en batch de 1000 documents
4.  Recherche sur 100 documents par index
5.  Mesure de la taille sur disque et l\'utilisation mémoire

**Livrable 1.4 - Document d\'analyse (1 page max):**

Répondez aux questions suivantes:

1.  **Performances**: Dans quels cas LevelDB est-il plus rapide? Dans
    quels cas CouchDB est-il plus adapté?

2.  **Trade-offs**: Complétez le tableau:

  --------------------------------------------------------------
  **Critère**        **LevelDB**    **CouchDB**   **Choix
                                                  recommandé**
  ------------------ -------------- ------------- --------------

  --------------------------------------------------------------

  -----------------------------------------------------------
  Performance                                  
  écriture                                     
  ------------------ -------------- ---------- --------------
  Distribution                                 
  multi-régions                                

  Complexité                                   
  opérationnelle                               

  Résilience aux                               
  pannes                                       
  -----------------------------------------------------------

1.  **Cas d\'usage e-commerce**: Pour chacun, justifiez LevelDB ou
    CouchDB:
    - Application mobile de point de vente (POS)
    - Registre de transactions multi-pays
    - Cache de catalogue produits
    - Audit trail global
2.  **Architecture hybride**: Proposez une architecture combinant les
    deux bases de données.

## 📦 Livrables finaux

### Étape 0 (25 points)

- Livrable 0.1: Réponses questions architecture LSM
- Livrable 0.2: Code complété + sortie programme
- Livrable 0.3: Code complété + réponses questions
- Livrable 0.4: Captures snapshot + explication différence CouchDB

### Étape 1 (25 points)

- Livrable 1.1: Code indexation + réponses questions
- Livrable 1.2: Log réplication + analyse conflits
- Livrable 1.3: Captures compaction + explication performances
- Livrable 1.4: Document d\'analyse (1 page)

## 🎓 Critères d\'évaluation

  --------------------------------------------------
  Critère             Points   Détails
  ------------------- -------- ---------------------
  **Compréhension     5        Questions
  LSM-tree**                   architecture

  **Opérations de     10       PUT/GET/Batch
  base**                       fonctionnels

  **Snapshots**       5        Backup/restore +
                               analyse

  **Indexation        8        Code + compréhension
  secondaire**                 

  **Réplication       5        Analyse complexité
  manuelle**                   

  **Compaction**      3        Observation +
                               explication

  **Analyse           9        Document qualité
  comparative**                

  **TOTAL**           **50**   
  --------------------------------------------------

## 💡 Conseils

### Pour réussir

✅ **Focus sur les concepts**, pas le code\
✅ **Comparez systématiquement** avec CouchDB\
✅ **Testez chaque mission** avant de passer à la suivante\
✅ **Documentez vos observations** au fur et à mesure

### Pièges à éviter

❌ Passer trop de temps sur le code (90% fourni)\
❌ Oublier de comparer avec CouchDB\
❌ Négliger les questions d\'analyse\
❌ Charger les CSV complets (trop lourds)

## 📚 Ressources fournies

### Code (90% fourni)

- `pkg/``leveldb``/``client.go` - Client complet avec hash
- `pkg/``leveldb``/``indexer.go` - Indexation secondaire
- `cmd``/setup/``main.go` - Setup/snapshot
- `cmd``/loader/``main.go` - Chargement CSV
- `cmd``/replicator/``main.go` - Export/import/validate
- `cmd``/compare/``main.go` - Benchmark automatique
- `cmd``/``query``/``main.go` - lire le registre

### Missions (squelettes à compléter)

- `missions/mission_0_``2.go` - PUT/GET
- `missions/mission_0_``3.go` - Batch
- `missions/mission_1_``1.go` - Indexation

### Missions (scripts à exécuter)

- `missions/mission_0_4_``modify.go` - Snapshot et restauration
- `missions/``test_``conflict.go` - Simuler conflit
- `missions/``compact.go` - Implémenter compaction
- `scripts/load_batches.sh` - chargement par batch avec offset sur
  linux/macOS
- `scripts/load_batches.ps1` - chargement par batch avec offset sur
  powershell
- `scripts/load_batches.bat` - chargement par batch avec offset sur cmd

### Dataset

- `data/*.csv`

## ❓ FAQ

**Q: Combien de lignes de code à écrire?**\
R: Maximum 15 lignes au total (missions principales).

**Q: Combien de temps par mission?**\
R: 15-30 minutes par mission. Total: 3-4h pour les 2 étapes.

**Q: Faut-il connaître Go?**\
R: Non, le code est fourni. Vous complétez juste quelques lignes
simples.

**Bonne chance!** 🚀
