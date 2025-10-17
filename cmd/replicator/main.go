// cmd/replicator/main.go
// Outil de réplication manuelle entre nœuds LevelDB

package main

import (
    "bytes"
    "encoding/base64"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "strings"
    "time"
    
    "github.com/syndtr/goleveldb/leveldb"
)

// ReplicationEntry représente une entrée pour export/import
type ReplicationEntry struct {
    Key       string          `json:"key"`
    Value     json.RawMessage `json:"value,omitempty"`
    RawValue  string          `json:"raw_value,omitempty"`
    IsRaw     bool            `json:"is_raw,omitempty"`
}

var quietMode bool

func main() {
    var (
        action = flag.String("action", "", "export|import|validate")
        source = flag.String("source", "", "Nœud source (export)")
        target = flag.String("target", "", "Nœud cible (import)")
        input  = flag.String("input", "", "Fichier JSON (import)")
        output = flag.String("output", "", "Fichier JSON (export)")
        node1  = flag.String("node1", "", "Nœud 1 (validate)")
        node2  = flag.String("node2", "", "Nœud 2 (validate)")
        quiet  = flag.Bool("quiet", false, "Mode silencieux (moins de logs)")
    )
    flag.Parse()
    
    quietMode = *quiet
    
    // Désactiver les timestamps dans les logs
    log.SetFlags(0)
    
    switch *action {
    case "export":
        if *source == "" || *output == "" {
            log.Fatal("Usage: -action export -source <node> -output <file.json>")
        }
        exportNode(*source, *output)
        
    case "import":
        if *target == "" || *input == "" {
            log.Fatal("Usage: -action import -target <node> -input <file.json>")
        }
        importNode(*target, *input)
        
    case "validate":
        if *node1 == "" || *node2 == "" {
            log.Fatal("Usage: -action validate -node1 <node1> -node2 <node2>")
        }
        validateReplication(*node1, *node2)
        
    default:
        fmt.Println("Actions disponibles:")
        fmt.Println("  export   - Exporter un nœud vers JSON")
        fmt.Println("  import   - Importer JSON vers un nœud")
        fmt.Println("  validate - Valider cohérence entre 2 nœuds")
        fmt.Println()
        fmt.Println("Exemples:")
        fmt.Println("  ./replicator -action export -source node1 -output export.json")
        fmt.Println("  ./replicator -action import -target node2 -input export.json")
        fmt.Println("  ./replicator -action validate -node1 node1 -node2 node2")
    }
}

// exportNode exporte toutes les entrées d'un nœud vers JSON
func exportNode(nodeName, outputFile string) {
    start := time.Now()
    
    nodePath := filepath.Join("leveldb-stores", nodeName)
    db, err := leveldb.OpenFile(nodePath, nil)
    if err != nil {
        log.Fatalf("Erreur ouverture %s: %v", nodePath, err)
    }
    defer db.Close()
    
    // Créer fichier JSON
    file, err := os.Create(outputFile)
    if err != nil {
        log.Fatalf("Erreur création fichier: %v", err)
    }
    defer file.Close()
    
    file.WriteString("[\n")
    
    // Itérer toutes les clés
    iter := db.NewIterator(nil, nil)
    defer iter.Release()
    
    count := 0
    first := true
    
    for iter.Next() {
        key := iter.Key()
        value := iter.Value()
        
        // Ignorer clés système (commencent par _)
        if len(key) > 0 && key[0] == '_' {
            continue
        }
        
        if !first {
            file.WriteString(",\n")
        }
        first = false
        
        keyStr := string(key)
        entry := ReplicationEntry{
            Key: keyStr,
        }
        
        // Gestion spéciale pour les clés d'index ou valeurs non-JSON
        if strings.HasPrefix(keyStr, "idx:") || !isValidJSON(value) {
            // Encoder la valeur en base64
            entry.RawValue = base64.StdEncoding.EncodeToString(value)
            entry.IsRaw = true
        } else {
            // Pour les données JSON normales
            entry.Value = json.RawMessage(value)
        }
        
        jsonBytes, err := json.MarshalIndent(entry, "  ", "  ")
        if err != nil {
            if !quietMode {
                log.Printf("Erreur sérialisation %s: %v", key, err)
            }
            continue
        }
        
        file.Write(jsonBytes)
        count++
    }
    
    file.WriteString("\n]")
    
    if err := iter.Error(); err != nil {
        log.Fatalf("Erreur itération: %v", err)
    }
    
    duration := time.Since(start)
    
    // Sortie simplifiée
    fmt.Printf("✓ %d documents exportés vers %s\n", count, outputFile)
    if !quietMode {
        fmt.Printf("Temps: %dms\n", duration.Milliseconds())
    }
}

// importNode importe un fichier JSON dans un nœud
func importNode(nodeName, inputFile string) {
    start := time.Now()
    
    nodePath := filepath.Join("leveldb-stores", nodeName)
    db, err := leveldb.OpenFile(nodePath, nil)
    if err != nil {
        log.Fatalf("Erreur ouverture %s: %v", nodePath, err)
    }
    defer db.Close()
    
    // Lire fichier JSON
    data, err := os.ReadFile(inputFile)
    if err != nil {
        log.Fatalf("Erreur lecture fichier: %v", err)
    }
    
    var entries []ReplicationEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        log.Fatalf("Erreur parsing JSON: %v", err)
    }
    
    // Insérer en batch pour performance
    batch := new(leveldb.Batch)
    batchSize := 0
    totalImported := 0
    
    for i, entry := range entries {
        var valueBytes []byte
        
        // Gestion des valeurs encodées en base64
        if entry.IsRaw {
            var err error
            valueBytes, err = base64.StdEncoding.DecodeString(entry.RawValue)
            if err != nil {
                if !quietMode {
                    log.Printf("Erreur décodage base64 pour %s: %v", entry.Key, err)
                }
                continue
            }
        } else {
            // Pour les données JSON
            valueBytes = []byte(entry.Value)
        }
        
        batch.Put([]byte(entry.Key), valueBytes)
        batchSize++
        
        // Écrire par lots de 1000 pour éviter batch trop gros
        if batchSize >= 1000 || i == len(entries)-1 {
            if err := db.Write(batch, nil); err != nil {
                if !quietMode {
                    log.Printf("Erreur import batch: %v", err)
                }
            } else {
                totalImported += batchSize
            }
            batch = new(leveldb.Batch)
            batchSize = 0
        }
    }
    
    duration := time.Since(start)
    
    // Sortie simplifiée
    fmt.Printf("✓ %d documents importés dans %s\n", totalImported, nodeName)
    if !quietMode {
        fmt.Printf("Temps: %dms\n", duration.Milliseconds())
    }
}

// validateReplication valide la cohérence entre deux nœuds
func validateReplication(node1Name, node2Name string) {
    node1Path := filepath.Join("leveldb-stores", node1Name)
    node2Path := filepath.Join("leveldb-stores", node2Name)
    
    db1, err := leveldb.OpenFile(node1Path, nil)
    if err != nil {
        log.Fatalf("Erreur ouverture %s: %v", node1Path, err)
    }
    defer db1.Close()
    
    db2, err := leveldb.OpenFile(node2Path, nil)
    if err != nil {
        log.Fatalf("Erreur ouverture %s: %v", node2Path, err)
    }
    defer db2.Close()
    
    // Compter clés dans chaque nœud
    count1 := countKeys(db1)
    count2 := countKeys(db2)
    
    if !quietMode {
        fmt.Printf("\n%s: %d clés\n", node1Name, count1)
        fmt.Printf("%s: %d clés\n", node2Name, count2)
    }
    
    if count1 != count2 {
        fmt.Printf("\n⚠ ATTENTION: Nombre de clés différent!\n")
        fmt.Printf("   Différence: %d clés\n", abs(count1-count2))
        return
    }
    
    // Vérifier contenu de chaque clé
    iter := db1.NewIterator(nil, nil)
    defer iter.Release()
    
    mismatches := 0
    missing := 0
    checked := 0
    
    for iter.Next() {
        key := iter.Key()
        
        // Ignorer clés système
        if len(key) > 0 && key[0] == '_' {
            continue
        }
        
        value1 := iter.Value()
        
        // Chercher dans node2
        value2, err := db2.Get(key, nil)
        if err != nil {
            if !quietMode {
                log.Printf("⚠ Clé manquante dans %s: %s", node2Name, key)
            }
            missing++
            continue
        }
        
        // Comparaison intelligente selon le type de données
        keyStr := string(key)
        isEqual := false
        
        if strings.HasPrefix(keyStr, "idx:") {
            // Comparaison binaire pour les index
            isEqual = bytes.Equal(value1, value2)
        } else if isValidJSON(value1) && isValidJSON(value2) {
            // Comparaison sémantique pour JSON
            isEqual = compareJSON(value1, value2)
        } else {
            // Comparaison binaire pour les autres types
            isEqual = bytes.Equal(value1, value2)
        }
        
        if !isEqual {
            if !quietMode {
                log.Printf("⚠ Valeur différente pour: %s", key)
            }
            mismatches++
        }
        
        checked++
    }
    
    // Sortie simplifiée
    if missing == 0 && mismatches == 0 {
        fmt.Printf("✓ Les nœuds sont identiques (%d documents)\n", checked)
    } else {
        fmt.Printf("\n⚠ ÉCHEC: %d problèmes détectés\n", missing+mismatches)
        fmt.Printf("  Clés manquantes:     %d\n", missing)
        fmt.Printf("  Valeurs différentes: %d\n", mismatches)
    }
}

// countKeys compte le nombre de clés (hors clés système)
func countKeys(db *leveldb.DB) int {
    iter := db.NewIterator(nil, nil)
    defer iter.Release()
    
    count := 0
    for iter.Next() {
        key := iter.Key()
        if len(key) > 0 && key[0] != '_' {
            count++
        }
    }
    return count
}

// isValidJSON vérifie si une valeur est du JSON valide
func isValidJSON(data []byte) bool {
    var js interface{}
    return json.Unmarshal(data, &js) == nil
}

// compareJSON compare deux valeurs JSON en ignorant les différences de formatage
func compareJSON(json1, json2 []byte) bool {
    var obj1, obj2 interface{}
    
    if err := json.Unmarshal(json1, &obj1); err != nil {
        return false
    }
    
    if err := json.Unmarshal(json2, &obj2); err != nil {
        return false
    }
    
    // Remarshal pour comparer la représentation canonique
    normalizedJSON1, _ := json.Marshal(obj1)
    normalizedJSON2, _ := json.Marshal(obj2)
    
    return bytes.Equal(normalizedJSON1, normalizedJSON2)
}

// abs retourne la valeur absolue
func abs(x int) int {
    if x < 0 {
        return -x
    }
    return x
}