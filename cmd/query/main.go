// cmd/query/main.go
// Outil de requêtes et statistiques sur les nœuds LevelDB

package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "path/filepath"
    
    //"github.com/syndtr/goleveldb/leveldb"
    //"github.com/syndtr/goleveldb/leveldb/util"
    tpleveldb "leveldb-tp/pkg/leveldb"
)

func main() {
    var (
        node     = flag.String("node", "node1", "Nœud à interroger (node1 ou node2)")
        count    = flag.Bool("count", false, "Compter le nombre de documents")
        stats    = flag.Bool("stats", false, "Afficher statistiques détaillées")
        get      = flag.String("get", "", "Récupérer un document par clé")
        index    = flag.String("index", "", "Champ d'index pour recherche")
        value    = flag.String("value", "", "Valeur à rechercher dans l'index")
        limit    = flag.Int("limit", 10, "Limite de résultats")
        verify   = flag.String("verify", "", "Vérifier l'intégrité d'un document")
    )
    flag.Parse()
    
    log.SetFlags(0) // Pas de timestamp dans les logs
    
    // Construire chemin du nœud
    nodePath := filepath.Join("leveldb-stores", *node)
    
    // Ouvrir client
    client, err := tpleveldb.NewClient(nodePath)
    if err != nil {
        log.Fatalf("Erreur ouverture nœud %s: %v", *node, err)
    }
    defer client.Close()
    
    // Router vers la bonne action
    switch {
    case *count:
        doCount(client, *node)
    case *stats:
        doStats(client, *node)
    case *get != "":
        doGet(client, *get)
    case *index != "" && *value != "":
        doSearch(client, *node, *index, *value, *limit)
    case *verify != "":
        doVerify(client, *verify)
    default:
        fmt.Println("Outil de requêtes LevelDB")
        fmt.Println()
        fmt.Println("Usage:")
        fmt.Println("  query -node node1 -count                    # Compter documents")
        fmt.Println("  query -node node1 -stats                    # Statistiques")
        fmt.Println("  query -node node1 -get order:00001          # Récupérer document")
        fmt.Println("  query -node node1 -index region -value NA   # Recherche par index")
        fmt.Println("  query -node node1 -verify order:00001       # Vérifier intégrité")
        fmt.Println()
        fmt.Println("Options:")
        flag.PrintDefaults()
    }
}

// doCount compte le nombre de documents
func doCount(client *tpleveldb.Client, node string) {
    count, err := client.Count()
    if err != nil {
        log.Fatalf("Erreur comptage: %v", err)
    }
    
    fmt.Printf("Nœud %s: %d documents\n", node, count)
}

// doStats affiche des statistiques détaillées
func doStats(client *tpleveldb.Client, node string) {
    fmt.Printf("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n")
    fmt.Printf("┃   Statistiques Nœud: %-17s┃\n", node)
    fmt.Printf("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
    fmt.Println()
    
    // Compter total
    total, err := client.Count()
    if err != nil {
        log.Fatalf("Erreur comptage: %v", err)
    }
    
    fmt.Printf("Documents totaux:     %d\n", total)
    fmt.Println()
    
    // Compter par type (parcourir et compter les préfixes)
    types := map[string]int{
        "order":   0,
        "product": 0,
        "seller":  0,
        "lead":    0,
        "idx":     0, // Index
    }
    
    db := client.GetDB()
    // Remplacer db.Find par une approche correcte utilisant NewIterator
    iter := db.NewIterator(nil, nil)
    defer iter.Release()
    
    for iter.Next() {
        key := string(iter.Key())
        
        // Ignorer clés système
        if len(key) > 0 && key[0] == '_' {
            continue
        }
        
        // Compter par préfixe
        for prefix := range types {
            if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
                types[prefix]++
                break
            }
        }
    }
    
    if err := iter.Error(); err != nil {
        log.Fatalf("Erreur parcours DB: %v", err)
    }
    
    fmt.Println("Répartition par type:")
    fmt.Printf("  Commandes:          %d\n", types["order"])
    fmt.Printf("  Produits:           %d\n", types["product"])
    fmt.Printf("  Vendeurs:           %d\n", types["seller"])
    fmt.Printf("  Prospects:          %d\n", types["lead"])
    fmt.Printf("  Index secondaires:  %d\n", types["idx"])
    fmt.Println()
    
    // Taille sur disque
    nodePath := filepath.Join("leveldb-stores", node)
    diskSize := getDiskSize(nodePath)
    fmt.Printf("Taille sur disque:    %.2f MB\n", diskSize)
}

// doGet récupère un document
func doGet(client *tpleveldb.Client, key string) {
    entry, err := client.Get(key)
    if err != nil {
        log.Fatalf("Document non trouvé: %v", err)
    }
    
    // Afficher joliment
    fmt.Printf("Document: %s\n", key)
    fmt.Println("════════════════════════════════════════")
    
    // Parser et afficher le JSON
    var prettyJSON map[string]interface{}
    if err := json.Unmarshal(entry.Data, &prettyJSON); err == nil {
        formatted, _ := json.MarshalIndent(prettyJSON, "", "  ")
        fmt.Println(string(formatted))
    } else {
        fmt.Println(string(entry.Data))
    }
    
    fmt.Println("════════════════════════════════════════")
    fmt.Printf("Hash:      %s\n", entry.Hash)
    fmt.Printf("Timestamp: %s\n", entry.Timestamp)
    fmt.Printf("Nœud:      %s\n", entry.Node)
}

// doSearch recherche via index secondaire
func doSearch(client *tpleveldb.Client, node, field, value string, limit int) {
    indexer := tpleveldb.NewIndexer(client.GetDB())
    
    // Déterminer le type (on suppose "order" par défaut)
    recordType := "order"
    
    fmt.Printf("Recherche: %s = %s (nœud: %s)\n", field, value, node)
    fmt.Println("════════════════════════════════════════")
    
    results, err := indexer.SearchByIndex(recordType, field, value)
    if err != nil {
        log.Fatalf("Erreur recherche: %v", err)
    }
    
    if len(results) == 0 {
        fmt.Println("Aucun résultat trouvé")
        return
    }
    
    fmt.Printf("Trouvé %d résultat(s)\n\n", len(results))
    
    // Afficher les premiers résultats
    count := 0
    for _, key := range results {
        if count >= limit {
            fmt.Printf("... et %d autres résultats\n", len(results)-limit)
            break
        }
        
        entry, err := client.Get(key)
        if err != nil {
            continue
        }
        
        // Afficher résumé
        fmt.Printf("%d. %s\n", count+1, key)
        
        // Parser et afficher un champ pertinent
        var data map[string]interface{}
        if err := json.Unmarshal(entry.Data, &data); err == nil {
            if amount, ok := data["amount"].(float64); ok {
                fmt.Printf("   Montant: %.2f\n", amount)
            }
            if region, ok := data["region"].(string); ok {
                fmt.Printf("   Région: %s\n", region)
            }
        }
        
        fmt.Println()
        count++
    }
}

// doVerify vérifie l'intégrité d'un document
func doVerify(client *tpleveldb.Client, key string) {
    fmt.Printf("Vérification intégrité: %s\n", key)
    fmt.Println("════════════════════════════════════════")
    
    valid, err := client.VerifyIntegrity(key)
    if err != nil {
        log.Fatalf("Erreur vérification: %v", err)
    }
    
    if valid {
        fmt.Println("✓ Intégrité vérifiée - Hash valide")
        fmt.Println()
        
        // Afficher le document
        entry, _ := client.Get(key)
        fmt.Printf("Hash: %s\n", entry.Hash)
        fmt.Printf("Timestamp: %s\n", entry.Timestamp)
    } else {
        fmt.Println("✗ ATTENTION: Hash invalide!")
        fmt.Println("Le document a peut-être été corrompu ou modifié")
    }
}

// getDiskSize calcule la taille d'un dossier
func getDiskSize(path string) float64 {
    var size int64
    
    err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() {
            size += info.Size()
        }
        return nil
    })
    
    if err != nil {
        return 0
    }
    
    return float64(size) / 1024 / 1024 // MB
}