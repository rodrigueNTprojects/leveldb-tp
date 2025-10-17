// ============================================================
// missions/mission_1_1.go
// Mission 1.1 : Indexation secondaire (1 ligne à compléter)
// ============================================================

package main

import (
    "fmt"
    "leveldb-tp/pkg/leveldb"
)

func main() {
    fmt.Println("=== Mission 1.1 : Indexation secondaire ===\n")
    
    client, err := leveldb.NewClient("leveldb-stores/node1")
    if err != nil {
        panic(err)
    }
    defer client.Close()
    
    indexer := leveldb.NewIndexer(client.GetDB())
    
    // Charger 100 commandes
    fmt.Println("Création de 100 commandes avec différentes régions...")
    for i := 0; i < 100; i++ {
        key := fmt.Sprintf("order:%05d", i)
        region := getRegion(i) // "NA", "EU", "AP"
        
        // Insérer document
        doc := map[string]interface{}{
            "order_id": key,
            "region":   region,
            "amount":   100.0 + float64(i),
        }
        client.Put(key, doc)
        
        // TODO Mission 1.1 - COMPLÉTEZ L'INDEXATION (1 ligne)
        // Créer un index secondaire sur le champ "region"
        // Format: indexer.CreateIndex(type, field, value, primaryKey)
        
        // ============ VOTRE CODE ICI ============
        
        // ========================================
    }
    
    // Recherche via index
    fmt.Println("\nRecherche des commandes par région:")
    
    regions := []string{"NA", "EU", "AP"}
    for _, region := range regions {
        results, err := indexer.SearchByIndex("order", "region", region)
        if err != nil {
            fmt.Printf("Erreur recherche %s: %v\n", region, err)
            continue
        }
        
        fmt.Printf("Région %s: %d commandes\n", region, len(results))
        
        // Afficher 3 premiers résultats
        for i := 0; i < 3 && i < len(results); i++ {
            fmt.Printf("  - %s\n", results[i])
        }
        fmt.Println()
    }
    
    fmt.Println("\n✓ Mission 1.1 réussie!")
}

func getRegion(i int) string {
    regions := []string{"NA", "EU", "AP"}
    return regions[i%3]
}