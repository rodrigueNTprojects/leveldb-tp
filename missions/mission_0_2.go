// ============================================================
// missions/mission_0_2.go
// Mission 0.2 : PUT/GET avec intégrité (3 lignes à compléter)
// ============================================================

package main

import (
    "fmt"
    "leveldb-tp/pkg/leveldb"
)

func mission_0_2() {
    fmt.Println("=== Mission 0.2 : PUT/GET avec intégrité ===\n")
    
    // Client déjà initialisé
    client, err := leveldb.NewClient("leveldb-stores/node1")
    if err != nil {
        panic(err)
    }
    defer client.Close()
    
    // Document à insérer
    order := map[string]interface{}{
        "order_id": "abc123",
        "amount":   150.50,
        "status":   "delivered",
    }
    
    fmt.Println("TODO : Complétez les 3 lignes suivantes")
    fmt.Println("Indices :")
    fmt.Println("  - Ligne 1: client.Put(clé, valeur)")
    fmt.Println("  - Ligne 2: client.Get(clé)")
    fmt.Println("  - Ligne 3: client.VerifyIntegrity(clé)")
    fmt.Println()
    
    // ============ COMPLÉTEZ ICI (3 lignes) ============
    // TODO 1: Insérer le document avec la clé "order:abc123"
    
    
    // TODO 2: Lire le document que vous venez d'insérer
    
    
    // TODO 3: Vérifier l'intégrité du hash SHA-256

      
    
    // ===================================================
    
    fmt.Println("\n✓ Mission 0.2 réussie!")
    fmt.Println("\nPour tester:")
    fmt.Println("  go run missions/mission_0_2.go")
}

func main() {
    mission_0_2()
}