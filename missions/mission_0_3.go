// ============================================================
// missions/mission_0_3.go
// Mission 0.3 : Batch atomique (5 lignes à compléter)
// ============================================================

package main
import (
    "time"
    "fmt"
    "strings"
    "leveldb-tp/pkg/leveldb"
)

func mission_0_3() {
    fmt.Println("=== Mission 0.3 : Batch atomique ===\n")
    
    client, err := leveldb.NewClient("leveldb-stores/node1")
    if err != nil {
        panic(err)
    }
    defer client.Close()
    
    // Préparer 100 commandes
    entries := make(map[string]interface{})
    
    fmt.Println("TODO : Complétez la boucle for (5 lignes)")
    fmt.Println("Objectif : Créer 100 entrées avec format :")
    fmt.Println("  - Clé: 'order:00000', 'order:00001', ..., 'order:00099'")
    fmt.Println("  - Valeur: {order_id: clé, amount: 100.0 + i}")
    fmt.Println()
    
    // ============ COMPLÉTEZ ICI (5 lignes) ============
    // TODO : Boucle pour créer 100 entrées
    // Indice : Utilisez fmt.Sprintf("order:%05d", i) pour la clé
    
    for i := 0; i < 100; i++ {
        // Ligne 1: créer la clé
        // Ligne 2: créer le document
        // Ligne 3: ajouter au map entries
    }
    // ===================================================
    
    // Insertion atomique
    fmt.Printf("Insertion batch de %d documents...\n", len(entries))
    start := time.Now()
    err = client.BatchInsert(entries)
    if err != nil {
        panic(err)
    }
    elapsed := time.Since(start)
    
    fmt.Printf("100 documents insérés en %v\n", elapsed)
    fmt.Println("\n✓ Mission 0.3 réussie!")
    
    // Vérifier quelques documents
    fmt.Println("\nVérification des documents insérés:")
    for i := 0; i < 3; i++ {
        key := fmt.Sprintf("order:%05d", i)
        entry, err := client.Get(key)
        if err != nil {
            fmt.Printf("Document %s non trouvé\n", key)
            continue
        }
        fmt.Printf("Document %s trouvé ✓\n", key)
        fmt.Printf("  Clé : %s\n", key)
        fmt.Printf("  Valeur: %s\n", entry)
        fmt.Println("  " + strings.Repeat("-", 50))
    }
}
func main() {
    mission_0_3()
}
