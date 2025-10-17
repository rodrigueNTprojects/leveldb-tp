// ============================================================
// missions/mission_0_4_modify.go
// Script fourni pour modifier des données (Mission 0.4)
// ============================================================

package main
import (
    "fmt"
    "leveldb-tp/pkg/leveldb"
	//"encoding/json"
)

func mission_0_4_modify() {
    fmt.Println("=== Mission 0.4 : Modification de données ===\n")
    fmt.Println("Ce script modifie 10 documents pour tester le snapshot")
    
    // Ouvrir la base
    client, err := leveldb.NewClient("leveldb-stores/node1")
    if err != nil {
        panic(err)
    }
    defer client.Close()
    
    // Modifier 10 documents pour simuler un changement
    for i := 0; i < 10; i++ {
        key := fmt.Sprintf("order:%05d", i)
        
        // Nouveau document avec status "MODIFIÉ"
        doc := map[string]interface{}{
            "order_id": key,
            "status":   "MODIFIÉ",
            "amount":   999.99,
        }
        
        if err := client.Put(key, doc); err != nil {
            fmt.Printf("Erreur modification %s: %v\n", key, err)
        } else {
            fmt.Printf("Document %s modifié\n", key)
        }
    }
    
    fmt.Println("\n✓ 10 documents modifiés!")
}

func main() {
    mission_0_4_modify()
}