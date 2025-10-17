package main

import (
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
    "runtime"
    "leveldb-tp/pkg/leveldb"
)

func main() {
    fmt.Println("=== Test de conflit de réplication ===")
    fmt.Println()
    
    // 1. Créer un document sur node1
    fmt.Println("1. Modification sur node1")
    client1, err := leveldb.NewClient("leveldb-stores/node1")
    if err != nil {
        fmt.Printf("Erreur ouverture node1: %v\n", err)
        return
    }
    
    doc1 := map[string]interface{}{
        "order_id": "order:00001",
        "amount":   1000.0,
        "version":  "NODE1",
    }
    client1.Put("order:00001", doc1)
    client1.Close()
    fmt.Println("   Document order:00001 modifié: amount = 1000.0")
    fmt.Println()
    
    // 2. Créer un document DIFFÉRENT sur node2 avec la même clé
    fmt.Println("2. Modification sur node2")
    client2, err := leveldb.NewClient("leveldb-stores/node2")
    if err != nil {
        fmt.Printf("Erreur ouverture node2: %v\n", err)
        return
    }
    
    doc2 := map[string]interface{}{
        "order_id": "order:00001",
        "amount":   2000.0,
        "version":  "NODE2",
    }
    client2.Put("order:00001", doc2)
    client2.Close()
    fmt.Println("   Document order:00001 modifié: amount = 2000.0")
    fmt.Println()
    
    // 3. Afficher état AVANT réplication
    fmt.Println("3. État AVANT réplication:")
    
    client1, _ = leveldb.NewClient("leveldb-stores/node1")
    entry1, _ := client1.Get("order:00001")
    if entry1 != nil {
        var data map[string]interface{}
        if err := json.Unmarshal(entry1.Data, &data); err == nil {
            if amount, ok := data["amount"].(float64); ok {
                fmt.Printf("   node1: order:00001 = %.1f\n", amount)
            }
        }
    }
    client1.Close()
    
    client2, _ = leveldb.NewClient("leveldb-stores/node2")
    entry2, _ := client2.Get("order:00001")
    if entry2 != nil {
        var data map[string]interface{}
        if err := json.Unmarshal(entry2.Data, &data); err == nil {
            if amount, ok := data["amount"].(float64); ok {
                fmt.Printf("   node2: order:00001 = %.1f\n", amount)
            }
        }
    }
    client2.Close()
    fmt.Println()
    
    // 4. Exécuter la réplication automatiquement
    fmt.Println("4. Réplication node1 → node2")
    
    // Déterminer l'exécutable selon l'OS
    replicatorCmd := "./bin/replicator"
    if runtime.GOOS == "windows" {
        replicatorCmd = ".\\bin\\replicator.exe"
    }
    
    // Export node1
    exportCmd := exec.Command(replicatorCmd, "-action", "export", "-source", "node1", "-output", "export.json")
    exportCmd.Stdout = nil // Supprimer output
    exportCmd.Stderr = nil
    if err := exportCmd.Run(); err != nil {
        fmt.Printf("   Erreur export: %v\n", err)
        fmt.Println("\n   Exécutez manuellement:")
        fmt.Printf("   %s -action export -source node1 -output export.json\n", replicatorCmd)
        return
    }
    
    // Import vers node2
    importCmd := exec.Command(replicatorCmd, "-action", "import", "-target", "node2", "-input", "export.json")
    importCmd.Stdout = nil
    importCmd.Stderr = nil
    if err := importCmd.Run(); err != nil {
        fmt.Printf("   Erreur import: %v\n", err)
        fmt.Println("\n   Exécutez manuellement:")
        fmt.Printf("   %s -action import -target node2 -input export.json\n", replicatorCmd)
        return
    }
    
    fmt.Println("   ✓ Export/Import terminé")
    fmt.Println()
    
    // 5. Afficher état APRÈS réplication
    fmt.Println("5. État APRÈS réplication:")
    
    client1, _ = leveldb.NewClient("leveldb-stores/node1")
    entry1, _ = client1.Get("order:00001")
    if entry1 != nil {
        var data map[string]interface{}
        if err := json.Unmarshal(entry1.Data, &data); err == nil {
            if amount, ok := data["amount"].(float64); ok {
                fmt.Printf("   node1: order:00001 = %.1f\n", amount)
            }
        }
    }
    client1.Close()
    
    client2, _ = leveldb.NewClient("leveldb-stores/node2")
    entry2, _ = client2.Get("order:00001")
    if entry2 != nil {
        var data map[string]interface{}
        if err := json.Unmarshal(entry2.Data, &data); err == nil {
            if amount, ok := data["amount"].(float64); ok {
                fmt.Printf("   node2: order:00001 = %.1f\n", amount)
            }
        }
    }
    client2.Close()
    fmt.Println()
    
    // 6. Conclusion
    fmt.Println("❌ PROBLÈME: quel est le problème avec les modifications effectué sur le noeud 2!")
    //fmt.Println("   Last-write-wins sans détection de conflit.")
    fmt.Println()
    
    // Nettoyer le fichier export.json
    os.Remove("export.json")
    
    // fmt.Println("💡 Avec CouchDB:")
    // fmt.Println("   • Détecterait le conflit via _rev")
    // fmt.Println("   • Garderait les 2 versions")
    // fmt.Println("   • Permettrait résolution manuelle")
}