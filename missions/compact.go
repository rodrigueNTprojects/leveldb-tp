package main

import (
    "fmt"
    "os"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    "github.com/syndtr/goleveldb/leveldb/util" 
)

func main() {
    fmt.Println("=== Compaction manuelle LevelDB ===\n")
    
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run compact.go <node_name>")
        os.Exit(1)
    }
    
    nodeName := os.Args[1]
    dbPath := fmt.Sprintf("leveldb-stores/%s", nodeName)
    
    fmt.Printf("Compaction de %s...\n", dbPath)
    
    // Ouvrir la base
    opts := &opt.Options{
        CompactionTableSize: 2 * 1024 * 1024, // 2MB (plus petit pour forcer la compaction)
        WriteBuffer:         1 * 1024 * 1024, // 1MB
    }
    
    db, err := leveldb.OpenFile(dbPath, opts)
    if err != nil {
        fmt.Printf("Erreur ouverture DB: %v\n", err)
        os.Exit(1)
    }
    defer db.Close()
    
    // Forcer la compaction complète
    fmt.Println("Lancement compaction...")
    
    // Option 1: Utiliser un Range vide mais pas nil
    rangeObj := &util.Range{} // Range vide qui représente tout l'espace des clés
    db.CompactRange(*rangeObj) // Passer l'objet (pas le pointeur)
      
    fmt.Println("Compaction terminée!")
    fmt.Println("\n✓ Compaction réussie!")
}