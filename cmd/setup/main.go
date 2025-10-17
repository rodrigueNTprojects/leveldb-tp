// cmd/setup/main.go
// Configuration et initialisation des nœuds LevelDB

package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "strings"
    "time"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    tpleveldb "leveldb-tp/pkg/leveldb" // Alias pour éviter le conflit
)

func main() {
    var (
        test     = flag.Bool("test", false, "Exécuter tests de base")
        snapshot = flag.String("snapshot", "", "Créer snapshot d'un nœud (node1|node2)")
        restore  = flag.String("restore", "", "Restaurer snapshot: node,path")
        clean    = flag.Bool("clean", false, "Nettoyer les nœuds existants")
    )
    flag.Parse()
    
    log.SetFlags(log.LstdFlags)
    
    log.Println("Configuration des nœuds LevelDB")
    log.Println("===============================")
    log.Println()
    
    // Mode test
    if *test {
        runTests()
        return
    }
    
    // Mode snapshot
    if *snapshot != "" {
        createSnapshot(*snapshot)
        return
    }
    
    // Mode restore
    if *restore != "" {
        parts := strings.Split(*restore, ",")
        if len(parts) != 2 {
            log.Fatal("Format restore: node,path (ex: node1,snapshots/node1_123456)")
        }
        restoreSnapshot(parts[0], parts[1])
        return
    }
    
    // Mode nettoyage
    if *clean {
        cleanNodes()
        return
    }
    
    // Configuration normale
    setupNodes()
}

func setupNodes() {
    nodes := []string{"node1", "node2"}
    
    for _, node := range nodes {
        nodePath := filepath.Join("leveldb-stores", node)
        
        log.Printf("Configuration %s...", node)
        
        // Créer dossier
        if err := os.MkdirAll(nodePath, 0755); err != nil {
            log.Fatalf("Erreur création dossier %s: %v", nodePath, err)
        }
        
        // Options LevelDB optimisées
        opts := &opt.Options{
            WriteBuffer:   4 * 1024 * 1024,    // 4MB buffer
            BlockSize:     4096,                // 4KB blocks
            Compression:   opt.SnappyCompression, // Compression
            // MaxOpenFiles n'existe pas dans cette version, on le supprime
        }
        
        // Ouvrir/Créer base LevelDB
        ldb, err := leveldb.OpenFile(nodePath, opts) // Utiliser OpenFile et non Open
        if err != nil {
            log.Printf("Avertissement ouverture %s: %v", nodePath, err)
            continue
        }
        
        // Créer métadonnées de configuration
        config := map[string]interface{}{
            "node":         node,
            "created_at":   time.Now().Format(time.RFC3339),
            "version":      "1.0",
            "description":  fmt.Sprintf("LevelDB node %s for e-commerce ledgers", node),
        }
        
        configJSON, _ := json.Marshal(config)
        if err := ldb.Put([]byte("_config"), configJSON, nil); err != nil { // Utiliser Put au lieu de Set
            log.Printf("Erreur configuration %s: %v", node, err)
        }
        
        // Créer les namespaces (métadata pour chaque type de registre)
        namespaces := []struct {
            Name        string
            Description string
            Type        string
        }{
            {"orders", "Commercial transactions ledger", "commercial_transaction"},
            {"products", "Product definitions ledger", "product_definition"},
            {"sellers", "Partner registry ledger", "partner_registry"},
            {"leads", "Sales pipeline ledger", "sales_pipeline"},
        }
        
        log.Printf("  Configuration des namespaces:")
        for _, ns := range namespaces {
            key := []byte(fmt.Sprintf("_namespace:%s", ns.Name))
            value := map[string]interface{}{
                "name":        ns.Name,
                "description": ns.Description,
                "type":        ns.Type,
                "created_at":  time.Now().Format(time.RFC3339),
                "count":       0,
            }
            
            valueJSON, _ := json.Marshal(value)
            if err := ldb.Put(key, valueJSON, nil); err != nil { // Utiliser Put au lieu de Set
                log.Printf("    Erreur namespace %s: %v", ns.Name, err)
            } else {
                log.Printf("    ✓ %s (%s)", ns.Name, ns.Type)
            }
        }
        
        ldb.Close()
        log.Printf("  ✓ %s configuré", node)
        log.Println()
    }
    
    log.Println("=============================")
    log.Println("✓ Nœuds initialisés avec succès!")
    log.Println()
    log.Println("Prochaines étapes:")
    log.Println("  1. Charger les données:   ./bin/loader -node node1 -csv ./data")
    log.Println("  2. Répliquer vers node2:  ./bin/replicator -action export -source node1 -output export.json")
    log.Println("                            ./bin/replicator -action import -target node2 -input export.json")
    log.Println("  3. Benchmark:             ./bin/benchmark -db both -compare")
}

func runTests() {
    log.Println("Exécution des tests de base...")
    log.Println()
    
    testNode := filepath.Join("leveldb-stores", "node1")
    
    // Vérifier que le nœud existe
    if _, err := os.Stat(testNode); os.IsNotExist(err) {
        log.Fatal("Node1 n'existe pas. Exécuter d'abord: ./setup (sans -test)")
    }
    
    // Ouvrir node1 pour test
    ldb, err := leveldb.OpenFile(testNode, nil) // Utiliser OpenFile et non Open
    if err != nil {
        log.Fatalf("Erreur ouverture: %v", err)
    }
    defer ldb.Close()
    
    log.Println("Test 1: PUT - Insertion de données")
    log.Println("-----------------------------------")
    
    testKey := []byte("test:order:001")
    testData := map[string]interface{}{
        "order_id": "test_001",
        "amount":   150.75,
        "region":   "NA",
        "status":   "delivered",
        "timestamp": time.Now().Unix(),
    }
    
    testJSON, _ := json.Marshal(testData)
    log.Printf("Clé:     %s", testKey)
    log.Printf("Données: %s", testJSON)
    
    if err := ldb.Put(testKey, testJSON, nil); err != nil { // Utiliser Put au lieu de Set
        log.Fatalf("⌧ Erreur PUT: %v", err)
    }
    log.Println("✓ PUT réussi")
    log.Println()
    
    log.Println("Test 2: GET - Lecture de données")
    log.Println("---------------------------------")
    
    value, err := ldb.Get(testKey, nil)
    if err != nil {
        log.Fatalf("⌧ Erreur GET: %v", err)
    }
    
    log.Printf("Valeur récupérée: %s", value)
    
    // Vérifier que les données correspondent
    var retrieved map[string]interface{}
    json.Unmarshal(value, &retrieved)
    
    if retrieved["order_id"] != testData["order_id"] {
        log.Fatal("⌧ Données ne correspondent pas!")
    }
    log.Println("✓ GET réussi - Données correctes")
    log.Println()
    
    log.Println("Test 3: Vérification d'intégrité avec hash")
    log.Println("------------------------------------------")
    
    // Utiliser le client personnalisé
    client, err := tpleveldb.NewClient(testNode)
    if err != nil {
        log.Fatalf("⌧ Erreur création client: %v", err)
    }
    defer client.Close()
    
    hashKey := "test:hashed:001"
    if err := client.Put(hashKey, testData); err != nil {
        log.Fatalf("⌧ Erreur PUT avec hash: %v", err)
    }
    
    log.Printf("Clé avec hash: %s", hashKey)
    
    // Vérifier intégrité
    valid, err := client.VerifyIntegrity(hashKey)
    if err != nil {
        log.Fatalf("⌧ Erreur vérification: %v", err)
    }
    
    if !valid {
        log.Fatal("⌧ Hash invalide!")
    }
    log.Println("✓ Intégrité vérifiée - Hash valide")
    log.Println()
    
    log.Println("Test 4: DELETE - Suppression")
    log.Println("-----------------------------")
    
    log.Printf("Suppression: %s", testKey)
    if err := ldb.Delete(testKey, nil); err != nil {
        log.Fatalf("⌧ Erreur DELETE: %v", err)
    }
    
    // Vérifier que c'est supprimé
    _, err = ldb.Get(testKey, nil)
    if err == nil {
        log.Fatal("⌧ Clé devrait être supprimée!")
    }
    log.Println("✓ DELETE réussi - Clé supprimée")
    log.Println()
    
    log.Println("Test 5: Batch atomique")
    log.Println("----------------------")
    
    batchEntries := make(map[string]interface{})
    for i := 0; i < 10; i++ {
        key := fmt.Sprintf("test:batch:%03d", i)
        batchEntries[key] = map[string]interface{}{
            "id":    i,
            "value": fmt.Sprintf("batch_value_%d", i),
        }
    }
    
    log.Printf("Insertion batch de %d entrées...", len(batchEntries))
    start := time.Now()
    
    if err := client.BatchInsert(batchEntries); err != nil {
        log.Fatalf("⌧ Erreur batch: %v", err)
    }
    
    elapsed := time.Since(start)
    log.Printf("✓ Batch réussi en %v", elapsed)
    log.Println()
    
    log.Println("=============================")
    log.Println("✓ Tous les tests réussis!")
    log.Println()
    log.Println("LevelDB est correctement configuré et fonctionnel.")
}

func createSnapshot(nodeName string) {
    log.Printf("Création snapshot de %s...", nodeName)
    log.Println()
    
    sourcePath := filepath.Join("leveldb-stores", nodeName)
    
    // Vérifier que le nœud existe
    if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
        log.Fatalf("Nœud %s n'existe pas", nodeName)
    }
    
    // Créer nom de snapshot avec timestamp
    timestamp := time.Now().Unix()
    snapshotName := fmt.Sprintf("%s_%d", nodeName, timestamp)
    snapshotPath := filepath.Join("snapshots", snapshotName)
    
    // Créer dossier snapshots
    if err := os.MkdirAll("snapshots", 0755); err != nil {
        log.Fatalf("Erreur création dossier snapshots: %v", err)
    }
    
    log.Printf("Source:      %s", sourcePath)
    log.Printf("Destination: %s", snapshotPath)
    log.Println()
    
    // Copier récursivement le dossier LevelDB
    log.Println("Copie des fichiers...")
    if err := copyDir(sourcePath, snapshotPath); err != nil {
        log.Fatalf("Erreur copie: %v", err)
    }
    
    // Créer fichier de métadonnées
    metadata := map[string]interface{}{
        "node":       nodeName,
        "created_at": time.Now().Format(time.RFC3339),
        "timestamp":  timestamp,
        "source":     sourcePath,
    }
    
    metadataJSON, _ := json.MarshalIndent(metadata, "", "  ")
    metadataFile := filepath.Join(snapshotPath, "snapshot.json")
    os.WriteFile(metadataFile, metadataJSON, 0644)
    
    log.Println("✓ Snapshot créé avec succès!")
    log.Printf("\nSnapshot: %s", snapshotPath)
    log.Println("\nPour restaurer:")
    log.Printf("  ./bin/setup -restore %s,%s\n", nodeName, snapshotPath)
}

func restoreSnapshot(nodeName, snapshotPath string) {
    log.Printf("Restauration de %s depuis %s...", nodeName, snapshotPath)
    log.Println()
    
    // Vérifier que le snapshot existe
    if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
        log.Fatalf("Snapshot %s n'existe pas", snapshotPath)
    }
    
    targetPath := filepath.Join("leveldb-stores", nodeName)
    
    // Sauvegarder l'ancien nœud si existe
    if _, err := os.Stat(targetPath); err == nil {
        backupPath := targetPath + "_backup_" + fmt.Sprint(time.Now().Unix())
        log.Printf("Sauvegarde ancien nœud: %s", backupPath)
        os.Rename(targetPath, backupPath)
    }
    
    // Copier snapshot vers nœud
    log.Println("Restauration des fichiers...")
    if err := copyDir(snapshotPath, targetPath); err != nil {
        log.Fatalf("Erreur restauration: %v", err)
    }
    
    log.Println("✓ Nœud restauré avec succès!")
    log.Printf("\nNœud: %s", targetPath)
}

func cleanNodes() {
    log.Println("Nettoyage des nœuds LevelDB...")
    log.Println()
    
    storesPath := "leveldb-stores"
    
    if _, err := os.Stat(storesPath); os.IsNotExist(err) {
        log.Println("Aucun nœud à nettoyer")
        return
    }
    
    log.Printf("Suppression: %s", storesPath)
    if err := os.RemoveAll(storesPath); err != nil {
        log.Fatalf("Erreur suppression: %v", err)
    }
    
    log.Println("✓ Nettoyage terminé")
    log.Println("\nPour recréer les nœuds:")
    log.Println("  ./bin/setup")
}

// Fonction utilitaire pour copier un dossier récursivement
func copyDir(src, dst string) error {
    return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        // Calculer chemin relatif
        relPath, err := filepath.Rel(src, path)
        if err != nil {
            return err
        }
        
        dstPath := filepath.Join(dst, relPath)
        
        if info.IsDir() {
            // Créer dossier
            return os.MkdirAll(dstPath, info.Mode())
        }
        
        // Copier fichier
        return copyFile(path, dstPath)
    })
}

func copyFile(src, dst string) error {
    sourceFile, err := os.Open(src)
    if err != nil {
        return err
    }
    defer sourceFile.Close()
    
    // Créer dossier parent si nécessaire
    os.MkdirAll(filepath.Dir(dst), 0755)
    
    destFile, err := os.Create(dst)
    if err != nil {
        return err
    }
    defer destFile.Close()
    
    _, err = io.Copy(destFile, sourceFile)
    return err
}