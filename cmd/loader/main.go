// cmd/loader/main.go
// Chargement des données CSV vers LevelDB

package main

import (
    "encoding/csv"
    "flag"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"
    
    "leveldb-tp/pkg/leveldb"
)

var (
    csvFiles = map[string]string{
        "orders":   "orders.csv",
        "products": "products.csv",
        "sellers":  "sellers.csv",
        "leads_qualified": "leads_qualified.csv",
        "leads_closed": "leads_closed.csv",
    }
)

func main() {
    var (
        node    = flag.String("node", "node1", "Nœud cible (node1 ou node2)")
        csvDir  = flag.String("csv", "./data", "Dossier contenant les CSV")
        limit   = flag.Int("limit", 0, "Limiter le nombre de lignes (0 = tout)")
        offset  = flag.Int("offset", 0, "Décalage de départ dans le fichier (0 = début)")
        verbose = flag.Bool("verbose", false, "Mode verbose")
    )
    flag.Parse()
    
    log.Println("Chargement CSV vers LevelDB")
    log.Println("===========================")
    log.Printf("Nœud cible: %s", *node)
    log.Printf("Dossier CSV: %s", *csvDir)
    if *limit > 0 {
        log.Printf("Limite: %d lignes", *limit)
    }
    if *offset > 0 {
        log.Printf("Offset: %d lignes", *offset)
    }
    
    // Ouvrir client LevelDB
    nodePath := filepath.Join("leveldb-stores", *node)
    client, err := leveldb.NewClient(nodePath)
    if err != nil {
        log.Fatalf("Erreur ouverture LevelDB: %v", err)
    }
    defer client.Close()
    
    // Charger chaque type de données
    stats := make(map[string]int)
    
    // 1. Charger orders
    if count, err := loadOrders(client, *csvDir, *limit, *offset, *verbose); err == nil {
        stats["orders"] = count
    } else {
        log.Printf("Erreur chargement orders: %v", err)
    }
    
    // 2. Charger products
    if count, err := loadProducts(client, *csvDir, *limit, *offset, *verbose); err == nil {
        stats["products"] = count
    } else {
        log.Printf("Erreur chargement products: %v", err)
    }
    
    // 3. Charger sellers
    if count, err := loadSellers(client, *csvDir, *limit, *offset, *verbose); err == nil {
        stats["sellers"] = count
    } else {
        log.Printf("Erreur chargement sellers: %v", err)
    }
    
    // 4. Charger leads
    if count, err := loadLeads(client, *csvDir, *limit, *offset, *verbose); err == nil {
        stats["leads"] = count
    } else {
        log.Printf("Erreur chargement leads: %v", err)
    }
    
    // Afficher statistiques
    log.Println("\n=== Statistiques de chargement ===")
    total := 0
    for typ, count := range stats {
        log.Printf("%-12s: %d documents", typ, count)
        total += count
    }
    log.Printf("%-12s: %d documents", "TOTAL", total)
    
    log.Println("\n✓ Chargement terminé avec succès!")
}

// loadOrders charge les commandes depuis orders.csv
func loadOrders(client *leveldb.Client, csvDir string, limit int, offset int, verbose bool) (int, error) {
    log.Println("\nChargement des commandes...")
    
    file, err := os.Open(filepath.Join(csvDir, "orders.csv"))
    if err != nil {
        return 0, fmt.Errorf("erreur ouverture orders.csv: %v", err)
    }
    defer file.Close()
    
    reader := csv.NewReader(file)
    records, err := reader.ReadAll()
    if err != nil {
        return 0, fmt.Errorf("erreur lecture CSV: %v", err)
    }
    
    if len(records) == 0 {
        return 0, fmt.Errorf("fichier vide")
    }
    
    // Header: order_id,customer_id,order_status,order_purchase_timestamp,...
    headers := records[0]
    
    entries := make(map[string]interface{})
    count := 0
    
    for i, record := range records[1:] {
        // Appliquer l'offset
        if i < offset {
            continue
        }
        
        // Appliquer la limite
        if limit > 0 && (i - offset) >= limit {
            break
        }
        
        // Créer map des données
        data := make(map[string]string)
        for j, value := range record {
            if j < len(headers) {
                data[headers[j]] = value
            }
        }
        
        // Clé: order:<order_id>
        orderID := data["order_id"]
        key := fmt.Sprintf("order:%s", orderID)
        
        // Créer document
        doc := map[string]interface{}{
            "ledger_type": "commercial_transaction",
            "order_id":    orderID,
            "customer_id": data["customer_id"],
            "status":      data["order_status"],
            "purchase_timestamp": data["order_purchase_timestamp"],
        }
        
        entries[key] = doc
        count++
        
        if verbose && count%1000 == 0 {
            log.Printf("  Préparé %d commandes...", count)
        }
    }
    
    // Insertion batch
    log.Printf("Insertion de %d commandes...", count)
    start := time.Now()
    if err := client.BatchInsert(entries); err != nil {
        return 0, err
    }
    elapsed := time.Since(start)
    
    log.Printf("✓ %d commandes insérées en %v", count, elapsed)
    return count, nil
}

// loadProducts charge les produits depuis products.csv
func loadProducts(client *leveldb.Client, csvDir string, limit int, offset int, verbose bool) (int, error) {
    log.Println("\nChargement des produits...")
    
    file, err := os.Open(filepath.Join(csvDir, "products.csv"))
    if err != nil {
        return 0, fmt.Errorf("erreur ouverture products.csv: %v", err)
    }
    defer file.Close()
    
    reader := csv.NewReader(file)
    records, err := reader.ReadAll()
    if err != nil {
        return 0, fmt.Errorf("erreur lecture CSV: %v", err)
    }
    
    if len(records) == 0 {
        return 0, fmt.Errorf("fichier vide")
    }
    
    headers := records[0]
    entries := make(map[string]interface{})
    count := 0
    
    for i, record := range records[1:] {
        // Appliquer l'offset
        if i < offset {
            continue
        }
        
        // Appliquer la limite
        if limit > 0 && (i - offset) >= limit {
            break
        }
        
        data := make(map[string]string)
        for j, value := range record {
            if j < len(headers) {
                data[headers[j]] = value
            }
        }
        
        productID := data["product_id"]
        key := fmt.Sprintf("product:%s", productID)
        
        doc := map[string]interface{}{
            "ledger_type":  "product_definition",
            "product_id":   productID,
            "category":     data["product_category_name"],
            "weight_g":     data["product_weight_g"],
            "length_cm":    data["product_length_cm"],
            "height_cm":    data["product_height_cm"],
            "width_cm":     data["product_width_cm"],
        }
        
        entries[key] = doc
        count++
        
        if verbose && count%1000 == 0 {
            log.Printf("  Préparé %d produits...", count)
        }
    }
    
    log.Printf("Insertion de %d produits...", count)
    start := time.Now()
    if err := client.BatchInsert(entries); err != nil {
        return 0, err
    }
    elapsed := time.Since(start)
    
    log.Printf("✓ %d produits insérés en %v", count, elapsed)
    return count, nil
}

// loadSellers charge les vendeurs depuis sellers.csv
func loadSellers(client *leveldb.Client, csvDir string, limit int, offset int, verbose bool) (int, error) {
    log.Println("\nChargement des vendeurs...")
    
    file, err := os.Open(filepath.Join(csvDir, "sellers.csv"))
    if err != nil {
        return 0, fmt.Errorf("erreur ouverture sellers.csv: %v", err)
    }
    defer file.Close()
    
    reader := csv.NewReader(file)
    records, err := reader.ReadAll()
    if err != nil {
        return 0, fmt.Errorf("erreur lecture CSV: %v", err)
    }
    
    if len(records) == 0 {
        return 0, fmt.Errorf("fichier vide")
    }
    
    headers := records[0]
    entries := make(map[string]interface{})
    count := 0
    
    for i, record := range records[1:] {
        // Appliquer l'offset
        if i < offset {
            continue
        }
        
        // Appliquer la limite
        if limit > 0 && (i - offset) >= limit {
            break
        }
        
        data := make(map[string]string)
        for j, value := range record {
            if j < len(headers) {
                data[headers[j]] = value
            }
        }
        
        sellerID := data["seller_id"]
        key := fmt.Sprintf("seller:%s", sellerID)
        
        doc := map[string]interface{}{
            "ledger_type":    "partner_registry",
            "seller_id":      sellerID,
            "city":           data["seller_city"],
            "state":          data["seller_state"],
            "zip_code_prefix": data["seller_zip_code_prefix"],
            "certification_status": "active",
        }
        
        entries[key] = doc
        count++
    }
    
    log.Printf("Insertion de %d vendeurs...", count)
    start := time.Now()
    if err := client.BatchInsert(entries); err != nil {
        return 0, err
    }
    elapsed := time.Since(start)
    
    log.Printf("✓ %d vendeurs insérés en %v", count, elapsed)
    return count, nil
}

// loadLeads charge les prospects depuis leads_qualified.csv et leads_closed.csv
func loadLeads(client *leveldb.Client, csvDir string, limit int, offset int, verbose bool) (int, error) {
    log.Println("\nChargement des prospects...")
    
    // Charger leads_qualified
    qualFile, err := os.Open(filepath.Join(csvDir, "leads_qualified.csv"))
    if err != nil {
        return 0, fmt.Errorf("erreur ouverture leads_qualified.csv: %v", err)
    }
    defer qualFile.Close()
    
    reader := csv.NewReader(qualFile)
    records, err := reader.ReadAll()
    if err != nil {
        return 0, fmt.Errorf("erreur lecture CSV: %v", err)
    }
    
    if len(records) == 0 {
        return 0, fmt.Errorf("fichier vide")
    }
    
    headers := records[0]
    entries := make(map[string]interface{})
    count := 0
    
    for i, record := range records[1:] {
        // Appliquer l'offset
        if i < offset {
            continue
        }
        
        // Appliquer la limite
        if limit > 0 && (i - offset) >= limit {
            break
        }
        
        data := make(map[string]string)
        for j, value := range record {
            if j < len(headers) {
                data[headers[j]] = value
            }
        }
        
        mqlID := data["mql_id"]
        key := fmt.Sprintf("lead:%s", mqlID)
        
        doc := map[string]interface{}{
            "ledger_type":         "sales_pipeline",
            "mql_id":              mqlID,
            "pipeline_stage":      "qualified",
            "first_contact_date":  data["first_contact_date"],
            "landing_page_id":     data["landing_page_id"],
            "origin":              data["origin"],
        }
        
        entries[key] = doc
        count++
        
        if verbose && count%1000 == 0 {
            log.Printf("  Préparé %d prospects...", count)
        }
    }
    
    log.Printf("Insertion de %d prospects...", count)
    start := time.Now()
    if err := client.BatchInsert(entries); err != nil {
        return 0, err
    }
    elapsed := time.Since(start)
    
    log.Printf("✓ %d prospects insérés en %v", count, elapsed)
    return count, nil
}