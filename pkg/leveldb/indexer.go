// pkg/leveldb/indexer.go
// Implémentation d'indexation secondaire pour LevelDB

package leveldb

import (
    "encoding/json"
    "fmt"
    "strings"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/util"
)

type Indexer struct {
    db *leveldb.DB
}

func NewIndexer(db *leveldb.DB) *Indexer {
    return &Indexer{db: db}
}

func (idx *Indexer) CreateIndex(recordType, field, value, primaryKey string) error {
    normalizedValue := strings.ToLower(strings.TrimSpace(value))
    
    indexKey := fmt.Sprintf("idx:%s:%s:%s:%s", 
        recordType, field, normalizedValue, primaryKey)
    
    // CORRECTION: Set → Put
    return idx.db.Put([]byte(indexKey), []byte(primaryKey), nil)
}

func (idx *Indexer) SearchByIndex(recordType, field, value string) ([]string, error) {
    normalizedValue := strings.ToLower(strings.TrimSpace(value))
    
    prefix := fmt.Sprintf("idx:%s:%s:%s:", recordType, field, normalizedValue)
    
    var results []string
    
    // CORRECTION: Find → NewIterator avec util.BytesPrefix
    iter := idx.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
    defer iter.Release()
    
    for iter.Next() {
        key := string(iter.Key())
        
        if !strings.HasPrefix(key, prefix) {
            break
        }
        
        primaryKey := string(iter.Value())
        results = append(results, primaryKey)
    }
    
    if err := iter.Error(); err != nil {
        return nil, fmt.Errorf("erreur itération: %v", err)
    }
    
    return results, nil
}

func (idx *Indexer) GetByIndex(recordType, field, value string) ([]Entry, error) {
    primaryKeys, err := idx.SearchByIndex(recordType, field, value)
    if err != nil {
        return nil, err
    }
    
    var entries []Entry
    
    for _, pk := range primaryKeys {
        valueBytes, err := idx.db.Get([]byte(pk), nil)
        if err != nil {
            continue
        }
        
        var entry Entry
        if err := json.Unmarshal(valueBytes, &entry); err != nil {
            continue
        }
        
        entries = append(entries, entry)
    }
    
    return entries, nil
}

func (idx *Indexer) CountByIndex(recordType, field, value string) (int, error) {
    results, err := idx.SearchByIndex(recordType, field, value)
    if err != nil {
        return 0, err
    }
    return len(results), nil
}

func (idx *Indexer) UpdateIndexes(recordType, primaryKey string, oldData, newData map[string]interface{}) error {
    if oldData != nil {
        for field, value := range oldData {
            if !isIndexableField(field) {
                continue
            }
            
            valueStr := fmt.Sprintf("%v", value)
            normalizedValue := strings.ToLower(strings.TrimSpace(valueStr))
            
            oldIndexKey := fmt.Sprintf("idx:%s:%s:%s:%s", 
                recordType, field, normalizedValue, primaryKey)
            
            idx.db.Delete([]byte(oldIndexKey), nil)
        }
    }
    
    if newData != nil {
        for field, value := range newData {
            if !isIndexableField(field) {
                continue
            }
            
            valueStr := fmt.Sprintf("%v", value)
            if err := idx.CreateIndex(recordType, field, valueStr, primaryKey); err != nil {
                return fmt.Errorf("erreur création index %s: %v", field, err)
            }
        }
    }
    
    return nil
}

func (idx *Indexer) DeleteIndexes(recordType, primaryKey string, data map[string]interface{}) error {
    if data == nil {
        return nil
    }
    
    for field, value := range data {
        if !isIndexableField(field) {
            continue
        }
        
        valueStr := fmt.Sprintf("%v", value)
        normalizedValue := strings.ToLower(strings.TrimSpace(valueStr))
        
        indexKey := fmt.Sprintf("idx:%s:%s:%s:%s", 
            recordType, field, normalizedValue, primaryKey)
        
        if err := idx.db.Delete([]byte(indexKey), nil); err != nil {
            return fmt.Errorf("erreur suppression index %s: %v", field, err)
        }
    }
    
    return nil
}

func (idx *Indexer) ListIndexes(recordType, field string) (map[string]int, error) {
    prefix := fmt.Sprintf("idx:%s:%s:", recordType, field)
    
    counts := make(map[string]int)
    
    // CORRECTION: Find → NewIterator
    iter := idx.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
    defer iter.Release()
    
    for iter.Next() {
        key := string(iter.Key())
        
        if !strings.HasPrefix(key, prefix) {
            break
        }
        
        parts := strings.Split(key, ":")
        if len(parts) >= 4 {
            value := parts[3]
            counts[value]++
        }
    }
    
    if err := iter.Error(); err != nil {
        return nil, err
    }
    
    return counts, nil
}

func (idx *Indexer) CreateCompositeIndex(recordType string, fields []string, values []string, primaryKey string) error {
    if len(fields) != len(values) {
        return fmt.Errorf("nombre de champs et valeurs différent")
    }
    
    compositeField := strings.Join(fields, "-")
    
    normalizedValues := make([]string, len(values))
    for i, v := range values {
        normalizedValues[i] = strings.ToLower(strings.TrimSpace(v))
    }
    compositeValue := strings.Join(normalizedValues, "-")
    
    indexKey := fmt.Sprintf("idx:%s:%s:%s:%s", 
        recordType, compositeField, compositeValue, primaryKey)
    
    // CORRECTION: Set → Put
    return idx.db.Put([]byte(indexKey), []byte(primaryKey), nil)
}

func (idx *Indexer) SearchByCompositeIndex(recordType string, fields []string, values []string) ([]string, error) {
    compositeField := strings.Join(fields, "-")
    
    normalizedValues := make([]string, len(values))
    for i, v := range values {
        normalizedValues[i] = strings.ToLower(strings.TrimSpace(v))
    }
    compositeValue := strings.Join(normalizedValues, "-")
    
    prefix := fmt.Sprintf("idx:%s:%s:%s:", recordType, compositeField, compositeValue)
    
    var results []string
    
    // CORRECTION: Find → NewIterator
    iter := idx.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
    defer iter.Release()
    
    for iter.Next() {
        key := string(iter.Key())
        if !strings.HasPrefix(key, prefix) {
            break
        }
        results = append(results, string(iter.Value()))
    }
    
    return results, nil
}

func isIndexableField(field string) bool {
    systemFields := map[string]bool{
        "hash":       true,
        "timestamp":  true,
        "node":       true,
        "ledger_type": false,
        "data":       true,
    }
    
    if excluded, exists := systemFields[field]; exists {
        return !excluded
    }
    
    return true
}