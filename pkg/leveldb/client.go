// pkg/leveldb/client.go
// Client LevelDB avec vérification d'intégrité cryptographique

package leveldb

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "time"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

type Client struct {
    db   *leveldb.DB
    node string
}

type Entry struct {
    Data      json.RawMessage `json:"data"`
    Hash      string          `json:"hash"`
    Timestamp string          `json:"timestamp"`
    Node      string          `json:"node"`
}

func NewClient(nodePath string) (*Client, error) {
    opts := &opt.Options{
        WriteBuffer: 4 * 1024 * 1024,
        Compression: opt.SnappyCompression,
    }
    
    // CORRECTION: Open prend directement le path
    db, err := leveldb.OpenFile(nodePath, opts)
    if err != nil {
        return nil, fmt.Errorf("erreur ouverture LevelDB: %v", err)
    }
    
    return &Client{
        db:   db,
        node: nodePath,
    }, nil
}

func (c *Client) Put(key string, data interface{}) error {
    dataBytes, err := json.Marshal(data)
    if err != nil {
        return fmt.Errorf("erreur sérialisation: %v", err)
    }
    
    hash := calculateHash(dataBytes)
    
    entry := Entry{
        Data:      dataBytes,
        Hash:      hash,
        Timestamp: time.Now().Format(time.RFC3339),
        Node:      c.node,
    }
    
    entryBytes, err := json.Marshal(entry)
    if err != nil {
        return fmt.Errorf("erreur sérialisation entry: %v", err)
    }
    
    // CORRECTION: Set → Put
    return c.db.Put([]byte(key), entryBytes, nil)
}

func (c *Client) Get(key string) (*Entry, error) {
    value, err := c.db.Get([]byte(key), nil)
    if err != nil {
        return nil, fmt.Errorf("clé non trouvée: %v", err)
    }
    
    var entry Entry
    if err := json.Unmarshal(value, &entry); err != nil {
        return nil, fmt.Errorf("erreur désérialisation: %v", err)
    }
    
    return &entry, nil
}

func (c *Client) VerifyIntegrity(key string) (bool, error) {
    entry, err := c.Get(key)
    if err != nil {
        return false, err
    }
    
    computedHash := calculateHash(entry.Data)
    
    if computedHash != entry.Hash {
        return false, fmt.Errorf("hash mismatch: attendu %s, obtenu %s", 
            entry.Hash, computedHash)
    }
    
    return true, nil
}

func (c *Client) Delete(key string) error {
    return c.db.Delete([]byte(key), nil)
}

func (c *Client) BatchInsert(entries map[string]interface{}) error {
    batch := new(leveldb.Batch)
    
    for key, data := range entries {
        dataBytes, err := json.Marshal(data)
        if err != nil {
            return fmt.Errorf("erreur sérialisation %s: %v", key, err)
        }
        
        hash := calculateHash(dataBytes)
        
        entry := Entry{
            Data:      dataBytes,
            Hash:      hash,
            Timestamp: time.Now().Format(time.RFC3339),
            Node:      c.node,
        }
        
        entryBytes, _ := json.Marshal(entry)
        
        // CORRECTION: Set → Put
        batch.Put([]byte(key), entryBytes)
    }
    
    // CORRECTION: Apply → Write
    return c.db.Write(batch, nil)
}

func (c *Client) Count() (int, error) {
    // CORRECTION: Find → NewIterator
    iter := c.db.NewIterator(nil, nil)
    defer iter.Release()
    
    count := 0
    for iter.Next() {
        key := iter.Key()
        if len(key) > 0 && key[0] != '_' {
            count++
        }
    }
    
    if err := iter.Error(); err != nil {
        return 0, err
    }
    
    return count, nil
}

func (c *Client) Close() error {
    return c.db.Close()
}

func (c *Client) GetDB() *leveldb.DB {
    return c.db
}

func calculateHash(data []byte) string {
    h := sha256.New()
    h.Write(data)
    return hex.EncodeToString(h.Sum(nil))
}