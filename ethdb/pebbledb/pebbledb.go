package pebbledb

import (
	"errors"
	"sync"
	"time"

	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/ethdb"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/metrics"
	"github.com/cockroachdb/pebble"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute

	// minCache is the minimum amount of memory in megabytes to allocate to leveldb
	// read and write caching, split half and half.
	minCache = 32

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 100

	// metricsGatheringInterval specifies the interval to retrieve leveldb database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second
)

type Database struct {
	fn string     // filename for reporting
	db *pebble.DB // PebbleDB instance

	compTimeMeter      metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter      metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter     metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter   metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter    metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge      metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter      metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter     metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge       metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge    metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge      metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {
	return NewCustom(file, namespace, func(options *pebble.Options) {
		// Ensure we have some minimal caching and file guarantees
		if cache < minCache {
			cache = minCache
		}
		if handles < minHandles {
			handles = minHandles
		}
		// Set default options
		options.MaxOpenFiles = handles
		options.Cache = pebble.NewCache(int64(cache/2) * opt.MiB)
		options.MemTableSize = cache / 2 * opt.MiB
		options.ReadOnly = readonly
	})
}

func NewCustom(file string, namespace string, customize func(options *pebble.Options)) (*Database, error) {
	options := configureOptions(customize)
	logger := log.New("database", file)
	usedCache := options.Cache.MaxSize()
	logCtx := []interface{}{"cache", common.StorageSize(usedCache), "handles", options.MaxOpenFiles}
	if options.ReadOnly {
		logCtx = append(logCtx, "readonly", "true")
	}
	logger.Info("Allocated cache and file handles", logCtx...)

	// Open the db and recover any potential corruptions
	db, err := pebble.Open(file, options)
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	pdb := &Database{
		fn:       file,
		db:       db,
		log:      logger,
		quitChan: make(chan chan error),
	}
	pdb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	pdb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	pdb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	pdb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	pdb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	pdb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	pdb.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	pdb.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	pdb.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	pdb.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	pdb.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	pdb.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)

	// Start up the metrics gathering and return
	//go pdb.meter(metricsGatheringInterval)
	return pdb, nil
}

func configureOptions(customizeFn func(options *pebble.Options)) *pebble.Options {
	// Set default options
	options := &pebble.Options{}
	// Allow caller to make custom modifications to the options
	if customizeFn != nil {
		customizeFn(options)
	}
	return options
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
		db.quitChan = nil
	}
	return db.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	_, closer, err := db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			// mask not found as false
			err = nil
		}
		return false, err
	}
	return true, closer.Close()
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	dat, closer, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(dat))
	copy(result, dat)
	return result, closer.Close()
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db   *pebble.DB
	b    *pebble.Batch
	ops  []op
	size int
}

type op struct {
	delete bool
	key    []byte
	value  []byte
}

// Put inserts the given value into the batch for later committing.
func (b batch) Put(key []byte, value []byte) error {
	b.b.Set(key, value, nil)
	b.size += len(value)
	b.ops = append(b.ops, op{
		delete: false,
		key:    key,
		value:  value,
	})
	return nil
}

// Delete inserts a key removal into the batch for later committing.
func (b batch) Delete(key []byte) error {
	b.b.Delete(key, nil)
	b.size += len(key)
	b.ops = append(b.ops, op{
		delete: true,
		key:    key,
		value:  nil,
	})
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b batch) Write() error {
	return b.b.Commit(nil)
}

// Reset resets the batch for reuse.
func (b batch) Reset() {
	b.b.Reset()
	b.size = 0
	b.ops = make([]op, 0)
}

// Replay replays the batch contents.
func (b batch) Replay(w ethdb.KeyValueWriter) error {
	var err error
	for _, op := range b.ops {
		if op.delete {
			err = w.Delete(op.key)
		} else {
			err = w.Put(op.key, op.value)
		}
		if err != nil {
			return err
		}
	}
	return err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.db.Set(key, value, nil)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.db.Delete(key, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	return &batch{
		db:  db.db,
		b:   db.db.NewBatch(),
		ops: make([]op, 0),
	}
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	r := bytesPrefixRange(prefix, start)
	iter := db.db.NewIter(&pebble.IterOptions{
		LowerBound: r.Start,
		UpperBound: r.Limit,
	})
	return iterWrapper{
		iter:   iter,
		closed: false,
	}
}

type iterWrapper struct {
	iter   *pebble.Iterator
	closed bool
}

func (i iterWrapper) Next() bool {
	return i.iter.Next()
}

func (i iterWrapper) Error() error {
	return i.iter.Error()
}

func (i iterWrapper) Key() []byte {
	return i.iter.Key()
}

func (i iterWrapper) Value() []byte {
	return i.iter.Value()
}

func (i iterWrapper) Release() {
	if !i.closed {
		i.iter.Close()
	}
	i.closed = true
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return "", errors.New("unknown property")
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	return db.db.Compact(start, limit, true)
}
