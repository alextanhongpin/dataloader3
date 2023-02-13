package dataloader3

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("dataloader: key not found")

type DataLoader[K comparable, V any] struct {
	wg              sync.WaitGroup
	done            chan struct{}
	cond            *sync.Cond
	cache           map[K]Result[V]
	ch              []chan K
	keyFn           keyFn[K, V]
	baseContext     func() context.Context
	batchFn         batchFn[K, V]
	batchInterval   time.Duration
	batchTimeout    time.Duration
	batchMaxKeys    int
	batchMaxWorkers int
	debounce        bool

	initOnce sync.Once
	stopOnce sync.Once
}

type batchFn[K comparable, V any] func(ctx context.Context, keys []K) ([]V, error)

type keyFn[K comparable, V any] func(V) K

func New[K comparable, V any](batchFn batchFn[K, V], keyFn keyFn[K, V], options ...Option) (*DataLoader[K, V], func()) {
	opt := NewDefaultOption()
	for _, o := range options {
		o(opt)
	}

	dl := &DataLoader[K, V]{
		cache:           make(map[K]Result[V]),
		cond:            sync.NewCond(&sync.Mutex{}),
		done:            make(chan struct{}),
		keyFn:           keyFn,
		baseContext:     opt.BaseContext,
		batchFn:         batchFn,
		batchInterval:   opt.BatchInterval,
		batchTimeout:    opt.BatchTimeout,
		batchMaxKeys:    opt.BatchMaxKeys,
		batchMaxWorkers: opt.BatchMaxWorkers,
		debounce:        opt.Debounce,
	}

	return dl, dl.close
}

// Has checks if the key exists in the cache.
func (dl *DataLoader[K, V]) Has(k K) bool {
	dl.cond.L.Lock()
	_, ok := dl.cache[k]
	dl.cond.L.Unlock()

	return ok
}

// Set overrides the existing cache value.
func (dl *DataLoader[K, V]) Set(k K, v V) {
	dl.cond.L.Lock()
	dl.cache[k] = Result[V]{Value: v}
	dl.cond.Broadcast()
	dl.cond.L.Unlock() // Notify all the other readers that a potential result has been written.
}

// LoadMany returns the result of the keys as well as partial errors.
// Due to how the keys may not return result, the err can be skipped.
func (dl *DataLoader[K, V]) LoadMany(keys []K) ([]V, error) {
	dl.init()

	// Lazily preloads the data, ensuring the data is loaded in batch.
	// This is more performant then loading them individually.
	dl.send(keys...)

	var keyErr *KeyError[K]

	values := make([]V, 0, len(keys))
	for _, key := range keys {
		v, err := dl.Load(key)
		if err != nil {
			if keyErr == nil {
				keyErr = NewKeyError[K]()
			}
			keyErr.Set(key, err)

			continue
		}

		values = append(values, v)
	}

	return values, keyErr
}

// Preload sends the keys to batch without waiting for the result.
func (dl *DataLoader[K, V]) Preload(keys ...K) {
	dl.init()

	dl.send(keys...)
}

// Load loads a cached value by key. If the key does not exists, it will lazily
// batch load multiple keys.
func (dl *DataLoader[K, V]) Load(key K) (V, error) {
	dl.init()

	select {
	case <-dl.done:
		// If the worker is closed, the dataloader just acts as a plain key-value
		// store.
		return dl.get(key)
	default:
		if !dl.send(key) {
			return dl.get(key)
		}
	}

	for {
		dl.cond.L.Lock()
		r, ok := dl.cache[key]
		if ok {
			dl.cond.L.Unlock()
			return r.Value, r.Error
		}
		dl.cond.Wait()
		dl.cond.L.Unlock()
	}
}

func (dl *DataLoader[K, V]) send(keys ...K) bool {
	if len(dl.ch) == 0 {
		return false
	}

	for _, key := range keys {
		select {
		case <-dl.done:
			return false
		case dl.ch[dl.partition(key)] <- key:
		}
	}

	return true
}

func (dl *DataLoader[K, V]) get(key K) (V, error) {
	dl.cond.L.Lock()
	r, ok := dl.cache[key]
	dl.cond.L.Unlock()
	if ok {
		return r.Value, r.Error
	}

	var v V
	return v, fmt.Errorf("%w: %v", ErrKeyNotFound, key)
}

func (dl *DataLoader[K, V]) worker(id int, ch chan K) {
	t := time.NewTicker(dl.batchInterval)
	defer t.Stop()

	// Due to the way partitioning is done, the same goroutine will always
	// receive the same set of keys.
	// This prevents concurrent fetch of the same key from multiple goroutines.
	keys := make(map[K]struct{})

	flush := func() {
		if len(keys) == 0 {
			return
		}

		defer func() {
			keys = make(map[K]struct{})
		}()

		uniqueKeys := make([]K, 0, len(keys))
		for key := range keys {
			uniqueKeys = append(uniqueKeys, key)
		}

		// Add timeout when fetching values.
		ctx, cancel := context.WithTimeout(dl.baseContext(), dl.batchTimeout)
		defer cancel()

		// batchFn is called on the unique keys.
		values, err := dl.batchFn(ctx, uniqueKeys)
		if err != nil {
			// If an error occured, mark all the keys as failed and broadcast the
			// result to all waiting readers.
			// This is to prevent the sync.Cond from blocking forever.
			dl.cond.L.Lock()
			for key := range keys {
				// But only set to those without results, to avoid overwriting existing
				// successful data.
				if _, ok := dl.cache[key]; !ok {
					dl.cache[key] = Result[V]{Error: err}
				}
			}

			// Broadcast to all waiting readers.
			dl.cond.Broadcast()
			dl.cond.L.Unlock()

			return
		}

		dl.cond.L.Lock()
		// Update the cache with the successful result.
		for _, v := range values {
			dl.cache[dl.keyFn(v)] = Result[V]{Value: v}
		}

		// When the length of the keys and the values does not match, the sync.Cond
		// might lock forever.
		// We need to handle the keys without result by marking them as failed.
		for key := range keys {
			if _, ok := dl.cache[key]; !ok {
				dl.cache[key] = Result[V]{Error: fmt.Errorf("%w: %v", ErrKeyNotFound, key)}
			}
		}

		// Broadcast to all waiting readers.
		dl.cond.Broadcast()
		dl.cond.L.Unlock()
	}

	for {
		select {
		case <-dl.done:
			// When the goroutine terminates, all the pending keys are failed.
			dl.cond.L.Lock()
			for key := range keys {
				if _, ok := dl.cache[key]; !ok {
					dl.cache[key] = Result[V]{Error: fmt.Errorf("%w: %v", ErrKeyNotFound, key)}
				}
			}
			dl.cond.Broadcast()
			dl.cond.L.Unlock()

			keys = nil

			return
		case key := <-ch:
			if _, ok := keys[key]; ok {
				continue
			}

			// Debounce improves batching by resetting the interval when a new
			// unknown key is requested.
			if dl.debounce {
				t.Reset(dl.batchInterval)
			}

			keys[key] = struct{}{}

			// Flush when the number of keys hits the threshold.
			if len(keys) >= dl.batchMaxKeys {
				flush()
				// NOTE: Would resetting interval here helps?
			}
		case <-t.C:
			// Flush at every interval.
			flush()
		}
	}
}

// init lazily starts the worker, to avoid creating unnecessary goroutines.
// This becomes extremely prominent when multiple dataloaders are created per
// session, but not all are utilized.
func (dl *DataLoader[K, V]) init() {
	dl.initOnce.Do(func() {
		n := dl.batchMaxWorkers

		dl.ch = make([]chan K, n)
		for i := 0; i < n; i++ {
			dl.ch[i] = make(chan K, n)
		}

		dl.wg.Add(n)

		for i, ch := range dl.ch {
			go func(i int, ch chan K) {
				defer dl.wg.Done()

				dl.worker(i, ch)
			}(i, ch)
		}
	})
}

func (dl *DataLoader[K, V]) partition(key K) int {
	if dl.batchMaxWorkers == 1 {
		return 0
	}

	h := fnv.New32()
	h.Write([]byte(fmt.Sprint(key)))
	partition := int(h.Sum32() % uint32(dl.batchMaxWorkers))
	return partition
}

func (dl *DataLoader[K, V]) close() {
	// If init is not called yet, this will prevent future execution, which may
	// be blocking.
	dl.initOnce.Do(func() {})
	dl.stopOnce.Do(func() {
		close(dl.done)
		dl.wg.Wait()
	})
}

type Result[K any] struct {
	Value K
	Error error
}

type DataLoaderOption struct {
	BatchTimeout    time.Duration
	BatchInterval   time.Duration
	BatchMaxKeys    int
	BatchMaxWorkers int
	Debounce        bool
	BaseContext     func() context.Context
}

func NewDefaultOption() *DataLoaderOption {
	return &DataLoaderOption{
		BatchTimeout:    5 * time.Second,
		BatchInterval:   16 * time.Millisecond,
		BatchMaxKeys:    1_000,
		BatchMaxWorkers: 1,
		Debounce:        true,
		BaseContext: func() context.Context {
			return context.Background()
		},
	}
}

// BatchInterval is the duration before the batchFn is called.
func BatchInterval(duration time.Duration) Option {
	return func(o *DataLoaderOption) {
		o.BatchInterval = duration
	}
}

func BatchTimeout(duration time.Duration) Option {
	return func(o *DataLoaderOption) {
		o.BatchTimeout = duration
	}
}

// BatchMaxKeys is the maximum number of keys that will be passed to batchFn.
func BatchMaxKeys(n int) Option {
	return func(o *DataLoaderOption) {
		o.BatchMaxKeys = n
	}
}

// NoDebounce sets debounce to false.
func NoDebounce() Option {
	return func(o *DataLoaderOption) {
		o.Debounce = false
	}
}

// BatchMaxWorkers is the number of background workers to run for batchFn.
// Keys will be partition to each worker, so that each worker receives the same
// key every time, hence increasing cache hit and reducing the chance of
// fetching the same key in different goroutines.
func BatchMaxWorkers(n int) Option {
	return func(o *DataLoaderOption) {
		o.BatchMaxWorkers = n
	}
}

// BaseContext provides a way to construct a new context when calling BatchFn.
func BaseContext(fn func() context.Context) Option {
	return func(o *DataLoaderOption) {
		o.BaseContext = fn
	}
}

type Option func(o *DataLoaderOption)

type KeyError[K comparable] struct {
	keys map[K]error
}

func NewKeyError[K comparable]() *KeyError[K] {
	return &KeyError[K]{
		keys: make(map[K]error),
	}
}

func (k *KeyError[K]) Map() map[K]error {
	res := make(map[K]error)
	for kk, v := range k.keys {
		res[kk] = v
	}

	return res
}

func (k *KeyError[K]) Len() int {
	return len(k.keys)
}

func (k *KeyError[K]) Set(key K, err error) {
	k.keys[key] = err
}

func (k *KeyError[K]) Get(key K) error {
	err, ok := k.keys[key]
	if ok {
		return err
	}

	return fmt.Errorf("%w: %v", ErrKeyNotFound, key)
}

func (k *KeyError[K]) Error() string {
	var label string
	if len(k.keys) == 1 {
		label = "key"
	} else {
		label = "keys"
	}
	return fmt.Sprintf("dataloader: %d %s failed", len(k.keys), label)
}
