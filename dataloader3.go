package dataloader3

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

var (
	ErrTimeout     = errors.New("dataloader: fetch timeout")
	ErrKeyNotFound = errors.New("dataloader: key not found")
)

type DataLoader[K comparable, V any] struct {
	ctx              context.Context
	mu               sync.RWMutex
	wg               sync.WaitGroup
	cache            map[K]Result[V]
	done             chan struct{}
	ch               []chan job[K, V]
	keyFn            keyFn[K, V]
	batchFn          batchFn[K, V]
	batchTimeout     time.Duration
	batchWaitTimeout time.Duration // How long before cancelling the batchFn
	batchMaxKeys     int
	batchMaxWorkers  int
	debounce         bool

	once sync.Once
}

type batchFn[K comparable, V any] func(ctx context.Context, keys []K) ([]V, error)

type keyFn[K comparable, V any] func(V) K

func New[K comparable, V any](
	ctx context.Context,
	batchFn batchFn[K, V],
	keyFn keyFn[K, V],
	options ...Option,
) (*DataLoader[K, V], func()) {
	opt := NewDefaultOption()
	for _, o := range options {
		o(opt)
	}

	ch := make([]chan job[K, V], opt.BatchMaxWorkers)
	for i := 0; i < opt.BatchMaxWorkers; i++ {
		ch[i] = make(chan job[K, V], opt.BatchMaxWorkers)
	}

	dl := &DataLoader[K, V]{
		ctx:              ctx,
		ch:               ch,
		cache:            make(map[K]Result[V]),
		done:             make(chan struct{}),
		keyFn:            keyFn,
		batchFn:          batchFn,
		batchTimeout:     opt.BatchTimeout,
		batchWaitTimeout: opt.BatchWaitTimeout,
		batchMaxKeys:     opt.BatchMaxKeys,
		batchMaxWorkers:  opt.BatchMaxWorkers,
		debounce:         opt.Debounce,
	}

	var once sync.Once

	return dl, func() {
		once.Do(func() {
			close(dl.done)
			dl.wg.Wait()
		})
	}
}

// Has checks if the key exists.
func (dl *DataLoader[K, V]) Has(k K) bool {
	dl.mu.RLock()
	_, ok := dl.cache[k]
	dl.mu.RUnlock()

	return ok
}

// Set overrides the existing cache value.
func (dl *DataLoader[K, V]) Set(k K, v V) {
	dl.mu.Lock()
	dl.cache[k] = Result[V]{Value: v}
	dl.mu.Unlock() // Notify all the other readers that a potential result has been written.
}

// LoadMany returns the result of the keys. Does not return error.
func (dl *DataLoader[K, V]) LoadMany(ctx context.Context, keys []K) []V {
	result := make([]Result[V], len(keys))

	var wg sync.WaitGroup
	wg.Add(len(keys))

	for i, key := range keys {
		go func(i int, key K) {
			defer wg.Done()

			v, err := dl.Load(ctx, key)
			if err != nil {
				result[i] = Result[V]{Error: err}
			} else {
				result[i] = Result[V]{Value: v}
			}
		}(i, key)
	}
	wg.Wait()

	values := make([]V, 0, len(result))
	for _, v := range result {
		if v.Error != nil {
			continue
		}

		values = append(values, v.Value)
	}

	return values
}

func (dl *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	dl.init()

	j := job[K, V]{
		key: key,
		ch:  make(chan Result[V]),
	}

	dl.ch[dl.partition(key)] <- j

	select {
	case <-ctx.Done():
		var v V
		return v, ctx.Err()
	case <-time.After(dl.batchWaitTimeout):
		var v V
		return v, fmt.Errorf("%w: %v", ErrTimeout, key)
	case r := <-j.ch:
		return r.Value, r.Error
	}
}

func (dl *DataLoader[K, V]) worker(id int, ch chan job[K, V]) {
	t := time.NewTicker(dl.batchTimeout)
	defer t.Stop()

	todos := make([]job[K, V], 0, dl.batchMaxKeys)

	flush := func() {
		if len(todos) == 0 {
			return
		}

		defer func() {
			// Keep the capacity, but empty the slice.
			todos = todos[:0]
		}()

		cache := make(map[K]bool)

		keys := make([]K, 0, len(todos))
		for _, todo := range todos {
			if cache[todo.key] {
				continue
			}
			cache[todo.key] = true

			keys = append(keys, todo.key)
		}

		// Fetch only unique values.
		values, err := dl.batchFn(dl.ctx, keys)
		if err != nil {
			for _, todo := range todos {
				todo.ch <- Result[V]{Error: err}
			}

			return
		}

		resByKey := make(map[K]Result[V])
		for _, v := range values {
			resByKey[dl.keyFn(v)] = Result[V]{Value: v}
		}

		dl.mu.Lock()
		defer dl.mu.Unlock()

		for _, todo := range todos {
			res, ok := resByKey[todo.key]
			if !ok {
				todo.ch <- Result[V]{Error: fmt.Errorf("%w: %v", ErrKeyNotFound, todo.key)}
			} else {
				// Only cache successful result.
				// This allows failed result to be retried later.
				todo.ch <- res
				dl.cache[todo.key] = res
			}
		}
	}

	for {
		select {
		case <-dl.ctx.Done():
			flush()

			return
		case <-dl.done:
			flush()

			return
		case res := <-ch:
			dl.mu.RLock()
			val, ok := dl.cache[res.key]
			dl.mu.RUnlock()

			if ok {
				// Cache hit.
				res.ch <- val
				continue
			}

			// Debounce improves batching by resetting the interval when a new
			// unknown key is requested.
			if dl.debounce {
				t.Reset(dl.batchTimeout)
			}

			todos = append(todos, res)

			// Flush when the number of keys hits the threshold.
			if len(todos) >= dl.batchMaxKeys {
				flush()
			}
		case <-t.C:
			// Flush at every interval.
			flush()
		}
	}
}

func (dl *DataLoader[K, V]) init() {
	// Lazily initiate the worker, to avoid creating unnecessary goroutines.
	dl.once.Do(func() {
		dl.wg.Add(len(dl.ch))

		for i, ch := range dl.ch {
			go func(i int, ch chan job[K, V]) {
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

type Result[K any] struct {
	Value K
	Error error
}

type DataLoaderOption struct {
	BatchTimeout     time.Duration
	BatchWaitTimeout time.Duration // How long before cancellingthe batchFn
	BatchMaxKeys     int
	BatchMaxWorkers  int
	Debounce         bool
}

func NewDefaultOption() *DataLoaderOption {
	return &DataLoaderOption{
		BatchTimeout:     16 * time.Millisecond,
		BatchWaitTimeout: 5 * time.Second,
		BatchMaxKeys:     1_000,
		BatchMaxWorkers:  1,
		Debounce:         true,
	}
}

// BatchTimeout is the duration before the batchFn is called.
func BatchTimeout(duration time.Duration) Option {
	return func(o *DataLoaderOption) {
		o.BatchTimeout = duration
	}
}

// BatchWaitTimeout is the duration to wait before invalidating the batchFn
// call.
// This is to prevent the batchFn from blocking forever.
func BatchWaitTimeout(duration time.Duration) Option {
	return func(o *DataLoaderOption) {
		o.BatchWaitTimeout = duration
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

type Option func(o *DataLoaderOption)

type job[K comparable, V any] struct {
	key K
	ch  chan Result[V]
}
