package dataloader3_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alextanhongpin/dataloader3"
	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFn, keyFn)
	defer flush()

	assert.True(!dl.Has(1))

	val, err := dl.Load(1)
	assert.Nil(err)
	assert.Equal("1", val)

	assert.True(dl.Has(1))
}

func TestLoadMany(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFn, keyFn)
	defer flush()

	keys := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	expValues := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	values, err := dl.LoadMany(keys)
	assert.Nil(err)
	assert.Equal(expValues, values)
}

func TestConcurrent(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFn, keyFn)
	defer flush()

	n := 1_000
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			duration := time.Duration(rand.Intn(500)) * time.Millisecond
			time.Sleep(duration)
			i := rand.Intn(n)

			val, err := dl.Load(i)
			assert.Nil(err)
			assert.Equal(fmt.Sprint(i), val)
		}()
	}

	wg.Wait()
}

func TestStruct(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(userBatchFn, userKeyFn)
	defer flush()

	val, err := dl.Load("1")
	assert.Nil(err)
	assert.Equal(user{id: "1"}, val)

	dl.Set("1", user{id: "new1"})
	val, err = dl.Load("1")
	assert.Nil(err)
	assert.Equal(user{id: "new1"}, val)
}

func TestError(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFnError, keyFn)
	defer flush()

	val, err := dl.Load(1)
	assert.NotNil(err)
	assert.Equal("", val)

	values, err := dl.LoadMany([]int{1})
	assert.NotNil(err)
	assert.True(len(values) == 0)
}

func TestNoResult(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFnNoResult, keyFn)
	defer flush()

	val, err := dl.Load(1)
	assert.NotNil(err)
	assert.Equal("", val)

	values, err := dl.LoadMany([]int{1})
	assert.NotNil(err)
	assert.True(len(values) == 0)
}

func TestTimeoutNoResult(t *testing.T) {
	assert := assert.New(t)
	dl, flush := dataloader3.New(batchFnTimeout, keyFn, dataloader3.BatchTimeout(20*time.Millisecond))
	defer flush()

	val, err := dl.Load(1)
	assert.NotNil(err)
	assert.Equal("", val)

	values, err := dl.LoadMany([]int{1})
	assert.NotNil(err)
	assert.True(len(values) == 0)
}

func TestTimeoutResult(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	dl, flush := dataloader3.New(batchFnTimeout, keyFn, dataloader3.BatchTimeout(20*time.Millisecond))
	defer flush()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	val, err := dl.Load(1)
	assert.NotNil(err)
	assert.Equal("", val)

	values, err := dl.LoadMany([]int{1})
	assert.NotNil(err)
	assert.True(len(values) == 0)
}

func TestCancel(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFnTimeout, keyFn)
	flush()

	dl.Preload(1)

	val, err := dl.Load(1)
	assert.NotNil(err)
	assert.Equal("", val)

	values, err := dl.LoadMany([]int{1})
	assert.NotNil(err)
	assert.True(len(values) == 0)
}

func TestContextCancel(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFnTimeout, keyFn)
	defer flush()

	val, err := dl.Load(1)
	assert.Nil(err)
	assert.Equal("1", val)

	values, err := dl.LoadMany([]int{1})
	assert.Nil(err)
	assert.True(len(values) == 1)
}

func TestMaxBatchKeys(t *testing.T) {
	assert := assert.New(t)

	dl, flush := dataloader3.New(batchFn, keyFn, dataloader3.BatchMaxKeys(2))
	defer flush()

	values, err := dl.LoadMany([]int{1, 2, 3})
	assert.Nil(err)
	assert.Equal([]string{"1", "2", "3"}, values)
}

func TestOptions(t *testing.T) {
	assert := assert.New(t)

	options := []dataloader3.Option{
		dataloader3.BatchMaxWorkers(2),
		dataloader3.BatchInterval(20 * time.Millisecond),
		dataloader3.BatchTimeout(1 * time.Second),
		dataloader3.NoDebounce(),
		dataloader3.BaseContext(func() context.Context {
			return context.Background()
		}),
	}
	dl, flush := dataloader3.New(batchFn, keyFn, options...)
	defer flush()

	values, err := dl.LoadMany([]int{1, 2, 3})
	assert.Nil(err)
	assert.Equal([]string{"1", "2", "3"}, values)

	// Should return the cached data even after flushed.
	flush()
	values, err = dl.LoadMany([]int{1, 2, 3})
	assert.Nil(err)
	assert.Equal([]string{"1", "2", "3"}, values)
}

func TestKeyError(t *testing.T) {
	keyErr := dataloader3.NewKeyError[string]()
	keyErr.Set("hello", errors.New("hello"))
	keyErr.Set("world", errors.New("world"))

	assert := assert.New(t)
	assert.Equal("dataloader: 2 keys failed", keyErr.Error())

	var err error
	err = keyErr

	var target *dataloader3.KeyError[string]
	assert.True(errors.As(err, &target))
	assert.NotNil(target.Get("hello"))
	assert.NotNil(target.Get("world"))
	assert.True(errors.Is(target.Get("unknown"), dataloader3.ErrKeyNotFound))
	assert.Equal(2, target.Len())
	assert.NotNil(target.Map())
}

func batchFn(ctx context.Context, keys []int) ([]string, error) {
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = fmt.Sprint(k)
	}

	return result, nil
}

func batchFnError(ctx context.Context, keys []int) ([]string, error) {
	return nil, errors.New("bad error")
}

func batchFnNoResult(ctx context.Context, keys []int) ([]string, error) {
	return nil, nil
}

func batchFnTimeout(ctx context.Context, keys []int) ([]string, error) {
	select {
	case <-time.After(200 * time.Millisecond):
		result := make([]string, len(keys))
		for i, k := range keys {
			result[i] = fmt.Sprint(k)
		}
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func keyFn(val string) (key int) {
	var err error
	key, err = strconv.Atoi(val)
	if err != nil {
		panic(err)
	}

	return
}

type user struct {
	id string
}

func userBatchFn(ctx context.Context, keys []string) ([]user, error) {
	result := make([]user, len(keys))
	for i, k := range keys {
		result[i] = user{id: k}
	}

	return result, nil
}

func userKeyFn(u user) (key string) {
	return u.id
}
