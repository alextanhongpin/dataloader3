package dataloader3_test

import (
	"context"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dl, flush := dataloader3.New(ctx, batchFn, keyFn)
	defer flush()

	assert.True(!dl.Has(1))

	val, err := dl.Load(ctx, 1)
	assert.Nil(err)
	assert.Equal("1", val)

	assert.True(dl.Has(1))
}

func TestLoadMany(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dl, flush := dataloader3.New(ctx, batchFn, keyFn)
	defer flush()

	keys := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	expValues := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	values := dl.LoadMany(ctx, keys)
	assert.Equal(expValues, values)
}

func TestConcurrent(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dl, flush := dataloader3.New(ctx, batchFn, keyFn)
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

			val, err := dl.Load(ctx, i)
			assert.Nil(err)
			assert.Equal(fmt.Sprint(i), val)
		}()
	}

	wg.Wait()
}

func TestStruct(t *testing.T) {
	assert := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dl, flush := dataloader3.New(ctx, userBatchFn, userKeyFn)
	defer flush()

	val, err := dl.Load(ctx, "1")
	assert.Nil(err)
	assert.Equal(user{id: "1"}, val)

	dl.Set("1", user{id: "new1"})
	val, err = dl.Load(ctx, "1")
	assert.Nil(err)
	assert.Equal(user{id: "new1"}, val)
}

func batchFn(ctx context.Context, keys []int) ([]string, error) {
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = fmt.Sprint(k)
	}

	return result, nil
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
