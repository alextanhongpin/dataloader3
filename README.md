# dataloader3


DataLoader for GraphQL.


## Usage

1. Define a batch function.
2. Define a key function.
3. Call the `Load`, `LoadMany` or `Preload` method.


```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/alextanhongpin/dataloader3"
)

func main() {
	dl, flush := dataloader3.New(batchFn, keyFn)
	defer flush()

	n := 10

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			u, err := dl.Load(id)
			if err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("user:", u)
			}
		}(i)
	}

	wg.Wait()

	users, err := dl.LoadMany([]int{1, 2, 3, 4, 5, 10, 20, 30, 40, 50})
	if err != nil {
		// NOTE: Even if the error is not nil, there may be results returned.
		fmt.Println("error:", err) // user 1, 2, 3 and 4 will failed.
	}
	fmt.Println("users:", users) // user 5, 10, 20, 30, 40 and 50 will be returned.
}

type User struct {
	ID int
}

func batchFn(ctx context.Context, uniqueKeys []int) ([]User, error) {
	fmt.Println("uniqueKeys:", uniqueKeys)
	var res []User

	// DataLoader is guaranteed to provide unique keys for
	// fetching.
	for _, k := range uniqueKeys {
		if k < 5 {
			continue
		}
		res = append(res, User{ID: k})
	}

	return res, nil
}

func keyFn(u User) int {
	return u.ID
}
```
