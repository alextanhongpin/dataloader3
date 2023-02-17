# dataloader3


DataLoader for GraphQL.


## Usage

1. Define a batch function.
2. Call the `Load`, `LoadMany` or `Preload` method.


## Example Load and Load Many

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
	dl, flush := dataloader3.New(batchFn)
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

func batchFn(ctx context.Context, uniqueKeys []int) (map[int]User, error) {
	fmt.Println("uniqueKeys:", uniqueKeys)

	users, err := fetchUsers(ctx, uniqueKeys)
	if err != nil {
		return nil, err
	}

	keyFn := func(u User) int {
		return u.ID
	}

	// Group the users by id. if the key does exists, an error KeyError will be
	// returned.
	return dataloader3.GroupBy(users, keyFn), nil
}

func fetchUsers(ctx context.Context, uniqueKeys []int) ([]User, error) {
	var users []User

	// DataLoader is guaranteed to provide unique keys for
	// fetching.
	for _, k := range uniqueKeys {
		if k < 5 {
			continue
		}
		users = append(users, User{ID: k})
	}

	return users, nil
}
```

## Example Lazy Load Nested Associations

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
	userLoader, flushUser := dataloader3.New(batchUserFn)
	defer flushUser()

	photoLoader, flushPhoto := dataloader3.New(batchPhotoFn)
	defer flushPhoto()

	tagLoader, flushTag := dataloader3.New(batchTagFn)
	defer flushTag()

	n := 10

	var wg sync.WaitGroup
	wg.Add(n)

	users := make([]User, n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()

			// Fetch user.
			u, err := userLoader.Load(i)
			if err != nil {
				// TODO: Handle error later.
				panic(err)
			}

			// Fetch user photos.
			photos, err := photoLoader.Load(u.ID)
			if err != nil {
				// TODO: Handle error later.
				panic(err)
			}

			u.Photos = photos

			// Writes to slice index is concurrent safe, unlike maps.
			users[i] = u

			// For each photo, fetch photo tags.
			wg.Add(len(photos))
			for j := range photos {

				go func(j int) {
					defer wg.Done()

					tags, err := tagLoader.Load(photos[j].ID)
					if err != nil {
						// TODO: Handle error later.
						panic(err)
					}

					users[i].Photos[j].Tags = tags
				}(j)
			}
		}(i)
	}
	wg.Wait()

	for i, u := range users {
		fmt.Printf("%d: %+v, photos: %d\n", i+1, u.ID, len(u.Photos)) // user 5, 10, 20, 30, 40 and 50 will be returned.
		for _, p := range u.Photos {
			fmt.Printf("\t%d, tags: %d\n", p.ID, len(p.Tags)) // user 5, 10, 20, 30, 40 and 50 will be returned.
		}

		fmt.Println()
	}
}

type User struct {
	ID     int
	Photos []Photo
}

type Photo struct {
	ID     int
	UserID int
	Tags   []string
}

func batchUserFn(ctx context.Context, userIds []int) (map[int]User, error) {
	fmt.Println("user:userIds:", userIds)

	users, err := fetchUsers(ctx, userIds)
	if err != nil {
		return nil, err
	}

	byID := func(u User) int {
		return u.ID
	}

	// Group the users by id. if the key does exists, an error KeyError will be
	// returned.
	return dataloader3.GroupBy(users, byID), nil
}

func batchPhotoFn(ctx context.Context, userIds []int) (map[int][]Photo, error) {
	fmt.Println("photo:userIds:", userIds)
	res := make([][]Photo, len(userIds))

	var wg sync.WaitGroup
	wg.Add(len(userIds))

	for i := range userIds {
		go func(i int) {
			defer wg.Done()

			photos, err := fetchPhotosByUserID(ctx, userIds[i])
			if err != nil {
				// TODO: Handle it later.
				panic(err)
			}

			res[i] = photos
		}(i)
	}

	wg.Wait()

	byUserID := func(photos []Photo) int {
		return photos[0].UserID
	}

	return dataloader3.GroupBy(res, byUserID), nil
}

func batchTagFn(ctx context.Context, photoIds []int) (map[int][]string, error) {
	fmt.Println("tag:photoIds:", photoIds)
	res := make([][]string, len(photoIds))

	var wg sync.WaitGroup
	wg.Add(len(photoIds))

	for i := range photoIds {
		go func(i int) {
			defer wg.Done()

			tags, err := fetchTagsByPhotoID(ctx, photoIds[i])
			if err != nil {
				// TODO: Handle it later.
				panic(err)
			}

			res[i] = tags
		}(i)
	}

	wg.Wait()

	byPhotoID := func(i int) int {
		return photoIds[i]
	}

	return dataloader3.GroupByIndex(res, byPhotoID), nil
}

func fetchUsers(ctx context.Context, ids []int) ([]User, error) {
	var users []User

	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

	// DataLoader is guaranteed to provide unique keys for
	// fetching.
	for _, id := range ids {
		users = append(users, User{ID: id})
	}

	return users, nil
}

func fetchPhotosByUserID(ctx context.Context, userID int) ([]Photo, error) {
	var photos []Photo

	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

	// DataLoader is guaranteed to provide unique keys for
	// fetching.
	n := rand.Intn(20) + 1
	for i := 0; i < n; i++ {
		photos = append(photos, Photo{
			ID:     i,
			UserID: userID,
		})
	}

	return photos, nil
}

func fetchTagsByPhotoID(ctx context.Context, photoID int) ([]string, error) {
	var tags []string

	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

	// DataLoader is guaranteed to provide unique keys for
	// fetching.
	n := rand.Intn(20) + 1
	for i := 0; i < n; i++ {
		tags = append(tags, fmt.Sprint(i))
	}

	return tags, nil
}
```
