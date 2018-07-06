PostgreSQL query bulder and executor.

[![GoDoc](https://godoc.org/syreclabs.com/go/prequel?status.svg)](https://godoc.org/syreclabs.com/go/prequel)
[![Build Status](https://travis-ci.org/syreclabs/prequel.svg?branch=master)](https://travis-ci.org/syreclabs/prequel)

### Requirements:

- Go >= 1.9
- PostgreSQL >= 9.5

### Installation

    go get -u syreclabs.com/go/prequel

_prequel_ is a fast and lightweight PostgreSQL query bulder and runner which uses `github.com/jmoiron/sqlx` under the hood.

A few examples:

#### SELECT

db.Select() allows querying slice of values:

```go
b := builder.
    Select("first_name", "last_name", "email").
    From("users").
    Where("email = $1", "user@example.com")

var users []*User
if err := db.Select(ctx, b, &users); err != nil {
    return err
}
```

```sql
SELECT first_name, last_name, email FROM users WHERE (email = $1) [user@example.com] 320.096µs
```

Use db.Get() to get a single result:

```go
b := builder.
    Select("first_name", "last_name", "email").
    From("users").
    Where("email = $1", "john@mail.net")

var user User
if err := db.Get(ctx, b, &user); err != nil {
    return err
}
```

Multiple Where() are joined with `AND`:

```go
b := builder.
    Select("first_name", "last_name", "email", "created_at").
    From("users").
    Where("email = $1", "user@example.com").
    Where("first_name = $1", "First").
    Where("created_at < $1", time.Now().Add(10*time.Second))
```

```sql
SELECT first_name, last_name, email, created_at FROM users WHERE (email = $1) AND (first_name = $2) AND (created_at < $3) [user@example.com First 2018-07-05 21:19:47.710477716 -0500 -05 m=+10.013333066] 501.125µs
```

Slice parameters are rewritten so they can be used in `IN`:

```go
b := builder.
    Select("first_name", "last_name", "email").
    From("users").
    Where("last_name IN ($1)", []string{"Last", "Doe", "Somebody", "Else"}).
    OrderBy("first_name DESC")
```

```sql
SELECT first_name, last_name, email FROM users WHERE (last_name IN ($1,$2,$3,$4)) ORDER BY first_name DESC [Last Doe Somebody Else] 266.623µs
```

`UNION`s are supported too:

```go
b := builder.
    Select("id", "first_name", "last_name", "email").
    From("users").
    Where("id = $1", 1).
    Union(false,
        builder.
            Select("id", "first_name", "last_name", "email").
            From("users").
            Where("id IN ($1)", []int64{1, 2})).
    Union(true,
        builder.
            Select("id", "first_name", "last_name", "email").
            From("users").
            Where("id IN ($1)", []int64{1, 2})).
    OrderBy("id")
```

```sql
SELECT id, first_name, last_name, email FROM users WHERE (id = $1) UNION SELECT id, first_name, last_name, email FROM users WHERE (id IN ($2,$3)) UNION ALL SELECT id, first_name, last_name, email FROM users WHERE (id IN ($4,$5)) ORDER BY id [1 1 2 1 2] 664.952µs
```

... as well as `DISTINCT`, `GROUP BY`, `HAVING`, `ORDER BY`, `OFFSET`, `LIMIT` and `WITH` queries (see builder/select_test.go for examples).

#### INSERT

Single row:

```go
b := builder.
    Insert("users").
    Columns("first_name", "last_name", "email").
    Values("Jane", "Doe", "janedoe@mymail.com")

res, _ := db.Exec(ctx, b)
```

```sql
INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3) [Jane Doe janedoe@mymail.com] 189.578µs
```

Multiple row inserts are supported too:

```go
b := builder.
    Insert("users").
    Columns("first_name", "last_name", "email").
    Values("Jane", "Doe", "janie@notmail.me").
    Values("John", "Roe", "john@notmail.me").
    Values("Max", "Rockatansky", "maxrockatansky@notmail.me")

res, _ := db.Exec(ctx, b)
```

```sql
INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) [Jane Doe janie@notmail.me John Roe john@notmail.me Max Rockatansky maxrockatansky@notmail.me] 220.521µs
```

`OnConflictDoNothing()` can be used to control PostgreSQL `ON CONFLICT` behaviour:

## TODO

#### UPDATE and DELETE

`UPDATE` and `DELETE` are straightforward:

```go
b := builder.
    Update("users").
    Set("last_name = $1", "Another").
    Where("email = $1", "user@example.com")

res, _ := db.Exec(ctx, b)
```

```sql
UPDATE users SET last_name = $1 WHERE (email = $2) [Another user@example.com] 158.102µs
```

```go
b := builder.
    Delete("users").
    Where("email = $1", "user@example.com")

res, _ := db.Exec(ctx, b)
```

```sql
DELETE FROM users WHERE (email = $1) [user@example.com] 190.161µs
```

#### Upsert

Upsert is implemented using PostgreSQL `ON CONFLICT` clause:

```go
b := builder.
    Upsert("users", "(email)").
    Columns("first_name", "last_name", "email").
    Values("Simple", "Last", "user@example.com")

res, _ := db.Exec(ctx, b)
```

```sql
INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3) ON CONFLICT (email) DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email [Simple Last user@example.com] 271.08µs
```

`target` arg accepts conditions and parameters:

```go
b := builder.
    Upsert("users", "(email) WHERE email != $1", "janie@notmail.me").
    Columns("first_name", "last_name", "email").
    Values("Complex", "Last", "user@example.com")

res, _ := db.Exec(ctx, b)
```

```sql
INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3) ON CONFLICT (email) WHERE email != $4 DO NOTHING [Wax Rockatansky maxrockatansky@notmail.me janie@notmail.me] 193.868µs
```

and upsert-ing multiple rows is also supported:

```go
b := builder.
    Upsert("users", "(email)").
    Columns("first_name", "last_name", "email").
    Values("Jane", "Doe", "janie@notmail.me").
    Values("John", "Roe", "john@notmail.me").
    Values("Max", "Rockatansky", "user@example.com")

res, _ := db.Exec(ctx, b)
```

```sql
INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) ON CONFLICT (email) DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email [Jane Doe janie@notmail.me John Roe john@notmail.me Max Rockatansky user@example.com] 199.644µs
```

#### Insect

```go
b := builder.Insect("users").
    Columns("first_name", "last_name", "email").
    Values(user.FirstName, user.LastName, user.Email).
    Where("email = $1", user.Email).
    Returning("*")

var users []*User
_ := db.Select(ctx, b, &users)
```

```sql
WITH sel AS (SELECT * FROM users WHERE (email = $1)), ins AS (INSERT INTO users (first_name, last_name, email) SELECT $2, $3, $4 WHERE (NOT EXISTS(SELECT * FROM sel)) RETURNING *) SELECT * FROM ins UNION ALL SELECT * FROM sel [user@example.com First Last user@example.com] 410.672µs
```
