package prequel

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/syreclabs/sqlx"
	"syreclabs.com/go/prequel/builder"
)

var (
	doTest bool
	db     *DB
)

func init() {
	const dsnEnv = "PREQUEL_TEST_DSN"

	dsn := os.Getenv(dsnEnv)
	doTest = dsn != "" && dsn != "skip"

	if !doTest {
		fmt.Printf("%s is not set, some tests will be skipped\n", dsnEnv)
		return
	}

	sqlxdb, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		fmt.Printf("sqlx.Connect: %#v\n", err)
		doTest = false
	}
	db = &DB{sqlxdb}
}

var schema = struct {
	create, drop string
}{
	`create table users(
		id serial primary key,
		first_name varchar(30),
		last_name text,
		email text,
		created_at timestamp without time zone default now()
	);`,

	`drop table users;`,
}

type User struct {
	Id        int       `db:"id"`
	FirstName string    `db:"first_name"`
	LastName  string    `db:"last_name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
}

func execMulti(ctx context.Context, e sqlx.ExecerContext, query string) error {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		if strings.Trim(s, " \n\t\r") == "" {
			continue
		}
		_, err := e.ExecContext(ctx, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func withSchema(ctx context.Context, testFunc func(ctx context.Context)) {
	if !doTest {
		return
	}
	defer func() {
		execMulti(ctx, db.DB, schema.drop)
	}()
	execMulti(ctx, db.DB, schema.create)
	testFunc(ctx)
}

func loadFixtures(ctx context.Context) {
	tx := db.MustBegin(ctx)
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "First", "Last", "user@example.com")
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "Johnny", "Doe", "john@mail.net")
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "Janie", "Roe", "janie@email.com")
	tx.Commit()
}

func TestSelectAll(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Select("id", "first_name", "last_name", "email").
			From("users")

		var users []*User
		if err := db.Select(ctx, b, &users); err != nil {
			t.Fatal(err)
		}
		if len(users) != 3 {
			t.Fatalf("expected %d records, got %d", 3, len(users))
		}
	})
}

func TestSelectWhere(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		t.Run("Single", func(t *testing.T) {
			b := builder.
				Select("first_name", "last_name", "email").
				From("users").
				Where("email = $1", "user@example.com")

			var users []*User
			if err := db.Select(ctx, b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 {
				t.Fatalf("expected %d records, got %d", 1, len(users))
			}
			if users[0].Email != "user@example.com" {
				t.Errorf("expected Email %q, got %q", "user@example.com", users[0].Email)
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			b := builder.
				Select("first_name", "last_name", "email", "created_at").
				From("users").
				Where("email = $1", "user@example.com").
				Where("first_name = $1", "First").
				Where("created_at < $1", time.Now().Add(10*time.Second))

			var users []*User
			if err := db.Select(ctx, b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 {
				t.Fatalf("expected %d records, got %d", 1, len(users))
			}
			if users[0].FirstName != "First" {
				t.Errorf("expected FirstName %q, got %q", "First", users[0].FirstName)
			}
		})

		t.Run("In", func(t *testing.T) {
			b := builder.
				Select("first_name", "last_name", "email").
				From("users").
				Where("last_name IN ($1)", []string{"Last", "Doe", "Somebody", "Else"}).
				OrderBy("first_name DESC")

			var users []*User
			if err := db.Select(ctx, b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 2 {
				t.Fatalf("expected %d records, got %d", 1, len(users))
			}
			if users[0].LastName != "Doe" {
				t.Errorf("expected LastName %q, got %q", "Last", users[0].LastName)
			}
		})
	})
}

func TestGet(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Select("first_name", "last_name", "email").
			From("users").
			Where("email = $1", "john@mail.net")

		var user User
		if err := db.Get(ctx, b, &user); err != nil {
			t.Fatal(err)
		}
		if user.Email != "john@mail.net" {
			t.Fatalf("expected Email %q, got %q", "john@mail.net", user.Email)
		}
	})
}

func TestExecInsert(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		t.Run("Single", func(t *testing.T) {
			b := builder.
				Insert("users").
				Columns("first_name", "last_name", "email").
				Values("Jane", "Doe", "janedoe@mymail.com")

			res, err := db.Exec(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 1 {
				t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			b := builder.
				Insert("users").
				Columns("first_name", "last_name", "email").
				Values("Jane", "Doe", "janie@notmail.me").
				Values("John", "Roe", "john@notmail.me").
				Values("Max", "Rockatansky", "maxrockatansky@notmail.me")

			res, err := db.Exec(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 3 {
				t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
			}
		})
	})
}

func TestExecUpdate(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Update("users").
			Set("last_name = $1", "Another").
			Where("email = $1", "user@example.com")

		res, err := db.Exec(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
		}
	})
}

func TestExecDelete(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Delete("users").
			Where("email = $1", "user@example.com")

		res, err := db.Exec(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
		}
	})
}

func TestTx(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Delete("users").
			Where("email = $1", "user@example.com")

		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Commit()

		res, err := tx.Exec(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
		}
	})
}

func TestConnSelect(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Select("id", "first_name", "last_name", "email").
			From("users")

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		var users []*User
		if err := conn.Select(ctx, b, &users); err != nil {
			t.Fatal(err)
		}
		if len(users) != 3 {
			t.Fatalf("expected %d records, got %d", 3, len(users))
		}
	})
}

func TestConnGet(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Select("first_name", "last_name", "email").
			From("users").
			Where("email = $1", "janie@email.com")

		conn := db.MustConn(ctx)
		defer conn.Close()

		var user User
		if err := conn.Get(ctx, b, &user); err != nil {
			t.Fatal(err)
		}
		if user.FirstName != "Janie" {
			t.Fatalf("expected FirstName %q, got %q", "Janie", user.FirstName)
		}
	})
}

func TestConnExecInsert(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		ib := builder.
			Insert("users").
			Columns("first_name", "last_name", "email").
			Values("Bob", "Bob", "bob@theirmail.com").
			Returning("id")

		conn := db.MustConn(ctx)
		defer conn.Close()

		var id int
		if err := conn.Get(ctx, ib, &id); err != nil {
			t.Fatal(err)
		}
		if id == 0 {
			t.Error("Expected Id not to be 0")
		}

		sb := builder.
			Select("first_name", "last_name", "email").
			From("users").
			Where("id = $1", id)

		var user User
		if err := conn.Get(ctx, sb, &user); err != nil {
			t.Fatal(err)
		}
		if user.Email != "bob@theirmail.com" {
			t.Fatalf("expected Email %q, got %q", "bob@theirmail.com", user.Email)
		}
	})
}

func TestConnExecUpdate(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		ub := builder.
			Update("users").
			Set("first_name = $1", "Alice").
			Where("email = $1", "janie@email.com").
			Returning("id")

		conn := db.MustConn(ctx)
		defer conn.Close()

		var ids []int
		if err := conn.Select(ctx, ub, &ids); err != nil {
			t.Fatal(err)
		}
		if len(ids) != 1 {
			t.Errorf("Expected ids to have %d elements, got %d", 1, len(ids))
		}

		sb := builder.
			Select("first_name").
			From("users").
			Where("id = $1", ids[0])

		var user User
		if err := conn.Get(ctx, sb, &user); err != nil {
			t.Fatal(err)
		}
		if user.FirstName != "Alice" {
			t.Fatalf("expected FirstName %q, got %q", "Alice", user.FirstName)
		}
	})
}

func TestConnExecDelete(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		b := builder.
			Delete("users").
			Where("email = $1", "john@mail.net")

		conn := db.MustConn(ctx)
		defer conn.Close()

		res, err := conn.Exec(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("expected RowsAffected to be %d, got %d", 1, rows)
		}
	})
}
