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
		fmt.Printf("%s is not set, some tests will be skipped", dsnEnv)
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

func execMulti(ctx context.Context, e Execer, query string) error {
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

func withSchema(ctx context.Context, testFunc func()) {
	if !doTest {
		return
	}
	defer func() {
		execMulti(ctx, db.DB, schema.drop)
	}()
	execMulti(ctx, db.DB, schema.create)
	testFunc()
}

func loadFixtures() {
	tx := db.MustBegin(context.Background())
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "First", "Last", "user@example.com")
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "Johnny", "Doe", "john@mail.net")
	tx.Tx.MustExec(tx.Tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "Janie", "Roe", "jane@lame.net")
	tx.Commit()
}

func TestSelectAll(t *testing.T) {
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Select("id", "first_name", "last_name", "email").
			From("users")

		var users []*User
		if err := db.Select(context.Background(), b, &users); err != nil {
			t.Fatal(err)
		}
		if len(users) != 3 {
			t.Fatalf("expected to get %d records, got %d", 3, len(users))
		}
	})
}

func TestSelectWhere(t *testing.T) {
	withSchema(context.Background(), func() {
		loadFixtures()

		t.Run("Single", func(t *testing.T) {
			b := builder.
				Select("first_name", "last_name", "email").
				From("users").
				Where("email = $1", "user@example.com")

			var users []*User
			if err := db.Select(context.Background(), b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 {
				t.Fatalf("expected to get %d records, got %d", 1, len(users))
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
			if err := db.Select(context.Background(), b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 {
				t.Fatalf("expected to get %d records, got %d", 1, len(users))
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
			if err := db.Select(context.Background(), b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 2 {
				t.Fatalf("expected to get %d records, got %d", 1, len(users))
			}
			if users[0].LastName != "Doe" {
				t.Errorf("expected LastName %q, got %q", "Last", users[0].LastName)
			}
		})
	})
}

func TestGet(t *testing.T) {
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Select("first_name", "last_name", "email").
			From("users").
			Where("email = $1", "john@mail.net")

		var user User
		if err := db.Get(context.Background(), b, &user); err != nil {
			t.Fatal(err)
		}
		if user.Email != "john@mail.net" {
			t.Fatalf("expected to Email %q, got %q", "john@mail.net", user.Email)
		}
	})
}

func TestExecInsert(t *testing.T) {
	withSchema(context.Background(), func() {
		loadFixtures()

		t.Run("Single", func(t *testing.T) {
			b := builder.
				Insert("users").
				Columns("first_name", "last_name", "email").
				Values("Jane", "Doe", "janedoe@mymail.com")

			res, err := db.Exec(context.Background(), b)
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

			res, err := db.Exec(context.Background(), b)
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
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Update("users").
			Set("last_name = $1", "Another").
			Where("email = $1", "user@example.com")

		res, err := db.Exec(context.Background(), b)
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
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Delete("users").
			Where("email = $1", "user@example.com")

		res, err := db.Exec(context.Background(), b)
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
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Select("first_name", "last_name", "email").
			From("users").
			Where("email = $1", "john@mail.net")

		tx, err := db.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Commit()

		var user User
		if err := tx.Get(context.Background(), b, &user); err != nil {
			t.Fatal(err)
		}
		if user.Email != "john@mail.net" {
			t.Fatalf("expected to Email %q, got %q", "john@mail.net", user.Email)
		}
	})
}
