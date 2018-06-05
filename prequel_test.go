package prequel

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"syreclabs.com/go/prequel/builder"
)

var (
	doTest bool
	db     *DB
)

func init() {
	Connect()
}

func Connect() {
	dsn := os.Getenv("SQLX_POSTGRES_DSN")
	doTest = dsn != "" && dsn != "skip"

	if doTest {
		sqlxdb, err := sqlx.Connect("postgres", dsn)
		if err != nil {
			fmt.Printf("sqlx.Connect: %#v\n", err)
			doTest = false
		}
		db = &DB{sqlxdb}
	}
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
	defer func() {
		execMulti(ctx, db, schema.drop)
	}()
	execMulti(ctx, db, schema.create)
	testFunc()
}

func loadFixtures() {
	tx := db.MustBegin(context.Background())
	tx.Tx.MustExec(tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "First", "Last", "user@example.com")
	tx.Tx.MustExec(tx.Rebind("INSERT INTO users (first_name, last_name, email) VALUES (?, ?, ?)"), "John", "Doe", "johndoe@mail.net")
	tx.Commit()
}

func TestSelectAll(t *testing.T) {
	withSchema(context.Background(), func() {
		loadFixtures()

		b := builder.
			Select("first_name", "last_name", "email").
			From("users")

		var users []*User
		if err := db.Select(context.Background(), b, &users); err != nil {
			t.Fatal(err)
		}
		if len(users) != 2 {
			t.Fatalf("expected to get %d records, got %d", 2, len(users))
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
				Select("first_name", "last_name", "email").
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
				Where("last_name IN ($1)", []string{"Last"})

			var users []*User
			if err := db.Select(context.Background(), b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 1 {
				t.Fatalf("expected to get %d records, got %d", 1, len(users))
			}
			if users[0].LastName != "Last" {
				t.Errorf("expected LastName %q, got %q", "Last", users[0].LastName)
			}
		})

	})
}