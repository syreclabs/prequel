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
	const dsnEnv = "PREQUEL_TEST_DSN"

	dsn := os.Getenv(dsnEnv)
	doTest = dsn != ""

	if !doTest {
		fmt.Printf("%s is not set, some tests will be skipped\n", dsnEnv)
		return
	}

	var err error
	db, err = Connect(context.Background(), "postgres", dsn)
	if err != nil {
		fmt.Printf("Connect: %v\n", err)
		doTest = false
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
		created_at timestamp without time zone default now(),
		UNIQUE(email)
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
	tx.Tx.MustExec("INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3)", "First", "Last", "user@example.com")
	tx.Tx.MustExec("INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3)", "Johnny", "Doe", "john@mail.net")
	tx.Tx.MustExec("INSERT INTO users (first_name, last_name, email) VALUES ($1, $2, $3)", "Janie", "Roe", "janie@email.com")
	tx.Commit()
}

var _ Runner = (*DB)(nil)
var _ Runner = (*Conn)(nil)
var _ Runner = (*Tx)(nil)

// var _ Runner = (*Stmt)(nil)

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

		t.Run("Union", func(t *testing.T) {
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

			var users []*User
			if err := db.Select(ctx, b, &users); err != nil {
				t.Fatal(err)
			}
			if len(users) != 4 {
				t.Fatalf("expected %d records, got %d", 4, len(users))
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

		t.Run("WithQuery", func(t *testing.T) {
			b := builder.
				Select("users.first_name", "users.last_name", "users.email").
				With("u2", builder.Select("first_name", "last_name").From("users")).
				From("users").
				From("INNER JOIN u2 ON u2.first_name = users.first_name").
				Where("users.last_name IN ($1)", []string{"Last", "Doe", "Somebody", "Else"}).
				OrderBy("users.first_name DESC")

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
				t.Fatalf("expected RowsAffected to be %d, got %d", 3, rows)
			}
		})

		t.Run("WithQuery", func(t *testing.T) {
			b := builder.
				Insert("users").
				Columns("first_name", "last_name", "email").
				With("u2", builder.Select("first_name", "last_name", "'test@notmail.me' as email").
					From("users").
					Where("email = $1", "maxrockatansky@notmail.me")).
				From(builder.Select("first_name", "last_name", "email").From("u2"))

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

		t.Run("OnConflict", func(t *testing.T) {
			t.Run("WithoutTarget", func(t *testing.T) {
				b := builder.
					Insert("users").
					Columns("first_name", "last_name", "email").
					Values("Wax", "Rockatansky", "maxrockatansky@notmail.me").
					OnConflictDoNothing("")

				res, err := db.Exec(ctx, b)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := res.RowsAffected()
				if err != nil {
					t.Fatal(err)
				}
				if rows != 0 {
					t.Fatalf("expected RowsAffected to be %d, got %d", 0, rows)
				}
			})

			t.Run("WithSimpleTarget", func(t *testing.T) {
				for _, target := range []string{"(email)", "ON CONSTRAINT users_email_key"} {
					b := builder.
						Insert("users").
						Columns("first_name", "last_name", "email").
						Values("Wax", "Rockatansky", "maxrockatansky@notmail.me").
						OnConflictDoNothing(target)

					res, err := db.Exec(ctx, b)
					if err != nil {
						t.Fatal(err)
					}
					rows, err := res.RowsAffected()
					if err != nil {
						t.Fatal(err)
					}
					if rows != 0 {
						t.Fatalf("expected RowsAffected to be %d, got %d", 0, rows)
					}
				}
			})

			t.Run("WithComplexTarget", func(t *testing.T) {
				b := builder.
					Insert("users").
					Columns("first_name", "last_name", "email").
					Values("Wax", "Rockatansky", "maxrockatansky@notmail.me").
					OnConflictDoNothing("(email) WHERE email != $1", "janie@notmail.me")

				res, err := db.Exec(ctx, b)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := res.RowsAffected()
				if err != nil {
					t.Fatal(err)
				}
				if rows != 0 {
					t.Fatalf("expected RowsAffected to be %d, got %d", 0, rows)
				}
			})
		})
	})
}

func TestExecUpsert(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		t.Run("SimpleTarget", func(t *testing.T) {
			for _, target := range []string{"(email)", "ON CONSTRAINT users_email_key"} {
				b := builder.
					Upsert("users", target).
					Columns("first_name", "last_name", "email").
					Values("Simple", "Last", "user@example.com")

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
			}
		})

		t.Run("ComplexTarget", func(t *testing.T) {
			b := builder.
				Upsert("users", "(email) WHERE email != $1", "janie@notmail.me").
				Columns("first_name", "last_name", "email").
				Values("Complex", "Last", "user@example.com")

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

		t.Run("InsertEmptyPrimaryKey", func(t *testing.T) {
			user := &User{
				FirstName: "EmptyPrimaryKey",
				LastName:  "Last",
				Email:     "user0@example.com",
			}
			b := builder.
				Upsert("users", "(id)").
				Columns("id", "first_name", "last_name", "email").
				Values(builder.Default(user.Id), user.FirstName, user.LastName, user.Email)

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

		t.Run("UpsertPrimaryKeyViolation", func(t *testing.T) {
			b := builder.
				Upsert("users", "(id)").
				Columns("id", "first_name", "last_name", "email").
				Values(builder.Default(1), "PrimaryKeyViolation", "Last", "user@example.com")

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
				Upsert("users", "(email)").
				Columns("first_name", "last_name", "email").
				Values("Jane", "Doe", "janie@notmail.me").
				Values("John", "Roe", "john@notmail.me").
				Values("Max", "Rockatansky", "user@example.com")

			res, err := db.Exec(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 3 {
				t.Fatalf("expected RowsAffected to be %d, got %d", 3, rows)
			}
		})

		t.Run("CustomUpdate", func(t *testing.T) {
			b := builder.
				Upsert("users", "(email)").
				Columns("first_name", "last_name", "email").
				Values("Jane", "Doe", "janie@notmail.me").
				Values("John", "Roe", "john@notmail.me").
				Values("Max", "Rockatansky", "user@example.com").
				Update("first_name = (EXCLUDED.first_name || $1), last_name = EXCLUDED.last_name WHERE EXCLUDED.email != $2", "hm", "xxxx@notmail.me")

			res, err := db.Exec(ctx, b)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				t.Fatal(err)
			}
			if rows != 3 {
				t.Fatalf("expected RowsAffected to be %d, got %d", 3, rows)
			}
		})

		t.Run("WithQuery", func(t *testing.T) {
			b := builder.
				Upsert("users", "(email)").
				With("u2", builder.Select("'Jane' as first_name", "last_name", "email").From("users").Where("email = $1", "janie@notmail.me")).
				Columns("first_name", "last_name", "email").
				From(builder.Select("*").From("u2"))

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
	})
}

func TestExecInsect(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		// existing user
		b := builder.
			Select("id", "first_name", "last_name", "email").
			From("users").
			Where("email = $1", "user@example.com")

		var existingUser User
		if err := db.Get(ctx, b, &existingUser); err != nil {
			t.Fatal(err)
		}
		if existingUser.Email != "user@example.com" {
			t.Fatalf("expected Email %q, got %q", "john@mail.net", existingUser.Email)
		}

		t.Run("RecordExists", func(t *testing.T) {
			t.Run("Insect", func(t *testing.T) {
				b := builder.Insect("users").
					Columns("first_name", "last_name", "email").
					Values(existingUser.FirstName, existingUser.LastName, existingUser.Email).
					Where("email = $1", existingUser.Email).
					Returning("*")

				var users []*User
				if err := db.Select(ctx, b, &users); err != nil {
					t.Fatal(err)
				}
				if len(users) != 1 {
					t.Fatalf("expected %d records, got %d", 1, len(users))
				}

				if users[0].Id != 1 {
					t.Fatalf("expected first user with ID (%d), got (%d)", 1, users[0].Id)
				}

				if users[0].Email != "user@example.com" {
					t.Fatalf("expected first user with Email (%s), got (%s)", "user@example.com", users[0].Email)
				}
			})

			t.Run("Union", func(t *testing.T) {
				b := builder.
					Select("*").From("ins").
					Union(true, builder.Select("*").From("sel")).
					With("sel", builder.
						Select("*").
						From("users").
						Where("email = $1", existingUser.Email)).
					With("ins", builder.
						Insert("users").
						Columns("first_name", "last_name", "email").
						From(builder.
							Select().
							Columns("$1, $2, $3", existingUser.FirstName, existingUser.LastName, existingUser.Email).
							Where("NOT EXISTS(SELECT * FROM sel)")).
						Returning("*"))

				var users []*User
				if err := db.Select(ctx, b, &users); err != nil {
					t.Fatal(err)
				}
				if len(users) != 1 {
					t.Fatalf("expected %d records, got %d", 1, len(users))
				}

				if users[0].Id != 1 {
					t.Fatalf("expected first user with ID (%d), got (%d)", 1, users[0].Id)
				}

				if users[0].Email != "user@example.com" {
					t.Fatalf("expected first user with Email (%s), got (%s)", "user@example.com", users[0].Email)
				}
			})
		})

		t.Run("RecordNotExists", func(t *testing.T) {
			t.Run("Insect", func(t *testing.T) {
				newUser := &User{
					FirstName: "New",
					LastName:  "Last",
					Email:     "user212121222221insect@example.com",
				}

				b := builder.Insect("users").
					Columns("first_name", "last_name", "email").
					Values(newUser.FirstName, newUser.LastName, newUser.Email).
					Where("email = $1", newUser.Email).
					Returning("*")

				var users []*User
				if err := db.Select(ctx, b, &users); err != nil {
					t.Fatal(err)
				}
				if len(users) != 1 {
					t.Fatalf("expected %d records, got %d", 1, len(users))
				}

				if users[0].Email != newUser.Email {
					t.Fatalf("expected first user with Email (%s), got (%s)", newUser.Email, users[0].Email)
				}
			})

			t.Run("Union", func(t *testing.T) {
				newUser := &User{
					FirstName: "New",
					LastName:  "Last",
					Email:     "user212121222221union@example.com",
				}

				b := builder.
					Select("*").From("ins").
					Union(true, builder.Select("*").From("sel")).
					With("sel", builder.
						Select("*").
						From("users").
						Where("email = $1", newUser.Email)).
					With("ins", builder.
						Insert("users").
						Columns("first_name", "last_name", "email").
						From(builder.
							Select().
							Columns("$1, $2, $3", newUser.FirstName, newUser.LastName, newUser.Email).
							Where("NOT EXISTS(SELECT * FROM sel)")).
						Returning("*"))

				var users []*User
				if err := db.Select(ctx, b, &users); err != nil {
					t.Fatal(err)
				}
				if len(users) != 1 {
					t.Fatalf("expected %d records, got %d", 1, len(users))
				}

				if users[0].Email != newUser.Email {
					t.Fatalf("expected first user with Email (%s), got (%s)", newUser.Email, users[0].Email)
				}
			})

		})
	})
}

func TestExecUpdate(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		t.Run("Simple", func(t *testing.T) {
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

		t.Run("WithQuery", func(t *testing.T) {
			b := builder.
				Update("users").
				With("u2", builder.Select("*").From("users").Where("email = $1", "user@example.com")).
				Set("last_name = $1", "u2.last_name").
				From("u2").
				Where("users.email = $1", "user@example.com")

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
	})
}

func TestExecDelete(t *testing.T) {
	withSchema(context.Background(), func(ctx context.Context) {
		loadFixtures(ctx)

		t.Run("Simple", func(t *testing.T) {
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

		t.Run("WithQuery", func(t *testing.T) {
			b := builder.
				Delete("users").
				With("u2", builder.Select("*").From("users").Where("email = $1", "janie@email.com")).
				Where("users.id IN (SELECT id FROM u2)")

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
