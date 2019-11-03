package dbx

import (
	"fmt"
	"log"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestTransaction(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("insert into cars (name,price) values('buick', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := checkOnlyOneRow(db, "buick"); err != nil {
		t.Fatal(err)
	}
}

func TestTransaction2(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("insert into cars (name,price) values('buick', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := checkOnlyOneRow(db, ""); err == nil || err.Error() != "No results" {
		t.Fatal("Should fail: no results")
	}
}

func TestTransaction3(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("insert into cars (name,price) values('buick', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := checkOnlyOneRow(db, "buick"); err != nil {
		t.Fatal(err)
	}
}

func TestTransaction4(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("insert into cars (name,price) values('buick', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := checkOnlyOneRow(db, ""); err == nil || err.Error() != "No results" {
		t.Fatal("Should fail: no results")
	}
}

func TestTransaction5(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := db.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("insert into cars (name,price) values('buick', 0)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatal(err)
	}

	err = db.Commit()
	if err == nil || !strings.Contains(err.Error(), "No transaction open") {
		t.Fatal("Should fail: No transaction open")
	}
}

func checkNoRows(db *DB) error {
	table, err := db.Query("select name from cars")
	if err != nil {
		return err
	}
	if len(table.Rows) != 0 {
		return fmt.Errorf("1 or more rows")
	}
	return nil
}

func checkOnlyOneRow(db *DB, name string) error {
	table, err := db.Query("select name from cars")
	if err != nil {
		return err
	}
	if len(table.Rows) == 0 {
		return fmt.Errorf("No results")
	}
	r := table.Rows[0]
	if r.Values[0].(string) != name {
		return fmt.Errorf("invalid name: %s", r.Values[0].(string))
	}
	return nil
}

func TestSelect(t *testing.T) {
	db, err := initDb(1)
	if err != nil {
		log.Fatal(err)
	}

	table, err := db.Query("select name from cars")
	if err != nil {
		t.Fatal(err)
	}

	r := table.Rows[0]
	if r.Values[0].(string) != "ford" {
		t.Fail()
	}
}

func TestInsert(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	r, err := db.Exec("insert into cars values(7, 'ford', 99)")
	if err != nil {
		t.Fatal(err)
	}

	i, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}

	if i != 7 {
		t.Fail()
	}
}

func TestInsert2(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	r, err := db.Exec("insert into cars (name, price) values (?, ?)", "ford", 99)
	if err != nil {
		t.Fatal(err)
	}

	i, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}

	if i != 1 {
		t.Fail()
	}
}

func TestInsert3(t *testing.T) {
	db, err := initDb(0)
	if err != nil {
		log.Fatal(err)
	}

	r, err := db.Exec("insert into cars (name, price) values('ford', 99)")
	if err != nil {
		t.Fatal(err)
	}

	i, err := r.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}

	if i != 1 {
		t.Fail()
	}
}

func initDb(rows int) (*DB, error) {
	orm, err := Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	_, err = orm.DB.Exec("CREATE TABLE cars(id INTEGER PRIMARY KEY, name TEXT, price INTEGER);")
	if err != nil {
		return nil, err
	}

	for i := 0; i < rows; i++ {
		if _, err := orm.Exec("insert into cars (name,price) values('ford', 99)"); err != nil {
			return nil, err
		}
	}

	return orm, nil
}

func BenchmarkSelect(b *testing.B) {
	db, err := initDb(30)
	if err != nil {
		log.Fatal(err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		var price int
		var name string

		rows, err := db.DB.Query("select price,name from cars")
		for rows.Next() {
			err = rows.Scan(&price, &name)
			if err != nil {
				b.Fatal(err)
			}
		}

		if price != 99 || name != "ford" {
			b.Fail()
		}
	}
}

func BenchmarkSelectSQX(b *testing.B) {
	db, err := initDb(30)
	if err != nil {
		log.Fatal(err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		table, err := db.Query("select price,name from cars")
		if err != nil {
			b.Fatal(err)
		}

		r := table.Rows[0]
		if r.Values[0].(int) != 99 {
			b.Fail()
		}

		if r.Values[1].(string) != "ford" {
			b.Fail()
		}
	}
}
