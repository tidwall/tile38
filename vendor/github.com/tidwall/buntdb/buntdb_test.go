package buntdb

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testOpen(t testing.TB) *DB {
	if err := os.RemoveAll("data.db"); err != nil {
		t.Fatal(err)
	}
	return testReOpen(t, nil)
}
func testReOpen(t testing.TB, db *DB) *DB {
	return testReOpenDelay(t, db, 0)
}

func testReOpenDelay(t testing.TB, db *DB, dur time.Duration) *DB {
	if db != nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(dur)
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testClose(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll("data.db")
}

func TestBackgroudOperations(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	for i := 0; i < 1000; i++ {
		if err := db.Update(func(tx *Tx) error {
			for j := 0; j < 200; j++ {
				if _, _, err := tx.Set(fmt.Sprintf("hello%d", j), "planet", nil); err != nil {
					return err
				}
			}
			if _, _, err := tx.Set("hi", "world", &SetOptions{Expires: true, TTL: time.Second / 2}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	err := db.View(func(tx *Tx) error {
		var err error
		n, err = tx.Len()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 201 {
		t.Fatalf("expecting '%v', got '%v'", 201, n)
	}
	time.Sleep(time.Millisecond * 1500)
	db = testReOpen(t, db)
	defer testClose(db)
	n = 0
	err = db.View(func(tx *Tx) error {
		var err error
		n, err = tx.Len()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("expecting '%v', got '%v'", 200, n)
	}
}
func TestSaveLoad(t *testing.T) {
	db, _ := Open(":memory:")
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 20; i++ {
			_, _, err := tx.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("planet:%d", i), nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.Close()
		os.RemoveAll("temp.db")
	}()
	if err := db.Save(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, _ = Open(":memory:")
	defer db.Close()
	f, err = os.Open("temp.db")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := db.Load(f); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		for i := 0; i < 20; i++ {
			ex := fmt.Sprintf("planet:%d", i)
			val, err := tx.Get(fmt.Sprintf("key:%d", i))
			if err != nil {
				return err
			}
			if ex != val {
				t.Fatalf("expected %s, got %s", ex, val)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMutatingIterator(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	count := 1000
	if err := db.CreateIndex("ages", "user:*:age", IndexInt); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := db.Update(func(tx *Tx) error {
			for j := 0; j < count; j++ {
				key := fmt.Sprintf("user:%d:age", j)
				val := fmt.Sprintf("%d", rand.Intn(100))
				if _, _, err := tx.Set(key, val, nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *Tx) error {
			return tx.Ascend("ages", func(key, val string) bool {
				_, err := tx.Delete(key)
				if err != ErrTxIterating {
					t.Fatal("should not be able to call Delete while iterating.")
				}
				_, _, err = tx.Set(key, "", nil)
				if err != ErrTxIterating {
					t.Fatal("should not be able to call Set while iterating.")
				}
				return true
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCaseInsensitiveIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	count := 1000
	if err := db.Update(func(tx *Tx) error {
		opts := &IndexOptions{
			CaseInsensitiveKeyMatching: true,
		}
		return tx.CreateIndexOptions("ages", "User:*:age", opts, IndexInt)
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		for j := 0; j < count; j++ {
			key := fmt.Sprintf("user:%d:age", j)
			val := fmt.Sprintf("%d", rand.Intn(100))
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		var vals []string
		err := tx.Ascend("ages", func(key, value string) bool {
			vals = append(vals, value)
			return true
		})
		if err != nil {
			return err
		}
		if len(vals) != count {
			return fmt.Errorf("expected '%v', got '%v'", count, len(vals))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

}

func TestIndexTransaction(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	var errFine = errors.New("this is fine")
	ascend := func(tx *Tx, index string) ([]string, error) {
		var vals []string
		if err := tx.Ascend(index, func(key, val string) bool {
			vals = append(vals, key, val)
			return true
		}); err != nil {
			return nil, err
		}
		return vals, nil
	}
	ascendEqual := func(tx *Tx, index string, vals []string) error {
		vals2, err := ascend(tx, index)
		if err != nil {
			return err
		}
		if len(vals) != len(vals2) {
			return errors.New("invalid size match")
		}
		for i := 0; i < len(vals); i++ {
			if vals[i] != vals2[i] {
				return errors.New("invalid order")
			}
		}
		return nil
	}
	// test creating an index and adding items
	if err := db.Update(func(tx *Tx) error {
		tx.Set("1", "3", nil)
		tx.Set("2", "2", nil)
		tx.Set("3", "1", nil)
		if err := tx.CreateIndex("idx1", "*", IndexInt); err != nil {
			return err
		}
		if err := ascendEqual(tx, "idx1", []string{"3", "1", "2", "2", "1", "3"}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test to see if the items persisted from previous transaction
	// test add item.
	// test force rollback.
	if err := db.Update(func(tx *Tx) error {
		if err := ascendEqual(tx, "idx1", []string{"3", "1", "2", "2", "1", "3"}); err != nil {
			return err
		}
		tx.Set("4", "0", nil)
		if err := ascendEqual(tx, "idx1", []string{"4", "0", "3", "1", "2", "2", "1", "3"}); err != nil {
			return err
		}
		return errFine
	}); err != errFine {
		t.Fatalf("expected '%v', got '%v'", errFine, err)
	}

	// test to see if the rollback happened
	if err := db.View(func(tx *Tx) error {
		if err := ascendEqual(tx, "idx1", []string{"3", "1", "2", "2", "1", "3"}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("expected '%v', got '%v'", nil, err)
	}

	// del item, drop index, rollback
	if err := db.Update(func(tx *Tx) error {
		if err := tx.DropIndex("idx1"); err != nil {
			return err
		}
		return errFine
	}); err != errFine {
		t.Fatalf("expected '%v', got '%v'", errFine, err)
	}

	// test to see if the rollback happened
	if err := db.View(func(tx *Tx) error {
		if err := ascendEqual(tx, "idx1", []string{"3", "1", "2", "2", "1", "3"}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("expected '%v', got '%v'", nil, err)
	}

	various := func(reterr error) error {
		// del item 3, add index 2, add item 4, test index 1 and 2.
		// flushdb, test index 1 and 2.
		// add item 1 and 2, add index 2 and 3, test index 2 and 3
		return db.Update(func(tx *Tx) error {
			tx.Delete("3")
			tx.CreateIndex("idx2", "*", IndexInt)
			tx.Set("4", "0", nil)
			if err := ascendEqual(tx, "idx1", []string{"4", "0", "2", "2", "1", "3"}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			if err := ascendEqual(tx, "idx2", []string{"4", "0", "2", "2", "1", "3"}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			tx.DeleteAll()
			if err := ascendEqual(tx, "idx1", []string{}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			if err := ascendEqual(tx, "idx2", []string{}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			tx.Set("1", "3", nil)
			tx.Set("2", "2", nil)
			tx.CreateIndex("idx1", "*", IndexInt)
			tx.CreateIndex("idx2", "*", IndexInt)
			if err := ascendEqual(tx, "idx1", []string{"2", "2", "1", "3"}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			if err := ascendEqual(tx, "idx2", []string{"2", "2", "1", "3"}); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			return reterr
		})
	}
	// various rollback
	if err := various(errFine); err != errFine {
		t.Fatalf("expected '%v', got '%v'", errFine, err)
	}
	// test to see if the rollback happened
	if err := db.View(func(tx *Tx) error {
		if err := ascendEqual(tx, "idx1", []string{"3", "1", "2", "2", "1", "3"}); err != nil {
			return fmt.Errorf("err: %v", err)
		}
		if err := ascendEqual(tx, "idx2", []string{"3", "1", "2", "2", "1", "3"}); err != ErrNotFound {
			return fmt.Errorf("err: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("expected '%v', got '%v'", nil, err)
	}

	// various commit
	if err := various(nil); err != nil {
		t.Fatalf("expected '%v', got '%v'", nil, err)
	}

	// test to see if the commit happened
	if err := db.View(func(tx *Tx) error {
		if err := ascendEqual(tx, "idx1", []string{"2", "2", "1", "3"}); err != nil {
			return fmt.Errorf("err: %v", err)
		}
		if err := ascendEqual(tx, "idx2", []string{"2", "2", "1", "3"}); err != nil {
			return fmt.Errorf("err: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("expected '%v', got '%v'", nil, err)
	}
}

func TestDeleteAll(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)

	db.Update(func(tx *Tx) error {
		tx.Set("hello1", "planet1", nil)
		tx.Set("hello2", "planet2", nil)
		tx.Set("hello3", "planet3", nil)
		return nil
	})
	db.CreateIndex("all", "*", IndexString)
	db.Update(func(tx *Tx) error {
		tx.Set("hello1", "planet1.1", nil)
		tx.DeleteAll()
		tx.Set("bb", "11", nil)
		tx.Set("aa", "**", nil)
		tx.Delete("aa")
		tx.Set("aa", "22", nil)
		return nil
	})
	var res string
	var res2 string
	db.View(func(tx *Tx) error {
		tx.Ascend("", func(key, val string) bool {
			res += key + ":" + val + "\n"
			return true
		})
		tx.Ascend("all", func(key, val string) bool {
			res2 += key + ":" + val + "\n"
			return true
		})
		return nil
	})
	if res != "aa:22\nbb:11\n" {
		t.Fatal("fail")
	}
	if res2 != "bb:11\naa:22\n" {
		t.Fatal("fail")
	}
	db = testReOpen(t, db)
	defer testClose(db)
	res = ""
	res2 = ""
	db.CreateIndex("all", "*", IndexString)
	db.View(func(tx *Tx) error {
		tx.Ascend("", func(key, val string) bool {
			res += key + ":" + val + "\n"
			return true
		})
		tx.Ascend("all", func(key, val string) bool {
			res2 += key + ":" + val + "\n"
			return true
		})
		return nil
	})
	if res != "aa:22\nbb:11\n" {
		t.Fatal("fail")
	}
	if res2 != "bb:11\naa:22\n" {
		t.Fatal("fail")
	}
	db.Update(func(tx *Tx) error {
		tx.Set("1", "1", nil)
		tx.Set("2", "2", nil)
		tx.Set("3", "3", nil)
		tx.Set("4", "4", nil)
		return nil
	})
	err := db.Update(func(tx *Tx) error {
		tx.Set("1", "a", nil)
		tx.Set("5", "5", nil)
		tx.Delete("2")
		tx.Set("6", "6", nil)
		tx.DeleteAll()
		tx.Set("7", "7", nil)
		tx.Set("8", "8", nil)
		tx.Set("6", "c", nil)
		return errors.New("please rollback")
	})
	if err == nil || err.Error() != "please rollback" {
		t.Fatal("expecteding 'please rollback' error")
	}

	res = ""
	res2 = ""
	db.View(func(tx *Tx) error {
		tx.Ascend("", func(key, val string) bool {
			res += key + ":" + val + "\n"
			return true
		})
		tx.Ascend("all", func(key, val string) bool {
			res2 += key + ":" + val + "\n"
			return true
		})
		return nil
	})
	if res != "1:1\n2:2\n3:3\n4:4\naa:22\nbb:11\n" {
		t.Fatal("fail")
	}
	if res2 != "1:1\nbb:11\n2:2\naa:22\n3:3\n4:4\n" {
		t.Fatal("fail")
	}
}

func TestAscendEqual(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 300; i++ {
			_, _, err := tx.Set(fmt.Sprintf("key:%05dA", i), fmt.Sprintf("%d", i+1000), nil)
			if err != nil {
				return err
			}
			_, _, err = tx.Set(fmt.Sprintf("key:%05dB", i), fmt.Sprintf("%d", i+1000), nil)
			if err != nil {
				return err
			}
		}
		return tx.CreateIndex("num", "*", IndexInt)
	}); err != nil {
		t.Fatal(err)
	}
	var res []string
	if err := db.View(func(tx *Tx) error {
		return tx.AscendEqual("", "key:00055A", func(key, value string) bool {
			res = append(res, key)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(res))
	}
	if res[0] != "key:00055A" {
		t.Fatalf("expected %v, got %v", "key:00055A", res[0])
	}
	res = nil
	if err := db.View(func(tx *Tx) error {
		return tx.AscendEqual("num", "1125", func(key, value string) bool {
			res = append(res, key)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("expected %v, got %v", 2, len(res))
	}
	if res[0] != "key:00125A" {
		t.Fatalf("expected %v, got %v", "key:00125A", res[0])
	}
	if res[1] != "key:00125B" {
		t.Fatalf("expected %v, got %v", "key:00125B", res[1])
	}
}
func TestDescendEqual(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 300; i++ {
			_, _, err := tx.Set(fmt.Sprintf("key:%05dA", i), fmt.Sprintf("%d", i+1000), nil)
			if err != nil {
				return err
			}
			_, _, err = tx.Set(fmt.Sprintf("key:%05dB", i), fmt.Sprintf("%d", i+1000), nil)
			if err != nil {
				return err
			}
		}
		return tx.CreateIndex("num", "*", IndexInt)
	}); err != nil {
		t.Fatal(err)
	}
	var res []string
	if err := db.View(func(tx *Tx) error {
		return tx.DescendEqual("", "key:00055A", func(key, value string) bool {
			res = append(res, key)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(res))
	}
	if res[0] != "key:00055A" {
		t.Fatalf("expected %v, got %v", "key:00055A", res[0])
	}
	res = nil
	if err := db.View(func(tx *Tx) error {
		return tx.DescendEqual("num", "1125", func(key, value string) bool {
			res = append(res, key)
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("expected %v, got %v", 2, len(res))
	}
	if res[0] != "key:00125B" {
		t.Fatalf("expected %v, got %v", "key:00125B", res[0])
	}
	if res[1] != "key:00125A" {
		t.Fatalf("expected %v, got %v", "key:00125A", res[1])
	}
}
func TestVariousTx(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("hello", "planet", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	errBroken := errors.New("broken")
	if err := db.Update(func(tx *Tx) error {
		_, _, _ = tx.Set("hello", "world", nil)
		return errBroken
	}); err != errBroken {
		t.Fatalf("did not correctly receive the user-defined transaction error.")
	}
	var val string
	err := db.View(func(tx *Tx) error {
		var err error
		val, err = tx.Get("hello")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if val == "world" {
		t.Fatal("a rollbacked transaction got through")
	}
	if val != "planet" {
		t.Fatalf("expecting '%v', got '%v'", "planet", val)
	}
	if err := db.Update(func(tx *Tx) error {
		tx.db = nil
		if _, _, err := tx.Set("hello", "planet", nil); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		if _, err := tx.Delete("hello"); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		if _, err := tx.Get("hello"); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		tx.db = db
		tx.writable = false
		if _, _, err := tx.Set("hello", "planet", nil); err != ErrTxNotWritable {
			t.Fatal("expecting a tx not writable error")
		}
		if _, err := tx.Delete("hello"); err != ErrTxNotWritable {
			t.Fatal("expecting a tx not writable error")
		}
		tx.writable = true
		if _, err := tx.Get("something"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		if _, err := tx.Delete("something"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		if _, _, err := tx.Set("var", "val", &SetOptions{Expires: true, TTL: 0}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Get("var"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		if _, err := tx.Delete("var"); err != ErrNotFound {
			tx.unlock()
			t.Fatalf("expecting not found error")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test non-managed transactions
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, _, err = tx.Set("howdy", "world", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	v, err := tx.Get("howdy")
	if err != nil {
		t.Fatal(err)
	}
	if v != "world" {
		t.Fatalf("expecting '%v', got '%v'", "world", v)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	v, err = tx.Get("howdy")
	if err != nil {
		t.Fatal(err)
	}
	if v != "world" {
		t.Fatalf("expecting '%v', got '%v'", "world", v)
	}
	_, err = tx.Delete("howdy")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// test for invalid commits
	if err := db.Update(func(tx *Tx) error {
		// we are going to do some hackery
		defer func() {
			if v := recover(); v != nil {
				if v.(string) != "managed tx commit not allowed" {
					t.Fatal(v.(string))
				}
			}
		}()
		return tx.Commit()
	}); err != nil {
		t.Fatal(err)
	}

	// test for invalid commits
	if err := db.Update(func(tx *Tx) error {
		// we are going to do some hackery
		defer func() {
			if v := recover(); v != nil {
				if v.(string) != "managed tx rollback not allowed" {
					t.Fatal(v.(string))
				}
			}
		}()
		return tx.Rollback()
	}); err != nil {
		t.Fatal(err)
	}

	// test for closed transactions
	if err := db.Update(func(tx *Tx) error {
		tx.db = nil
		return nil
	}); err != ErrTxClosed {
		t.Fatal("expecting tx closed error")
	}
	db.mu.Unlock()

	// test for invalid writes
	if err := db.Update(func(tx *Tx) error {
		tx.writable = false
		return nil
	}); err != ErrTxNotWritable {
		t.Fatal("expecting tx not writable error")
	}
	db.mu.Unlock()
	// test for closed transactions
	if err := db.View(func(tx *Tx) error {
		tx.db = nil
		return nil
	}); err != ErrTxClosed {
		t.Fatal("expecting tx closed error")
	}
	db.mu.RUnlock()
	// flush to unwritable file
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("var1", "val1", nil)
		if err != nil {
			t.Fatal(err)
		}
		return tx.db.file.Close()
	}); err == nil {
		t.Fatal("should not be able to commit when the file is closed")
	}
	db.file, err = os.OpenFile("data.db", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.file.Seek(0, 2); err != nil {
		t.Fatal(err)
	}
	db.buf = nil
	if err := db.CreateIndex("blank", "*", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("real", "*", IndexInt); err != nil {
		t.Fatal(err)
	}
	// test scanning
	if err := db.Update(func(tx *Tx) error {
		less, err := tx.GetLess("junk")
		if err != ErrNotFound {
			t.Fatalf("expecting a not found, got %v", err)
		}
		if less != nil {
			t.Fatal("expecting nil, got a less function")
		}
		less, err = tx.GetLess("blank")
		if err != ErrNotFound {
			t.Fatalf("expecting a not found, got %v", err)
		}
		if less != nil {
			t.Fatal("expecting nil, got a less function")
		}
		less, err = tx.GetLess("real")
		if err != nil {
			return err
		}
		if less == nil {
			t.Fatal("expecting a less function, got nil")
		}
		_, _, err = tx.Set("nothing", "here", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		s := ""
		err := tx.Ascend("", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}
		if s != "hello:planet\nnothing:here\n" {
			t.Fatal("invalid scan")
		}
		tx.db = nil
		err = tx.Ascend("", func(key, val string) bool { return true })
		if err != ErrTxClosed {
			tx.unlock()
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		err = tx.Ascend("na", func(key, val string) bool { return true })
		if err != ErrNotFound {
			t.Fatal("expecting not found error")
		}
		err = tx.Ascend("blank", func(key, val string) bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		s = ""
		err = tx.AscendLessThan("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}
		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		err = tx.Descend("", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}
		if s != "nothing:here\nhello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		err = tx.DescendLessOrEqual("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}

		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		err = tx.DescendGreaterThan("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}

		if s != "nothing:here\n" {
			t.Fatal("invalid scan")
		}
		s = ""
		err = tx.DescendRange("", "liger", "apple", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}
		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test some spatial stuff
	if err := db.CreateSpatialIndex("spat", "rect:*", IndexRect); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSpatialIndex("junk", "rect:*", nil); err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		rect, err := tx.GetRect("spat")
		if err != nil {
			return err
		}
		if rect == nil {
			t.Fatal("expecting a rect function, got nil")
		}
		rect, err = tx.GetRect("junk")
		if err != ErrNotFound {
			t.Fatalf("expecting a not found, got %v", err)
		}
		if rect != nil {
			t.Fatal("expecting nil, got a rect function")
		}
		rect, err = tx.GetRect("na")
		if err != ErrNotFound {
			t.Fatalf("expecting a not found, got %v", err)
		}
		if rect != nil {
			t.Fatal("expecting nil, got a rect function")
		}
		if _, _, err := tx.Set("rect:1", "[10 10],[20 20]", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("rect:2", "[15 15],[25 25]", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("shape:1", "[12 12],[25 25]", nil); err != nil {
			return err
		}
		s := ""
		err = tx.Intersects("spat", "[5 5],[13 13]", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if err != nil {
			return err
		}
		if s != "rect:1:[10 10],[20 20]\n" {
			t.Fatal("invalid scan")
		}
		tx.db = nil
		err = tx.Intersects("spat", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != ErrTxClosed {
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		err = tx.Intersects("", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Intersects("na", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != ErrNotFound {
			t.Fatal("expecting not found error")
		}
		err = tx.Intersects("junk", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		n, err := tx.Len()
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expecting %v, got %v", 5, n)
		}
		tx.db = nil
		_, err = tx.Len()
		if err != ErrTxClosed {
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// test after closing
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error { return nil }); err != ErrDatabaseClosed {
		t.Fatalf("should not be able to perform transactionso on a closed database.")
	}
}

func Example_descKeys() {
	db, _ := Open(":memory:")
	db.CreateIndex("name", "*", IndexString)
	db.Update(func(tx *Tx) error {
		tx.Set("user:100:first", "Tom", nil)
		tx.Set("user:100:last", "Johnson", nil)
		tx.Set("user:101:first", "Janet", nil)
		tx.Set("user:101:last", "Prichard", nil)
		tx.Set("user:102:first", "Alan", nil)
		tx.Set("user:102:last", "Cooper", nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		tx.AscendKeys("user:101:*",
			func(key, value string) bool {
				fmt.Printf("%s: %s\n", key, value)
				return true
			})
		tx.AscendKeys("user:10?:*",
			func(key, value string) bool {
				fmt.Printf("%s: %s\n", key, value)
				return true
			})
		tx.AscendKeys("*2*",
			func(key, value string) bool {
				fmt.Printf("%s: %s\n", key, value)
				return true
			})
		tx.DescendKeys("user:101:*",
			func(key, value string) bool {
				fmt.Printf("%s: %s\n", key, value)
				return true
			})
		tx.DescendKeys("*",
			func(key, value string) bool {
				fmt.Printf("%s: %s\n", key, value)
				return true
			})
		return nil
	})
	// Output:
	//user:101:first: Janet
	//user:101:last: Prichard
	//user:100:first: Tom
	//user:100:last: Johnson
	//user:101:first: Janet
	//user:101:last: Prichard
	//user:102:first: Alan
	//user:102:last: Cooper
	//user:102:first: Alan
	//user:102:last: Cooper
	//user:101:last: Prichard
	//user:101:first: Janet
	//user:102:last: Cooper
	//user:102:first: Alan
	//user:101:last: Prichard
	//user:101:first: Janet
	//user:100:last: Johnson
	//user:100:first: Tom
}

func ExampleDesc() {
	db, _ := Open(":memory:")
	db.CreateIndex("last_name_age", "*", IndexJSON("name.last"), Desc(IndexJSON("age")))
	db.Update(func(tx *Tx) error {
		tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
		tx.Set("5", `{"name":{"first":"Sam","last":"Anderson"},"age":51}`, nil)
		tx.Set("6", `{"name":{"first":"Melinda","last":"Prichard"},"age":44}`, nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		tx.Ascend("last_name_age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	// Output:
	//3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
	//5: {"name":{"first":"Sam","last":"Anderson"},"age":51}
	//4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
	//1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
	//2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
	//6: {"name":{"first":"Melinda","last":"Prichard"},"age":44}
}

func ExampleDB_CreateIndex_jSON() {
	db, _ := Open(":memory:")
	db.CreateIndex("last_name", "*", IndexJSON("name.last"))
	db.CreateIndex("age", "*", IndexJSON("age"))
	db.Update(func(tx *Tx) error {
		tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		fmt.Println("Order by last name")
		tx.Ascend("last_name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age range 30-50")
		tx.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	// Output:
	// Order by last name
	// 3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
	// 4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
	// 1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
	// 2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
	// Order by age
	// 4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
	// 1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
	// 2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
	// 3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
	// Order by age range 30-50
	// 1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
	// 2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
}

func ExampleDB_CreateIndex_strings() {
	db, _ := Open(":memory:")
	db.CreateIndex("name", "*", IndexString)
	db.Update(func(tx *Tx) error {
		tx.Set("1", "Tom", nil)
		tx.Set("2", "Janet", nil)
		tx.Set("3", "Carol", nil)
		tx.Set("4", "Alan", nil)
		tx.Set("5", "Sam", nil)
		tx.Set("6", "Melinda", nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		tx.Ascend("name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	// Output:
	//4: Alan
	//3: Carol
	//2: Janet
	//6: Melinda
	//5: Sam
	//1: Tom
}

func ExampleDB_CreateIndex_ints() {
	db, _ := Open(":memory:")
	db.CreateIndex("age", "*", IndexInt)
	db.Update(func(tx *Tx) error {
		tx.Set("1", "30", nil)
		tx.Set("2", "51", nil)
		tx.Set("3", "16", nil)
		tx.Set("4", "76", nil)
		tx.Set("5", "23", nil)
		tx.Set("6", "43", nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	// Output:
	//3: 16
	//5: 23
	//1: 30
	//6: 43
	//2: 51
	//4: 76
}
func ExampleDB_CreateIndex_multipleFields() {
	db, _ := Open(":memory:")
	db.CreateIndex("last_name_age", "*", IndexJSON("name.last"), IndexJSON("age"))
	db.Update(func(tx *Tx) error {
		tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
		tx.Set("5", `{"name":{"first":"Sam","last":"Anderson"},"age":51}`, nil)
		tx.Set("6", `{"name":{"first":"Melinda","last":"Prichard"},"age":44}`, nil)
		return nil
	})
	db.View(func(tx *Tx) error {
		tx.Ascend("last_name_age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	// Output:
	//5: {"name":{"first":"Sam","last":"Anderson"},"age":51}
	//3: {"name":{"first":"Carol","last":"Anderson"},"age":52}
	//4: {"name":{"first":"Alan","last":"Cooper"},"age":28}
	//1: {"name":{"first":"Tom","last":"Johnson"},"age":38}
	//6: {"name":{"first":"Melinda","last":"Prichard"},"age":44}
	//2: {"name":{"first":"Janet","last":"Prichard"},"age":47}
}

func TestNoExpiringItem(t *testing.T) {
	item := &dbItem{key: "key", val: "val"}
	if !item.expiresAt().Equal(maxTime) {
		t.Fatal("item.expiresAt() != maxTime")
	}
	if min, max := item.Rect(nil); min != nil || max != nil {
		t.Fatal("item min,max should both be nil")
	}
}
func TestAutoShrink(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	for i := 0; i < 1000; i++ {
		err := db.Update(func(tx *Tx) error {
			for i := 0; i < 20; i++ {
				if _, _, err := tx.Set(fmt.Sprintf("HELLO:%d", i), "WORLD", nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	db = testReOpen(t, db)
	defer testClose(db)
	db.config.AutoShrinkMinSize = 64 * 1024 // 64K
	for i := 0; i < 2000; i++ {
		err := db.Update(func(tx *Tx) error {
			for i := 0; i < 20; i++ {
				if _, _, err := tx.Set(fmt.Sprintf("HELLO:%d", i), "WORLD", nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second * 3)
	db = testReOpen(t, db)
	defer testClose(db)
	err := db.View(func(tx *Tx) error {
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != 20 {
			t.Fatalf("expecting 20, got %v", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// test database format loading
func TestDatabaseFormat(t *testing.T) {
	// should succeed
	func() {
		resp := strings.Join([]string{
			"*3\r\n$3\r\nset\r\n$4\r\nvar1\r\n$4\r\n1234\r\n",
			"*3\r\n$3\r\nset\r\n$4\r\nvar2\r\n$4\r\n1234\r\n",
			"*2\r\n$3\r\ndel\r\n$4\r\nvar1\r\n",
			"*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\n10\r\n",
		}, "")
		if err := os.RemoveAll("data.db"); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile("data.db", []byte(resp), 0666); err != nil {
			t.Fatal(err)
		}
		db := testOpen(t)
		defer testClose(db)
	}()
	testBadFormat := func(resp string) {
		if err := os.RemoveAll("data.db"); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile("data.db", []byte(resp), 0666); err != nil {
			t.Fatal(err)
		}
		db, err := Open("data.db")
		if err == nil {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll("data.db"); err != nil {
				t.Fatal(err)
			}
			t.Fatalf("invalid database should not be allowed")
		}
	}
	testBadFormat("*3\r")
	testBadFormat("*3\n")
	testBadFormat("*a\r\n")
	testBadFormat("*2\r\n")
	testBadFormat("*2\r\n%3")
	testBadFormat("*2\r\n$")
	testBadFormat("*2\r\n$3\r\n")
	testBadFormat("*2\r\n$3\r\ndel")
	testBadFormat("*2\r\n$3\r\ndel\r\r")
	testBadFormat("*0\r\n*2\r\n$3\r\ndel\r\r")
	testBadFormat("*1\r\n$3\r\nnop\r\n")
	testBadFormat("*1\r\n$3\r\ndel\r\n")
	testBadFormat("*1\r\n$3\r\nset\r\n")
	testBadFormat("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nxx\r\n$2\r\n10\r\n")
	testBadFormat("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
	testBadFormat("*15\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
	testBadFormat("*1A\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
	testBadFormat("*5\r\n$13\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
	testBadFormat("*5\r\n$1A\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
	testBadFormat("*5\r\n$3\r\nset\r\n$5000\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
}

func TestInsertsAndDeleted(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.CreateIndex("any", "*", IndexString); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSpatialIndex("rect", "*", IndexRect); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("item1", "value1", &SetOptions{Expires: true, TTL: time.Second}); err != nil {
			return err
		}
		if _, _, err := tx.Set("item2", "value2", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("item3", "value3", &SetOptions{Expires: true, TTL: time.Second}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test replacing items in the database
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("item1", "nvalue1", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("item2", "nvalue2", nil); err != nil {
			return err
		}
		if _, err := tx.Delete("item3"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// test index compare functions
func TestIndexCompare(t *testing.T) {
	if !IndexFloat("1.5", "1.6") {
		t.Fatalf("expected true, got false")
	}
	if !IndexInt("-1", "2") {
		t.Fatalf("expected true, got false")
	}
	if !IndexUint("10", "25") {
		t.Fatalf("expected true, got false")
	}
	if !IndexBinary("Hello", "hello") {
		t.Fatalf("expected true, got false")
	}
	if IndexString("hello", "hello") {
		t.Fatalf("expected false, got true")
	}
	if IndexString("Hello", "hello") {
		t.Fatalf("expected false, got true")
	}
	if IndexString("hello", "Hello") {
		t.Fatalf("expected false, got true")
	}
	if !IndexString("gello", "Hello") {
		t.Fatalf("expected true, got false")
	}
	if IndexString("Hello", "gello") {
		t.Fatalf("expected false, got true")
	}
	if Rect(IndexRect("[1 2 3 4],[5 6 7 8]")) != "[1 2 3 4],[5 6 7 8]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3 4],[5 6 7 8]", Rect(IndexRect("[1 2 3 4],[5 6 7 8]")))
	}
	if Rect(IndexRect("[1 2 3 4]")) != "[1 2 3 4]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3 4]", Rect(IndexRect("[1 2 3 4]")))
	}
	if Rect(nil, nil) != "[]" {
		t.Fatalf("expected '%v', got '%v'", "", Rect(nil, nil))
	}
	if Point(1, 2, 3) != "[1 2 3]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3]", Point(1, 2, 3))
	}
}

// test opening a folder.
func TestOpeningAFolder(t *testing.T) {
	if err := os.RemoveAll("dir.tmp"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("dir.tmp", 0700); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll("dir.tmp") }()
	db, err := Open("dir.tmp")
	if err == nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("opening a directory should not be allowed")
	}
}

// test opening an invalid resp file.
func TestOpeningInvalidDatabaseFile(t *testing.T) {
	if err := os.RemoveAll("data.db"); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile("data.db", []byte("invalid\r\nfile"), 0666); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll("data.db") }()
	db, err := Open("data.db")
	if err == nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("invalid database should not be allowed")
	}
}

// test closing a closed database.
func TestOpeningClosedDatabase(t *testing.T) {
	if err := os.RemoveAll("data.db"); err != nil {
		t.Fatal(err)
	}
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll("data.db") }()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatal("should not be able to close a closed database")
	}
	db, err = Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatal("should not be able to close a closed database")
	}
}

// test shrinking a database.
func TestShrink(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat("data.db")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("expected %v, got %v", 0, fi.Size())
	}
	// add 10 items
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 10; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// add the same 10 items
	// this will create 10 duplicate log entries
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 10; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat("data.db")
	if err != nil {
		t.Fatal(err)
	}
	sz1 := fi.Size()
	if sz1 == 0 {
		t.Fatalf("expected > 0, got %v", sz1)
	}
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat("data.db")
	if err != nil {
		t.Fatal(err)
	}
	sz2 := fi.Size()
	if sz2 >= sz1 {
		t.Fatalf("expected < %v, got %v", sz1, sz2)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Shrink(); err != ErrDatabaseClosed {
		t.Fatal("shrink on a closed databse should not be allowed")
	}
	// Now we will open a db that does not persist
	db, err = Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// add 10 items
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 10; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// add the same 10 items
	// this will create 10 duplicate log entries
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 10; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx) error {
		n, err := tx.Len()
		if err != nil {
			t.Fatal(err)
		}
		if n != 10 {
			t.Fatalf("expecting %v, got %v", 10, n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// this should succeed even though it's basically a noop.
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
}

func TestVariousIndexOperations(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	// test creating an index with no index name.
	err := db.CreateIndex("", "", nil)
	if err == nil {
		t.Fatal("should not be able to create an index with no name")
	}
	// test creating an index with a name that has already been used.
	err = db.CreateIndex("hello", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateIndex("hello", "", nil)
	if err == nil {
		t.Fatal("should not be able to create a duplicate index")
	}
	err = db.Update(func(tx *Tx) error {

		if _, _, err := tx.Set("user:1", "tom", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("user:2", "janet", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("alt:1", "from", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("alt:2", "there", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("rect:1", "[1 2],[3 4]", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("rect:2", "[5 6],[7 8]", nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// test creating an index after adding items. use pattern matching. have some items in the match and some not.
	if err := db.CreateIndex("string", "user:*", IndexString); err != nil {
		t.Fatal(err)
	}
	// test creating a spatial index after adding items. use pattern matching. have some items in the match and some not.
	if err := db.CreateSpatialIndex("rect", "rect:*", IndexRect); err != nil {
		t.Fatal(err)
	}
	// test dropping an index
	if err := db.DropIndex("hello"); err != nil {
		t.Fatal(err)
	}
	// test dropping an index with no name
	if err := db.DropIndex(""); err == nil {
		t.Fatal("should not be allowed to drop an index with no name")
	}
	// test dropping an index with no name
	if err := db.DropIndex("na"); err == nil {
		t.Fatal("should not be allowed to drop an index that does not exist")
	}
	// test retrieving index names
	names, err := db.Indexes()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "rect,string" {
		t.Fatalf("expecting '%v', got '%v'", "rect,string", strings.Join(names, ","))
	}
	// test creating an index after closing database
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("new-index", "", nil); err != ErrDatabaseClosed {
		t.Fatal("should not be able to create an index on a closed database")
	}
	// test getting index names after closing database
	if _, err := db.Indexes(); err != ErrDatabaseClosed {
		t.Fatal("should not be able to get index names on a closed database")
	}
	// test dropping an index after closing database
	if err := db.DropIndex("rect"); err != ErrDatabaseClosed {
		t.Fatal("should not be able to drop an index on a closed database")
	}
}

func test(t *testing.T, a, b bool) {
	if a != b {
		t.Fatal("failed, bummer...")
	}
}

func TestBasic(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db := testOpen(t)
	defer testClose(db)

	// create a simple index
	if err := db.CreateIndex("users", "fun:user:*", IndexString); err != nil {
		t.Fatal(err)
	}

	// create a spatial index
	if err := db.CreateSpatialIndex("rects", "rect:*", IndexRect); err != nil {
		t.Fatal(err)
	}
	if true {
		err := db.Update(func(tx *Tx) error {
			if _, _, err := tx.Set("fun:user:0", "tom", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:1", "Randi", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:2", "jane", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:4", "Janet", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:5", "Paula", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:6", "peter", nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("fun:user:7", "Terri", nil); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// add some random items
		start := time.Now()
		if err := db.Update(func(tx *Tx) error {
			for _, i := range rand.Perm(100) {
				if _, _, err := tx.Set(fmt.Sprintf("tag:%d", i+100), fmt.Sprintf("val:%d", rand.Int()%100+100), nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if false {
			println(time.Now().Sub(start).String(), db.keys.Len())
		}
		// add some random rects
		if err := db.Update(func(tx *Tx) error {
			if _, _, err := tx.Set("rect:1", Rect([]float64{10, 10}, []float64{20, 20}), nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("rect:2", Rect([]float64{15, 15}, []float64{24, 24}), nil); err != nil {
				return err
			}
			if _, _, err := tx.Set("rect:3", Rect([]float64{17, 17}, []float64{27, 27}), nil); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// verify the data has been created
	buf := &bytes.Buffer{}
	err := db.View(func(tx *Tx) error {
		err := tx.Ascend("users", func(key, val string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, val)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendRange("", "tag:170", "tag:172", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "tag:195", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "rect:", func(key, val string) bool {
			if !strings.HasPrefix(key, "rect:") {
				return false
			}
			min, max := IndexRect(val)
			fmt.Fprintf(buf, "%s: %v,%v\n", key, min, max)
			return true
		})
		expect := make([]string, 2)
		n := 0
		err = tx.Intersects("rects", "[0 0],[15 15]", func(key, val string) bool {
			if n == 2 {
				t.Fatalf("too many rects where received, expecting only two")
			}
			min, max := IndexRect(val)
			s := fmt.Sprintf("%s: %v,%v\n", key, min, max)
			if key == "rect:1" {
				expect[0] = s
			} else if key == "rect:2" {
				expect[1] = s
			}
			n++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range expect {
			if _, err := buf.WriteString(s); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	res := `
fun:user:2 jane
fun:user:4 Janet
fun:user:5 Paula
fun:user:6 peter
fun:user:1 Randi
fun:user:7 Terri
fun:user:0 tom
tag:170
tag:171
tag:195
tag:196
tag:197
tag:198
tag:199
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
rect:3: [17 17],[27 27]
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
`
	res = strings.Replace(res, "\r", "", -1)
	if strings.TrimSpace(buf.String()) != strings.TrimSpace(res) {
		t.Fatalf("expected [%v], got [%v]", strings.TrimSpace(res), strings.TrimSpace(buf.String()))
	}
}

func TestIndexAscend(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db := testOpen(t)
	defer testClose(db)

	// create a simple index
	if err := db.CreateIndex("usr", "usr:*", IndexInt); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 10; i > 0; i-- {
			tx.Set(fmt.Sprintf("usr:%d", i), fmt.Sprintf("%d", 10-i), nil)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	err := db.View(func(tx *Tx) error {
		tx.Ascend("usr", func(key, value string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, value)
			return true
		})
		fmt.Fprintln(buf)

		tx.AscendGreaterOrEqual("usr", "8", func(key, value string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, value)
			return true
		})
		fmt.Fprintln(buf)

		tx.AscendLessThan("usr", "3", func(key, value string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, value)
			return true
		})
		fmt.Fprintln(buf)

		tx.AscendRange("usr", "4", "8", func(key, value string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, value)
			return true
		})
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	res := `
usr:10 0
usr:9 1
usr:8 2
usr:7 3
usr:6 4
usr:5 5
usr:4 6
usr:3 7
usr:2 8
usr:1 9

usr:2 8
usr:1 9

usr:10 0
usr:9 1
usr:8 2

usr:6 4
usr:5 5
usr:4 6
usr:3 7
`
	res = strings.Replace(res, "\r", "", -1)
	s1 := strings.TrimSpace(buf.String())
	s2 := strings.TrimSpace(res)
	if s1 != s2 {
		t.Fatalf("expected [%v], got [%v]", s1, s2)
	}
}

func testRectStringer(min, max []float64) error {
	nmin, nmax := IndexRect(Rect(min, max))
	if len(nmin) != len(min) {
		return fmt.Errorf("rect=%v,%v, expect=%v,%v", nmin, nmax, min, max)
	}
	for i := 0; i < len(min); i++ {
		if min[i] != nmin[i] || max[i] != nmax[i] {
			return fmt.Errorf("rect=%v,%v, expect=%v,%v", nmin, nmax, min, max)
		}
	}
	return nil
}
func TestRectStrings(t *testing.T) {
	test(t, Rect(IndexRect(Point(1))) == "[1]", true)
	test(t, Rect(IndexRect(Point(1, 2, 3, 4))) == "[1 2 3 4]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[1 2]")))) == "[1 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[2 2]")))) == "[1 2],[2 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[2 2],[3]")))) == "[1 2],[2 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2]")))) == "[1 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1.5 2 4.5 5.6]")))) == "[1.5 2 4.5 5.6]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1.5 2 4.5 5.6 -1],[]")))) == "[1.5 2 4.5 5.6 -1]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[]")))) == "[]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("")))) == "[]", true)
	if err := testRectStringer(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{}, []float64{}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1}, []float64{2}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2}, []float64{3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3}, []float64{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3, 4}, []float64{5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3, 4, 5}, []float64{6, 7, 8, 9, 10}); err != nil {
		t.Fatal(err)
	}
}

// TestTTLReOpen test setting a TTL and then immediately closing the database and
// then waiting the TTL before reopening. The key should not be accessible.
func TestTTLReOpen(t *testing.T) {
	ttl := time.Second * 3
	db := testOpen(t)
	defer testClose(db)
	err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("key1", "val1", &SetOptions{Expires: true, TTL: ttl}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db = testReOpenDelay(t, db, ttl/4)
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get("key1")
		if err != nil {
			return err
		}
		if val != "val1" {
			t.Fatalf("expecting '%v', got '%v'", "val1", val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db = testReOpenDelay(t, db, ttl-ttl/4)
	defer testClose(db)
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get("key1")
		if err == nil || err != ErrNotFound || val != "" {
			t.Fatal("expecting not found")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTTL(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("key1", "val1", &SetOptions{Expires: true, TTL: time.Second}); err != nil {
			return err
		}
		if _, _, err := tx.Set("key2", "val2", nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx) error {
		dur1, err := tx.TTL("key1")
		if err != nil {
			t.Fatal(err)
		}
		if dur1 > time.Second || dur1 <= 0 {
			t.Fatalf("expecting between zero and one second, got '%v'", dur1)
		}
		dur1, err = tx.TTL("key2")
		if err != nil {
			t.Fatal(err)
		}
		if dur1 >= 0 {
			t.Fatalf("expecting a negative value, got '%v'", dur1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConfig(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)

	err := db.SetConfig(Config{SyncPolicy: SyncPolicy(-1)})
	if err == nil {
		t.Fatal("expecting a config syncpolicy error")
	}
	err = db.SetConfig(Config{SyncPolicy: SyncPolicy(3)})
	if err == nil {
		t.Fatal("expecting a config syncpolicy error")
	}
	err = db.SetConfig(Config{SyncPolicy: Never})
	if err != nil {
		t.Fatal(err)
	}
	err = db.SetConfig(Config{SyncPolicy: EverySecond})
	if err != nil {
		t.Fatal(err)
	}
	err = db.SetConfig(Config{AutoShrinkMinSize: 100, AutoShrinkPercentage: 200, SyncPolicy: Always})
	if err != nil {
		t.Fatal(err)
	}

	var c Config
	if err := db.ReadConfig(&c); err != nil {
		t.Fatal(err)
	}
	if c.AutoShrinkMinSize != 100 || c.AutoShrinkPercentage != 200 && c.SyncPolicy != Always {
		t.Fatalf("expecting %v, %v, and %v, got %v, %v, and %v", 100, 200, Always, c.AutoShrinkMinSize, c.AutoShrinkPercentage, c.SyncPolicy)
	}
}
func testUint64Hex(n uint64) string {
	s := strconv.FormatUint(n, 16)
	s = "0000000000000000" + s
	return s[len(s)-16:]
}
func textHexUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 16, 64)
	return n
}
func benchClose(t *testing.B, persist bool, db *DB) {
	if persist {
		if err := os.RemoveAll("data.db"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func benchOpenFillData(t *testing.B, N int,
	set, persist, random bool,
	geo bool,
	batch int) (db *DB, keys, vals []string) {
	///
	t.StopTimer()
	rand.Seed(time.Now().UnixNano())
	var err error
	if persist {
		if err := os.RemoveAll("data.db"); err != nil {
			t.Fatal(err)
		}
		db, err = Open("data.db")
	} else {
		db, err = Open(":memory:")
	}
	if err != nil {
		t.Fatal(err)
	}
	keys = make([]string, N)
	vals = make([]string, N)
	perm := rand.Perm(N)
	for i := 0; i < N; i++ {
		if random && set {
			keys[perm[i]] = testUint64Hex(uint64(i))
			vals[perm[i]] = strconv.FormatInt(rand.Int63()%1000+1000, 10)
		} else {
			keys[i] = testUint64Hex(uint64(i))
			vals[i] = strconv.FormatInt(rand.Int63()%1000+1000, 10)
		}
	}
	if set {
		t.StartTimer()
	}
	for i := 0; i < N; {
		err := db.Update(func(tx *Tx) error {
			var err error
			for j := 0; j < batch && i < N; j++ {
				_, _, err = tx.Set(keys[i], vals[i], nil)
				i++
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if set {
		t.StopTimer()
	}
	var n uint64
	err = db.View(func(tx *Tx) error {
		err := tx.Ascend("", func(key, value string) bool {
			n2 := textHexUint64(key)
			if n2 != n {
				t.Fatalf("expecting '%v', got '%v'", n2, n)
			}
			n++
			return true
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(N) {
		t.Fatalf("expecting '%v', got '%v'", N, n)
	}
	t.StartTimer()
	return db, keys, vals
}

func benchSetGet(t *testing.B, set, persist, random bool, batch int) {
	N := t.N
	for N > 0 {
		n := 0
		if N >= 100000 {
			n = 100000
		} else {
			n = N
		}
		func() {
			db, keys, _ := benchOpenFillData(t, n, set, persist, random, false, batch)
			defer benchClose(t, persist, db)
			if !set {
				for i := 0; i < n; {
					err := db.View(func(tx *Tx) error {
						var err error
						for j := 0; j < batch && i < n; j++ {
							_, err = tx.Get(keys[i])
							i++
						}
						return err
					})
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}()
		N -= n
	}
}

// Set Persist
func Benchmark_Set_Persist_Random_1(t *testing.B) {
	benchSetGet(t, true, true, true, 1)
}
func Benchmark_Set_Persist_Random_10(t *testing.B) {
	benchSetGet(t, true, true, true, 10)
}
func Benchmark_Set_Persist_Random_100(t *testing.B) {
	benchSetGet(t, true, true, true, 100)
}
func Benchmark_Set_Persist_Sequential_1(t *testing.B) {
	benchSetGet(t, true, true, false, 1)
}
func Benchmark_Set_Persist_Sequential_10(t *testing.B) {
	benchSetGet(t, true, true, false, 10)
}
func Benchmark_Set_Persist_Sequential_100(t *testing.B) {
	benchSetGet(t, true, true, false, 100)
}

// Set NoPersist
func Benchmark_Set_NoPersist_Random_1(t *testing.B) {
	benchSetGet(t, true, false, true, 1)
}
func Benchmark_Set_NoPersist_Random_10(t *testing.B) {
	benchSetGet(t, true, false, true, 10)
}
func Benchmark_Set_NoPersist_Random_100(t *testing.B) {
	benchSetGet(t, true, false, true, 100)
}
func Benchmark_Set_NoPersist_Sequential_1(t *testing.B) {
	benchSetGet(t, true, false, false, 1)
}
func Benchmark_Set_NoPersist_Sequential_10(t *testing.B) {
	benchSetGet(t, true, false, false, 10)
}
func Benchmark_Set_NoPersist_Sequential_100(t *testing.B) {
	benchSetGet(t, true, false, false, 100)
}

// Get
func Benchmark_Get_1(t *testing.B) {
	benchSetGet(t, false, false, false, 1)
}
func Benchmark_Get_10(t *testing.B) {
	benchSetGet(t, false, false, false, 10)
}
func Benchmark_Get_100(t *testing.B) {
	benchSetGet(t, false, false, false, 100)
}

func benchScan(t *testing.B, asc bool, count int) {
	N := count
	db, _, _ := benchOpenFillData(t, N, false, false, false, false, 100)
	defer benchClose(t, false, db)
	for i := 0; i < t.N; i++ {
		count := 0
		err := db.View(func(tx *Tx) error {
			if asc {
				return tx.Ascend("", func(key, val string) bool {
					count++
					return true
				})
			}
			return tx.Descend("", func(key, val string) bool {
				count++
				return true
			})

		})
		if err != nil {
			t.Fatal(err)
		}
		if count != N {
			t.Fatalf("expecting '%v', got '%v'", N, count)
		}
	}
}

func Benchmark_Ascend_1(t *testing.B) {
	benchScan(t, true, 1)
}
func Benchmark_Ascend_10(t *testing.B) {
	benchScan(t, true, 10)
}
func Benchmark_Ascend_100(t *testing.B) {
	benchScan(t, true, 100)
}
func Benchmark_Ascend_1000(t *testing.B) {
	benchScan(t, true, 1000)
}
func Benchmark_Ascend_10000(t *testing.B) {
	benchScan(t, true, 10000)
}

func Benchmark_Descend_1(t *testing.B) {
	benchScan(t, false, 1)
}
func Benchmark_Descend_10(t *testing.B) {
	benchScan(t, false, 10)
}
func Benchmark_Descend_100(t *testing.B) {
	benchScan(t, false, 100)
}
func Benchmark_Descend_1000(t *testing.B) {
	benchScan(t, false, 1000)
}
func Benchmark_Descend_10000(t *testing.B) {
	benchScan(t, false, 10000)
}

/*
func Benchmark_Spatial_2D(t *testing.B) {
	N := 100000
	db, _, _ := benchOpenFillData(t, N, true, true, false, true, 100)
	defer benchClose(t, false, db)

}
*/
func TestCoverCloseAlreadyClosed(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	_ = db.file.Close()
	if err := db.Close(); err == nil {
		t.Fatal("expecting an error")
	}
}

func TestCoverConfigClosed(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	_ = db.Close()
	var config Config
	if err := db.ReadConfig(&config); err != ErrDatabaseClosed {
		t.Fatal("expecting database closed error")
	}
	if err := db.SetConfig(config); err != ErrDatabaseClosed {
		t.Fatal("expecting database closed error")
	}
}
func TestCoverShrinkShrink(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 10000; i++ {
			_, _, err := tx.Set(fmt.Sprintf("%d", i), fmt.Sprintf("%d", i), nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 250; i < 250+100; i++ {
			_, err := tx.Delete(fmt.Sprintf("%d", i))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = db.Shrink()
	}()
	go func() {
		defer wg.Done()
		err2 = db.Shrink()
	}()
	wg.Wait()
	//println(123)
	//fmt.Printf("%v\n%v\n", err1, err2)
	if err1 != ErrShrinkInProcess && err2 != ErrShrinkInProcess {
		t.Fatal("expecting a shrink in process error")
	}
	db = testReOpen(t, db)
	defer testClose(db)
	if err := db.View(func(tx *Tx) error {
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != 9900 {
			t.Fatal("expecting 9900 items")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPreviousItem(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)
	err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("hello", "world", nil)
		if err != nil {
			return err
		}
		prev, replaced, err := tx.Set("hello", "planet", nil)
		if err != nil {
			return err
		}
		if !replaced {
			t.Fatal("should be replaced")
		}
		if prev != "world" {
			t.Fatalf("expecting '%v', got '%v'", "world", prev)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONIndex(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)

	_ = db.CreateIndex("last_name", "*", IndexJSON("name.last"))
	_ = db.CreateIndex("last_name_cs", "*", IndexJSONCaseSensitive("name.last"))
	_ = db.CreateIndex("age", "*", IndexJSON("age"))
	_ = db.CreateIndex("student", "*", IndexJSON("student"))
	_ = db.Update(func(tx *Tx) error {
		_, _, _ = tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38,"student":false}`, nil)
		_, _, _ = tx.Set("2", `{"name":{"first":"Janet","last":"Prichard"},"age":47,"student":true}`, nil)
		_, _, _ = tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52,"student":true}`, nil)
		_, _, _ = tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28,"student":false}`, nil)
		_, _, _ = tx.Set("5", `{"name":{"first":"bill","last":"frank"},"age":21,"student":true}`, nil)
		_, _, _ = tx.Set("6", `{"name":{"first":"sally","last":"randall"},"age":68,"student":false}`, nil)
		return nil
	})
	var keys []string
	_ = db.View(func(tx *Tx) error {
		_ = tx.Ascend("last_name_cs", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			keys = append(keys, key)
			return true
		})
		_ = tx.Ascend("last_name", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			keys = append(keys, key)
			return true
		})
		_ = tx.Ascend("age", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			keys = append(keys, key)
			return true
		})
		_ = tx.Ascend("student", func(key, value string) bool {
			//fmt.Printf("%s: %s\n", key, value)
			keys = append(keys, key)
			return true
		})
		return nil
	})
	expect := "3,4,1,2,5,6,3,4,5,1,2,6,5,4,1,2,3,6,1,4,6,2,3,5"
	if strings.Join(keys, ",") != expect {
		t.Fatalf("expected %v, got %v", expect, strings.Join(keys, ","))
	}
}
