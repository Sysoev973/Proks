// Command gendata grows the demo catalogue in csv_files to -books books, so
// load tests spread over many keys instead of four. The first -keep rows of
// books.csv and book_authors.csv are kept as they are (other tables refer to
// them); the rest is generated deterministically from -seed.
//
//	go run ./cmd/gendata -books 1000
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
)

var (
	adjectives = []string{"Тихий", "Последний", "Северный", "Забытый", "Новый", "Старый", "Дальний", "Красный", "Ночной", "Великий"}
	nouns      = []string{"город", "мир", "дом", "берег", "сад", "путь", "остров", "лес", "век", "архив"}
	suffixes   = []string{"", "", ": история", ": война и мир", " и основание", ": записки Шерлока", " на Go", ""}
)

func main() {
	dir := flag.String("dir", "csv_files", "directory with books.csv and book_authors.csv")
	books := flag.Int("books", 1000, "total number of books")
	keep := flag.Int("keep", 5, "hand-written rows to keep")
	genres := flag.Int("genres", 5, "number of genres in genres.csv")
	authors := flag.Int("authors", 5, "number of authors in authors.csv")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	if err := run(*dir, *books, *keep, *genres, *authors, *seed); err != nil {
		log.Fatal(err)
	}
}

func run(dir string, books, keep, genres, authors int, seed int64) error {
	if books < keep {
		return fmt.Errorf("-books (%d) must be at least -keep (%d)", books, keep)
	}
	rnd := rand.New(rand.NewSource(seed))

	bookRows, err := keptRows(filepath.Join(dir, "books.csv"), keep)
	if err != nil {
		return err
	}
	authorRows, err := keptRows(filepath.Join(dir, "book_authors.csv"), keep)
	if err != nil {
		return err
	}
	for id := keep + 1; id <= books; id++ {
		title := adjectives[rnd.Intn(len(adjectives))] + " " + nouns[rnd.Intn(len(nouns))] + suffixes[rnd.Intn(len(suffixes))]
		if rnd.Intn(3) == 0 {
			title += ", том " + strconv.Itoa(1+rnd.Intn(5))
		}
		isbn := fmt.Sprintf("978-5-%04d-%04d-%d", rnd.Intn(10000), rnd.Intn(10000), rnd.Intn(10))
		bookRows = append(bookRows, []string{
			strconv.Itoa(id),
			strconv.Itoa(1 + rnd.Intn(genres)),
			title,
			isbn,
			strconv.Itoa(1800 + rnd.Intn(225)),
			strconv.FormatBool(rnd.Intn(20) == 0),
			strconv.FormatBool(rnd.Intn(2) == 0),
		})
		first := 1 + rnd.Intn(authors)
		authorRows = append(authorRows, []string{strconv.Itoa(id), strconv.Itoa(first)})
		if second := 1 + rnd.Intn(authors); rnd.Intn(5) == 0 && second != first {
			authorRows = append(authorRows, []string{strconv.Itoa(id), strconv.Itoa(second)})
		}
	}
	if err := writeCSV(filepath.Join(dir, "books.csv"), bookRows); err != nil {
		return err
	}
	return writeCSV(filepath.Join(dir, "book_authors.csv"), authorRows)
}

// keptRows returns the header and the first keep data rows of file.
func keptRows(file string, keep int) ([][]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if len(rows) < keep+1 {
		return nil, fmt.Errorf("%s has %d data rows, fewer than -keep %d", file, len(rows)-1, keep)
	}
	return rows[:keep+1], nil
}

func writeCSV(file string, rows [][]string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
