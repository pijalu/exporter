package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	_ "github.com/go-sql-driver/mysql"
)

type Queries struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"`
}

type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Name     string `yaml:"name"`
	} `yaml:"database"`

	Queries []Queries `yaml:"queries"`
}

func (c *Config) getQuery(id string) (*Queries, error) {
	for _, q := range c.Queries {
		if strings.ToLower(q.Name) == strings.ToLower(id) {
			return &q, nil
		}
	}
	return nil, fmt.Errorf("Could not find query %s", id)
}

func main() {
	config := getConfig(os.Args)

	log.Printf("Loaded config: %v", config)

	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Query: %v", r.URL)
		query, err := config.getQuery(r.URL.Query().Get("id"))
		if err != nil {
			log.Printf("Error running query: %v", err)
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("404 - Not Found"))
			return
		}
		if runQuery(config, w, query.Query) != nil {
			log.Printf("Error running query: %v", err)
			w.Write([]byte(err.Error()))
			return
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("200 - Healthy!"))
	})

	log.Printf("Starting web service on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Printf("Error starting web service: %v", err)
	}
}

func getConfig(args []string) *Config {
	var configFile string

	if len(args) == 2 {
		configFile = args[1]
	} else {
		configFile = os.Getenv("CONFIG_FILE")
	}

	if configFile == "" {
		panic("Configuration file not specified.")
	}

	file, err := os.Open(configFile)
	if err != nil {
		panic(fmt.Sprintf("Error opening configuration file: %v", err))
	}

	defer file.Close()

	var config Config
	if err := yaml.NewDecoder(file).Decode(&config); err != nil {
		panic(fmt.Sprintf("Error decoding configuration file: %v", err))
	}

	return &config
}

func runQuery(config *Config, w io.Writer, query string) error {
	// Connect to the database using the connection information from the config file.
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.Database.Username,
		config.Database.Password,
		config.Database.Host,
		config.Database.Port,
		config.Database.Name))
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	// Run the SQL query against the database and return the result set.
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Error running query: %v", err)
		return fmt.Errorf("error running query: %s", err)
	}
	defer rows.Close()

	// save columns
	c, err := rows.Columns()
	if err != nil {
		log.Printf("Error getting columns name: %v", err)
		return fmt.Errorf("error getting columns name: %v", err)
	}
	// Create a new CSV writer for the response stream.
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()
	csvWriter.Write(c)

	rowPtr := make([]any, len(c))
	rowString := make([]*string, len(c))
	for i := range rowString {
		rowPtr[i] = &rowString[i]
	}
	rowStringNull := make([]string, len(c))

	for rows.Next() {
		err := rows.Scan(rowPtr...)
		if err != nil {
			log.Printf("Error fetching results: %v", err)
			return fmt.Errorf("error getting fetching columns: %v", err)
		}
		for i, str := range rowString {
			if str == nil {
				rowStringNull[i] = "null"
			} else {
				rowStringNull[i] = *str
			}
		}

		csvWriter.Write(rowStringNull)
	}

	return nil
}
