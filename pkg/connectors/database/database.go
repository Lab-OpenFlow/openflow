package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// DatabaseConnector executes SQL queries and mutations against relational databases with connection pooling.
type DatabaseConnector struct {
	poolMap sync.Map // map[string]*sql.DB (dsn -> *sql.DB)
}

// NewDatabaseConnector creates a new Database connector.
func NewDatabaseConnector() *DatabaseConnector {
	return &DatabaseConnector{}
}

func (c *DatabaseConnector) Type() model.ConnectorType {
	return model.ConnectorTypeDatabase
}

func (c *DatabaseConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeDatabase,
		Name:        "SQL Database Connector",
		Description: "Execute queries, inserts, updates, and transactions on SQL databases (PostgreSQL, MySQL, SQLite) with pooled connections",
		Category:    "storage",
		Icon:        "Database",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"required": []string{"driver", "dsn", "query"},
			"properties": map[string]interface{}{
				"driver": map[string]interface{}{"type": "string", "enum": []string{"postgres", "mysql", "sqlite3"}},
				"dsn":    map[string]interface{}{"type": "string", "description": "Database connection DSN/URI"},
				"query":  map[string]interface{}{"type": "string", "description": "SQL query with parameter placeholders"},
				"args":   map[string]interface{}{"type": "array", "description": "Positional arguments for query"},
			},
		},
	}
}

func (c *DatabaseConnector) Validate(config map[string]interface{}) error {
	query, ok := config["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return fmt.Errorf("database connector: 'query' is required")
	}
	return nil
}

func (c *DatabaseConnector) getDB(driver, dsn string) (*sql.DB, error) {
	key := fmt.Sprintf("%s::%s", driver, dsn)
	if val, ok := c.poolMap.Load(key); ok {
		return val.(*sql.DB), nil
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)

	c.poolMap.Store(key, db)
	return db, nil
}

func (c *DatabaseConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	driver, _ := config["driver"].(string)
	if driver == "" {
		driver = "postgres"
	}
	dsn, _ := config["dsn"].(string)
	query, _ := config["query"].(string)

	var args []interface{}
	if rawArgs, ok := config["args"].([]interface{}); ok {
		args = rawArgs
	}

	// For mock/standalone execution when DSN is empty or mock driver
	if dsn == "" || strings.HasPrefix(dsn, "mock://") {
		return map[string]interface{}{
			"driver":        driver,
			"query":         query,
			"rows_affected": 1,
			"simulated":     true,
			"timestamp":     time.Now().Format(time.RFC3339),
		}, nil
	}

	db, err := c.getDB(driver, dsn)
	if err != nil {
		return nil, err
	}

	isSelect := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT")

	if isSelect {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute sql query: %w", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get sql columns: %w", err)
		}

		var results []map[string]interface{}
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}

			rowMap := make(map[string]interface{})
			for i, colName := range cols {
				val := columnPointers[i].(*interface{})
				rowMap[colName] = *val
			}
			results = append(results, rowMap)
		}

		return map[string]interface{}{
			"driver":       driver,
			"query":        query,
			"rows":         results,
			"rows_count":   len(results),
			"timestamp":    time.Now().Format(time.RFC3339),
		}, nil
	}

	// For INSERT / UPDATE / DELETE
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql statement: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	return map[string]interface{}{
		"driver":        driver,
		"query":         query,
		"rows_affected": rowsAffected,
		"last_insert_id": lastInsertID,
		"timestamp":     time.Now().Format(time.RFC3339),
	}, nil
}
