package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"

	_ "github.com/godror/godror"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/jsoniter"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
)

func init() {
	bulker.RegisterBulker(OracleBulkerTypeId, NewOracle)
}

const (
	OracleBulkerTypeId = "oracle"

	// Include declared length, precision, and scale to reconstruct types like VARCHAR2(255), NUMBER(p,s), TIMESTAMP(n)
	oracleTableSchemaQuery        = `SELECT COLUMN_NAME, DATA_TYPE, CHAR_COL_DECL_LENGTH, DATA_PRECISION, DATA_SCALE FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 ORDER BY COLUMN_ID`
	oraclePrimaryKeyFieldsQuery   = `SELECT cols.COLUMN_NAME FROM ALL_CONS_COLUMNS cols JOIN ALL_CONSTRAINTS cons ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME WHERE cons.CONSTRAINT_TYPE = 'P' AND cons.OWNER = :1 AND cons.TABLE_NAME = :2 ORDER BY cols.POSITION`
	oracleDropPrimaryKeyTemplate  = `ALTER TABLE %s%s DROP PRIMARY KEY`
	oracleAlterPrimaryKeyTemplate = `ALTER TABLE %s%s ADD PRIMARY KEY (%s)`
	oracleCreateSchemaTemplate    = "CREATE USER %s IDENTIFIED BY %s"
	oracleIndexTemplate           = `CREATE INDEX %s ON %s%s (%s)`
	oracleRenameTableTemplate     = `ALTER TABLE %s%s%s RENAME TO %s`

	// oracleMergeQuery = `MERGE INTO {{.Namespace}}{{.TableName}} USING (SELECT {{.Placeholders}} FROM DUAL) src ON ({{.JoinConditions}}) WHEN MATCHED THEN UPDATE SET {{.UpdateSet}} WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.Placeholders}})`

	oracleBulkMergeQuery = `MERGE INTO {{.Namespace}}{{.TableTo}} T USING (SELECT {{.Columns}} FROM {{.NamespaceFrom}}{{.TableFrom}} ) S ON ({{.JoinConditions}}) WHEN MATCHED THEN UPDATE SET {{.UpdateSet}} WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.SourceColumns}})`
	// Insert-only variant used when there are no non-PK columns to update
	oracleBulkMergeInsertOnlyQuery = `MERGE INTO {{.Namespace}}{{.TableTo}} T USING (SELECT {{.Columns}} FROM {{.NamespaceFrom}}{{.TableFrom}} ) S ON ({{.JoinConditions}}) WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.SourceColumns}})`
)

var (
	oracleBulkMergeQueryTemplate, _      = template.New("oracleBulkMergeQuery").Parse(oracleBulkMergeQuery)
	oracleBulkMergeInsertOnlyTemplate, _ = template.New("oracleBulkMergeInsertOnlyQuery").Parse(oracleBulkMergeInsertOnlyQuery)

	oracleTypes = map[types2.DataType][]string{
		types2.STRING:    {"VARCHAR2(255)", "CLOB"},
		types2.INT64:     {"NUMBER"},
		types2.FLOAT64:   {"FLOAT"},
		types2.TIMESTAMP: {"TIMESTAMP"},
		types2.BOOL:      {"NUMBER(1)"},
		types2.JSON:      {"CLOB"}, // Oracle 21c+ supports JSON type
		types2.UNKNOWN:   {"CLOB"},
	}

	oraclePrimaryKeyTypesMapping = map[string]string{
		"CLOB": "VARCHAR2(255)",
	}
)

// Oracle is adapter for creating, patching (schema or table), inserting data to Oracle database
type Oracle struct {
	*SQLAdapterBase[DataSourceConfig]
}

func (o *Oracle) Type() string {
	return OracleBulkerTypeId
}

// NewOracle returns configured Oracle adapter instance
func NewOracle(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &DataSourceConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}

	if config.Parameters == nil {
		config.Parameters = map[string]string{}
	}
	utils.MapPutIfAbsent(config.Parameters, "charset", "AL32UTF8")

	dbConnectFunction := func(ctx context.Context, cfg *DataSourceConfig) (*sql.DB, error) {

		connectionString := oracleDriverConnectionString(config)
		dataSource, err := sql.Open("godror", connectionString)
		if err != nil {
			return nil, err
		}
		if err := dataSource.PingContext(ctx); err != nil {
			dataSource.Close()
			return nil, err
		}
		dataSource.SetConnMaxLifetime(3 * time.Minute)
		dataSource.SetMaxIdleConns(10)
		return dataSource, nil
	}
	typecastFunc := func(placeholder string, column types2.SQLColumn) string {
		return placeholder
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulker.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}
	// Oracle uses :1, :2, ... for placeholders
	oracleParameterPlaceholder := func(idx int, _ string) string {
		return fmt.Sprintf(":%d", idx)
	}
	sqlAdapterBase, err := newSQLAdapterBase(bulkerConfig.Id, OracleBulkerTypeId, config, config.Username, dbConnectFunction, oracleTypes, queryLogger, typecastFunc, oracleParameterPlaceholder, oracleColumnDDL, oracleMapColumnValue, checkErr, true)
	o := &Oracle{
		SQLAdapterBase: sqlAdapterBase,
	}
	o.batchFileFormat = types2.FileFormatNDJSON
	o.tableHelper = NewTableHelper(OracleBulkerTypeId, 30, '"')
	o.createPKFunc = o.createPrimaryKey
	o.dropPKFunc = o.deletePrimaryKey
	o.renameToSchemaless = true
	return o, err
}

func (o *Oracle) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))
	if err := o.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.Stream:
		return newAutoCommitStream(id, o, tableName, streamOptions...)
	case bulker.Batch:
		return newTransactionalStream(id, o, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, o, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, o, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (o *Oracle) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		options.Add(option)
	}
	return nil
}

func (o *Oracle) createSchemaIfNotExists(ctx context.Context, schema string, password string) error {
	if schema == "" {
		return nil
	}
	query := fmt.Sprintf(oracleCreateSchemaTemplate, schema, password)
	_, err := o.txOrDb(ctx).ExecContext(ctx, query)
	if err != nil && !strings.Contains(err.Error(), "ORA-01920") { // user already exists
		return errorj.CreateSchemaError.Wrap(err, "failed to create schema").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Database:  o.config.Schema,
				Statement: query,
			})
	}
	return nil
}

// InitDatabase creates schema if doesn't exist
func (o *Oracle) InitDatabase(ctx context.Context) error {
	// Oracle: schemas are users, so you may want to create user/schema if needed
	o.createSchemaIfNotExists(ctx, o.config.Schema, o.config.Password)
	return nil
}

// OpenTx opens underline sql transaction and return wrapped instance
func (o *Oracle) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return o.openTx(ctx, o)
}

func (o *Oracle) Insert(ctx context.Context, table *Table, merge bool, objects ...types2.Object) error {
	// Follow Snowflake's approach: do SELECT on PK and then UPDATE or INSERT to avoid MERGE updating PKs
	if !merge || len(table.GetPKFields()) == 0 {
		return o.insert(ctx, table, objects)
	}
	for _, object := range objects {
		pkMatchConditions := &WhenConditions{}
		for _, pkColumn := range table.GetPKFields() {
			value := object.GetN(pkColumn)
			if value == nil {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "IS NULL", nil)
			} else {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "=", value)
			}
		}
		res, err := o.SQLAdapterBase.Select(ctx, table.Namespace, table.Name, pkMatchConditions, nil)
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed check primary key collision").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:      o.config.Schema,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
				})
		}
		if len(res) > 0 {
			if err := o.Update(ctx, table, object, pkMatchConditions); err != nil {
				return err
			}
		} else {
			if err := o.insert(ctx, table, []types2.Object{object}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Oracle) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) (bulker.WarehouseState, error) {
	if mergeWindow <= 0 {
		return o.copy(ctx, targetTable, sourceTable)
	}
	// Build MERGE statement locally to exclude PKs from UPDATE SET
	quotedSchema := o.namespacePrefix(targetTable.Namespace)
	quotedSchemaFrom := o.namespacePrefix(sourceTable.Namespace)
	quotedTargetTableName := o.quotedTableName(targetTable.Name)
	quotedSourceTableName := o.quotedTableName(sourceTable.Name)

	count := sourceTable.ColumnsCount()
	columnNames := sourceTable.MappedColumnNames(o.quotedColumnName)
	updateColumns := make([]string, 0, count)
	insertColumns := make([]string, count)
	joinConditions := make([]string, 0, targetTable.PKFields.Size())
	sourceTable.Columns.ForEachIndexed(func(i int, name string, col types2.SQLColumn) {
		colName := columnNames[i]
		if !targetTable.PKFields.Contains(name) {
			updateColumns = append(updateColumns, fmt.Sprintf(`%s=%s.%s`, colName, "S", colName))
		}
		insertColumns[i] = o.typecastFunc(fmt.Sprintf(`%s.%s`, "S", colName), col)
	})
	targetTable.PKFields.ForEach(func(pkField string) {
		pkName := o.quotedColumnName(pkField)
		joinConditions = append(joinConditions, fmt.Sprintf("%s.%s = %s.%s", "T", pkName, "S", pkName))
	})
	payload := QueryPayload{
		Namespace:      quotedSchema,
		NamespaceFrom:  quotedSchemaFrom,
		TableTo:        quotedTargetTableName,
		TableFrom:      quotedSourceTableName,
		Columns:        strings.Join(columnNames, ","),
		PrimaryKeyName: targetTable.PrimaryKeyName,
		JoinConditions: strings.Join(joinConditions, " AND "),
		SourceColumns:  strings.Join(insertColumns, ", "),
		UpdateSet:      strings.Join(updateColumns, ","),
	}
	var buf strings.Builder
	tpl := oracleBulkMergeQueryTemplate
	if len(updateColumns) == 0 {
		// No non-PK columns to update; use insert-only merge
		tpl = oracleBulkMergeInsertOnlyTemplate
	}
	if err := tpl.Execute(&buf, payload); err != nil {
		return bulker.WarehouseState{}, errorj.ExecuteInsertError.Wrap(err, "failed to build query from template")
	}
	statement := buf.String()
	if _, err := o.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return bulker.WarehouseState{}, errorj.BulkMergeError.Wrap(err, "failed to bulk insert").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTargetTableName,
				PrimaryKeys: targetTable.GetPKFields(),
				Statement:   statement,
			})
	}
	return bulker.WarehouseState{Name: "merge", TimeProcessedMs: 0}, nil
}

func (o *Oracle) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (state bulker.WarehouseState, err error) {
	quotedTableName := o.quotedTableName(targetTable.Name)
	quotedNamespace := o.namespacePrefix(targetTable.Namespace)

	if loadSource.Type != LocalFile {
		return state, fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != o.batchFileFormat {
		return state, fmt.Errorf("LoadTable: only %s format is supported", o.batchFileFormat)
	}
	count := targetTable.ColumnsCount()
	columnNames := make([]string, count)
	placeholders := make([]string, count)
	targetTable.Columns.ForEachIndexed(func(i int, name string, col types2.SQLColumn) {
		columnNames[i] = o.quotedColumnName(name)
		placeholders[i] = o.typecastFunc(o.parameterPlaceholder(i+1, name), col)
	})
	insertPayload := QueryPayload{
		Namespace:      quotedNamespace,
		TableName:      quotedTableName,
		Columns:        strings.Join(columnNames, ", "),
		Placeholders:   strings.Join(placeholders, ", "),
		PrimaryKeyName: targetTable.PrimaryKeyName,
	}
	buf := strings.Builder{}
	err = insertQueryTemplate.Execute(&buf, insertPayload)
	if err != nil {
		return state, errorj.ExecuteInsertError.Wrap(err, "failed to build query from template")
	}
	statement := buf.String()
	defer func() {
		if err != nil {
			err = errorj.LoadError.Wrap(err, "failed to load table").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    o.config.Schema,
					Table:     quotedTableName,
					Statement: statement,
				})
		}
	}()

	stmt, err := o.txOrDb(ctx).PrepareContext(ctx, statement)
	if err != nil {
		return state, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	file, err := os.Open(loadSource.Path)
	if err != nil {
		return state, err
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := jsoniter.NewDecoder(file)
	decoder.UseNumber()
	args := make([]any, count)
	for {
		var object map[string]any
		err = decoder.Decode(&object)
		if err != nil {
			if err == io.EOF {
				break
			}
			return state, err
		}
		targetTable.Columns.ForEachIndexed(func(i int, name string, col types2.SQLColumn) {
			val, ok := object[name]
			if ok {
				val, _ = types2.ReformatValue(val)
			}
			args[i] = o.valueMappingFunction(val, ok, col)
		})
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return state, checkErr(err)
		}
	}
	return state, nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (o *Oracle) GetTableSchema(ctx context.Context, namespace string, tableName string) (*Table, error) {
	table, err := o.getTable(ctx, namespace, tableName)
	if err != nil {
		return nil, err
	}
	if table.ColumnsCount() == 0 {
		return table, nil
	}
	pkFields, err := o.getPrimaryKeys(ctx, namespace, tableName)
	if err != nil {
		return nil, err
	}
	table.PKFields = pkFields
	if pkFields.Size() > 0 {
		table.PrimaryKeyName = "PRIMARY"
	}
	return table, nil
}

func (o *Oracle) getTable(ctx context.Context, namespace, tableName string) (*Table, error) {
	tableName = o.TableName(tableName)
	namespace = o.NamespaceName(namespace)
	table := &Table{Name: tableName, Namespace: namespace, Columns: NewColumns(0), PKFields: types.NewOrderedSet[string]()}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	rows, err := o.dataSource.QueryContext(ctx, oracleTableSchemaQuery, namespace, tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Database:    namespace,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   oracleTableSchemaQuery,
				Values:      []any{namespace, tableName},
			})
	}
	defer rows.Close()
	for rows.Next() {
		var columnName, dataType string
		var charLen sql.NullInt64
		var dataPrecision sql.NullInt64
		var dataScale sql.NullInt64
		if err := rows.Scan(&columnName, &dataType, &charLen, &dataPrecision, &dataScale); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Database:    namespace,
					Table:       tableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   oracleTableSchemaQuery,
					Values:      []any{namespace, tableName},
				})
		}
		if dataType == "" {
			continue
		}
		// Reconstruct type with length/precision/scale when applicable
		upperType := strings.ToUpper(strings.TrimSpace(dataType))
		declaredType := upperType
		switch upperType {
		case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR":
			if charLen.Valid && charLen.Int64 > 0 {
				declaredType = fmt.Sprintf("%s(%d)", upperType, charLen.Int64)
			}
		case "NUMBER":
			if dataPrecision.Valid {
				if dataScale.Valid {
					if dataScale.Int64 > 0 {
						declaredType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision.Int64, dataScale.Int64)
					} else {
						declaredType = fmt.Sprintf("NUMBER(%d)", dataPrecision.Int64)
					}
				} else {
					declaredType = fmt.Sprintf("NUMBER(%d)", dataPrecision.Int64)
				}
			}
		case "TIMESTAMP":
			// Use scale as fractional seconds precision if available (common default is 6)
			if dataScale.Valid && dataScale.Int64 > 0 {
				declaredType = fmt.Sprintf("TIMESTAMP(%d)", dataScale.Int64)
			}
		}

		dt, _ := o.GetDataType(declaredType)
		table.Columns.Set(columnName, types2.SQLColumn{Type: declaredType, DataType: dt})
	}
	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Database:    namespace,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   oracleTableSchemaQuery,
				Values:      []any{namespace, tableName},
			})
	}
	return table, nil
}

func (o *Oracle) getPrimaryKeys(ctx context.Context, namespace, tableName string) (types.OrderedSet[string], error) {
	tableName = o.TableName(tableName)
	namespace = o.NamespaceName(namespace)
	pkFieldsRows, err := o.dataSource.QueryContext(ctx, oraclePrimaryKeyFieldsQuery, namespace, tableName)
	if err != nil {
		return types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Database:  namespace,
				Table:     tableName,
				Statement: oraclePrimaryKeyFieldsQuery,
				Values:    []any{namespace, tableName},
			})
	}
	defer pkFieldsRows.Close()
	pkFields := types.NewOrderedSet[string]()
	for pkFieldsRows.Next() {
		var fieldName string
		if err := pkFieldsRows.Scan(&fieldName); err != nil {
			return types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Database:  namespace,
					Table:     tableName,
					Statement: oraclePrimaryKeyFieldsQuery,
					Values:    []any{namespace, tableName},
				})
		}
		pkFields.Put(fieldName)
	}
	if err := pkFieldsRows.Err(); err != nil {
		return types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Database:  namespace,
				Table:     tableName,
				Statement: oraclePrimaryKeyFieldsQuery,
				Values:    []any{namespace, tableName},
			})
	}
	return pkFields, nil
}

func (o *Oracle) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
	tmpTable := "dp_" + time.Now().Format("_20060102_150405") + targetTableName
	err1 := o.renameTable(ctx, true, replacementTable.Namespace, targetTableName, tmpTable)
	err = o.renameTable(ctx, false, replacementTable.Namespace, replacementTable.Name, targetTableName)
	if dropOldTable && err1 == nil && err == nil {
		row := o.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'`, replacementTable.Namespace, tmpTable))
		var exists int
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if exists == 0 {
			return nil
		}
		return o.DropTable(ctx, replacementTable.Namespace, tmpTable, false)
	}
	return
}

func (o *Oracle) renameTable(ctx context.Context, ifExists bool, namespace, tableName, newTableName string) error {
	if ifExists {
		db := o.NamespaceName(namespace)
		tableName = o.TableName(tableName)
		row := o.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'`, db, tableName))
		var exists int
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if exists == 0 {
			return nil
		}
	}
	return o.SQLAdapterBase.renameTable(ctx, false, namespace, tableName, newTableName)

}

func oracleDriverConnectionString(config *DataSourceConfig) string {
	// user/password@host:port/service_name
	return fmt.Sprintf("%s/%s@%s:%d/%s",
		config.Username, config.Password, config.Host, config.Port, config.Db)

}

// oracleColumnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func oracleColumnDDL(quotedName, name string, table *Table, column types2.SQLColumn) string {
	sqlType := column.GetDDLType()
	if table.PKFields.Contains(name) {
		if typeForPKField, ok := oraclePrimaryKeyTypesMapping[sqlType]; ok {
			sqlType = typeForPKField
		}
	}
	return fmt.Sprintf("%s %s", quotedName, sqlType)
}

func oracleMapColumnValue(value any, valuePresent bool, column types2.SQLColumn) any {
	if !valuePresent {
		return value
	}
	if datetime, ok := value.(time.Time); ok {
		if datetime.IsZero() {
			return time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
		}
	}
	return value
}

func (o *Oracle) CreateTable(ctx context.Context, schemaToCreate *Table) (*Table, error) {
	err := o.SQLAdapterBase.CreateTable(ctx, schemaToCreate)
	if err != nil {
		return nil, err
	}
	if !schemaToCreate.Temporary && schemaToCreate.TimestampColumn != "" {
		err = o.createIndex(ctx, schemaToCreate)
		if err != nil {
			o.DropTable(ctx, schemaToCreate.Namespace, schemaToCreate.Name, false)
			return nil, fmt.Errorf("failed to create sort key: %v", err)
		}
	}
	return schemaToCreate, nil
}

func (o *Oracle) createPrimaryKey(ctx context.Context, table *Table) error {
	if table.PKFields.Empty() {
		return nil
	}
	quotedTableName := o.quotedTableName(table.Name)
	quotedSchema := o.namespacePrefix(table.Namespace)
	columnNames := make([]string, table.PKFields.Size())
	for i, column := range table.GetPKFields() {
		columnNames[i] = o.quotedColumnName(column)
	}
	statement := fmt.Sprintf(oracleAlterPrimaryKeyTemplate, quotedSchema,
		quotedTableName, strings.Join(columnNames, ","))
	if _, err := o.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CreatePrimaryKeysError.Wrap(err, "failed to set primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}
	return nil
}

func (o *Oracle) deletePrimaryKey(ctx context.Context, table *Table) error {
	if table.DeletePrimaryKeyNamed == "" {
		return nil
	}
	quotedTableName := o.quotedTableName(table.Name)
	quotedSchema := o.namespacePrefix(table.Namespace)
	query := fmt.Sprintf(oracleDropPrimaryKeyTemplate, quotedSchema, quotedTableName)
	if _, err := o.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.DeletePrimaryKeysError.Wrap(err, "failed to delete primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   query,
			})
	}
	return nil
}

func (o *Oracle) createIndex(ctx context.Context, table *Table) error {
	if table.TimestampColumn == "" {
		return nil
	}
	quotedTableName := o.quotedTableName(table.Name)
	quotedNamespace := o.namespacePrefix(table.Namespace)
	statement := fmt.Sprintf(oracleIndexTemplate, "bulker_timestamp_index",
		quotedNamespace, quotedTableName, o.quotedColumnName(table.TimestampColumn))
	if _, err := o.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.AlterTableError.Wrap(err, "failed to set sort key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}
	return nil
}
