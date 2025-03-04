package parser

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// 定义SQL语句类型
const (
	SelectStmt = iota
	InsertStmt
	UpdateStmt
	DeleteStmt
	CreateTableStmt
	CreateIndexStmt
	DropIndexStmt
)

// 定义错误类型
var (
	ErrInvalidSyntax = errors.New("invalid SQL syntax")
	ErrUnsupported   = errors.New("unsupported SQL statement")
)

// Statement 表示一个SQL语句
type Statement struct {
	Type      int              // 语句类型
	Table     string           // 表名
	Columns   []string         // 列名
	Values    [][]string       // 值（用于INSERT）
	Where     *Condition       // WHERE条件
	GroupBy   []string         // GROUP BY子句
	OrderBy   []*OrderByItem   // ORDER BY子句
	Limit     int              // LIMIT子句
	Offset    int              // OFFSET子句
	AggFuncs  []*AggregateFunc // 聚合函数
	IndexName string           // 索引名称
	IndexType int              // 索引类型
	Unique    bool             // 是否唯一索引
}

// AggregateFunc 表示聚合函数
type AggregateFunc struct {
	Name   string // 函数名 (MAX, MIN, SUM)
	Column string // 列名
}

// Condition 表示WHERE条件
type Condition struct {
	Column    string
	Operator  string // =, >, <, >=, <=, !=, IN
	Value     string
	Values    []string // 用于IN操作符的多个值
	NextLogic string   // AND, OR
	Next      *Condition
}

// OrderByItem 表示ORDER BY项
type OrderByItem struct {
	Column string
	Desc   bool // 是否降序排序
}

// Parser SQL解析器
type Parser struct {
	charset string // 字符集编码
}

// NewParser 创建一个新的SQL解析器
func NewParser() *Parser {
	return &Parser{
		charset: "utf8mb4", // 默认使用utf8mb4字符集
	}
}

// Parse 解析SQL语句
func (p *Parser) Parse(sql string) (*Statement, error) {
	// 去除多余空格并转为小写以简化解析
	sql = strings.TrimSpace(sql)

	// 检查SQL类型
	upperSQL := strings.ToUpper(sql)
	if strings.HasPrefix(upperSQL, "SELECT") {
		return p.parseSelect(sql)
	} else if strings.HasPrefix(upperSQL, "INSERT") {
		return p.parseInsert(sql)
	} else if strings.HasPrefix(upperSQL, "UPDATE") {
		return p.parseUpdate(sql)
	} else if strings.HasPrefix(upperSQL, "DELETE") {
		return p.parseDelete(sql)
	} else if strings.HasPrefix(upperSQL, "CREATE TABLE") {
		return p.parseCreateTable(sql)
	} else if strings.HasPrefix(upperSQL, "CREATE INDEX") || strings.HasPrefix(upperSQL, "CREATE UNIQUE INDEX") {
		return p.parseCreateIndex(sql)
	} else if strings.HasPrefix(upperSQL, "DROP INDEX") {
		return p.parseDropIndex(sql)
	}

	return nil, ErrUnsupported
}

// parseSelect 解析SELECT语句
func (p *Parser) parseSelect(sql string) (*Statement, error) {
	// 创建语句对象
	stmt := &Statement{
		Type:   SelectStmt,
		Limit:  -1, // 默认不限制
		Offset: 0,
	}

	// 解析LIMIT和OFFSET子句（如果有）
	// 先提取这些子句，以便后续正则表达式匹配更简单
	sql, stmt.Limit, stmt.Offset = extractLimitOffset(sql)

	// 解析ORDER BY子句（如果有）
	var orderByStr string
	sql, orderByStr = extractOrderBy(sql)
	if orderByStr != "" {
		stmt.OrderBy = parseOrderBy(orderByStr)
	}

	// 解析GROUP BY子句（如果有）
	var groupByStr string
	sql, groupByStr = extractGroupBy(sql)
	if groupByStr != "" {
		stmt.GroupBy = parseGroupBy(groupByStr)
	}

	// 使用正则表达式匹配SELECT语句的基本部分
	// 格式: SELECT column1, column2, ... FROM table [WHERE condition]
	selectRegex := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)
	matches := selectRegex.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, ErrInvalidSyntax
	}

	// 解析列名
	columnsStr := matches[1]
	stmt.Columns = parseColumns(columnsStr)

	// 解析聚合函数
	stmt.AggFuncs = parseAggregates(columnsStr)

	// 获取表名
	stmt.Table = matches[2]

	// 解析WHERE条件（如果有）
	if len(matches) > 3 && matches[3] != "" {
		condition, err := parseCondition(matches[3])
		if err != nil {
			return nil, err
		}
		stmt.Where = condition
	}

	return stmt, nil
}

// parseInsert 解析INSERT语句
func (p *Parser) parseInsert(sql string) (*Statement, error) {
	// 使用正则表达式匹配INSERT语句的各个部分
	// 格式: INSERT INTO table (column1, column2, ...) VALUES (value1, value2, ...), ...
	insertRegex := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)\s*\((.+?)\)\s*VALUES\s*(.+)$`)
	matches := insertRegex.FindStringSubmatch(sql)

	if len(matches) < 4 {
		return nil, ErrInvalidSyntax
	}

	// 获取表名
	table := matches[1]

	// 解析列名
	columnsStr := matches[2]
	columns := parseColumns(columnsStr)

	// 解析值
	valuesStr := matches[3]
	values, err := parseValues(valuesStr)
	if err != nil {
		return nil, err
	}

	// 创建语句对象
	return &Statement{
		Type:    InsertStmt,
		Table:   table,
		Columns: columns,
		Values:  values,
	}, nil
}

// parseUpdate 解析UPDATE语句
func (p *Parser) parseUpdate(sql string) (*Statement, error) {
	// 使用正则表达式匹配UPDATE语句的各个部分
	// 格式: UPDATE table SET column1 = value1, column2 = value2, ... [WHERE condition]
	updateRegex := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$`)
	matches := updateRegex.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, ErrInvalidSyntax
	}

	// 获取表名
	table := matches[1]

	// 解析SET部分
	setStr := matches[2]
	columns, values, err := parseSetClause(setStr)
	if err != nil {
		return nil, err
	}

	// 创建语句对象
	stmt := &Statement{
		Type:    UpdateStmt,
		Table:   table,
		Columns: columns,
		Values:  [][]string{values}, // 对于UPDATE，只有一组值
	}

	// 解析WHERE条件（如果有）
	if len(matches) > 3 && matches[3] != "" {
		condition, err := parseCondition(matches[3])
		if err != nil {
			return nil, err
		}
		stmt.Where = condition
	}

	return stmt, nil
}

// parseDelete 解析DELETE语句
func (p *Parser) parseDelete(sql string) (*Statement, error) {
	// 使用正则表达式匹配DELETE语句的各个部分
	// 格式: DELETE FROM table [WHERE condition]
	deleteRegex := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)
	matches := deleteRegex.FindStringSubmatch(sql)

	if len(matches) < 2 {
		return nil, ErrInvalidSyntax
	}

	// 获取表名
	table := matches[1]

	// 创建语句对象
	stmt := &Statement{
		Type:  DeleteStmt,
		Table: table,
	}

	// 解析WHERE条件（如果有）
	if len(matches) > 2 && matches[2] != "" {
		condition, err := parseCondition(matches[2])
		if err != nil {
			return nil, err
		}
		stmt.Where = condition
	}

	return stmt, nil
}

// parseCreateTable 解析CREATE TABLE语句
func (p *Parser) parseCreateTable(sql string) (*Statement, error) {
	// 使用正则表达式匹配CREATE TABLE语句的各个部分
	// 格式: CREATE TABLE table (column1 type1, column2 type2, ...)
	createRegex := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)\s*\((.+?)\)$`)
	matches := createRegex.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, ErrInvalidSyntax
	}

	// 获取表名
	table := matches[1]

	// 解析列定义
	columnsDefStr := matches[2]
	columns := parseColumnDefinitions(columnsDefStr)

	// 创建语句对象
	return &Statement{
		Type:    CreateTableStmt,
		Table:   table,
		Columns: columns,
	}, nil
}

// parseColumns 解析列名列表，同时识别聚合函数
func parseColumns(columnsStr string) []string {
	columns := []string{}
	for _, col := range strings.Split(columnsStr, ",") {
		columns = append(columns, strings.TrimSpace(col))
	}
	return columns
}

// parseAggregates 解析聚合函数
func parseAggregates(columnsStr string) []*AggregateFunc {
	aggFuncs := []*AggregateFunc{}

	// 匹配聚合函数模式：MAX(column), MIN(column), SUM(column), AVG(column)
	aggRegex := regexp.MustCompile(`(?i)(MAX|MIN|SUM|AVG)\(([^\)]+)\)`)
	matches := aggRegex.FindAllStringSubmatch(columnsStr, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			funcName := strings.ToUpper(match[1])
			colName := strings.TrimSpace(match[2])
			aggFuncs = append(aggFuncs, &AggregateFunc{
				Name:   funcName,
				Column: colName,
			})
		}
	}

	return aggFuncs
}

// parseValues 解析VALUES部分
func parseValues(valuesStr string) ([][]string, error) {
	// 匹配所有的值组 (value1, value2, ...), ...
	valueGroupsRegex := regexp.MustCompile(`\(([^\(\)]+)\)`)
	valueGroups := valueGroupsRegex.FindAllStringSubmatch(valuesStr, -1)

	if len(valueGroups) == 0 {
		return nil, ErrInvalidSyntax
	}

	result := [][]string{}
	for _, group := range valueGroups {
		if len(group) < 2 {
			continue
		}

		valueStr := group[1]
		values := []string{}

		// 更智能地处理引号内的值和逗号
		inQuote := false
		quoteChar := '\x00'
		currentVal := ""

		for _, char := range valueStr {
			switch char {
			case '\'':
				if !inQuote {
					inQuote = true
					quoteChar = '\''
				} else if quoteChar == '\'' {
					inQuote = false
					quoteChar = '\x00'
				} else {
					currentVal += string(char)
				}
			case '"':
				if !inQuote {
					inQuote = true
					quoteChar = '"'
				} else if quoteChar == '"' {
					inQuote = false
					quoteChar = '\x00'
				} else {
					currentVal += string(char)
				}
			case ',':
				if inQuote {
					currentVal += string(char)
				} else {
					// 添加当前值并重置
					values = append(values, strings.TrimSpace(currentVal))
					currentVal = ""
				}
			default:
				currentVal += string(char)
			}
		}

		// 添加最后一个值
		if currentVal != "" {
			values = append(values, strings.TrimSpace(currentVal))
		}

		result = append(result, values)
	}

	return result, nil
}

// parseSetClause 解析SET子句
func parseSetClause(setStr string) ([]string, []string, error) {
	parts := strings.Split(setStr, ",")
	columns := []string{}
	values := []string{}

	for _, part := range parts {
		assignment := strings.Split(part, "=")
		if len(assignment) != 2 {
			return nil, nil, ErrInvalidSyntax
		}

		column := strings.TrimSpace(assignment[0])
		value := strings.TrimSpace(assignment[1])

		// 去除引号
		if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
			(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
			value = value[1 : len(value)-1]
		}

		columns = append(columns, column)
		values = append(values, value)
	}

	return columns, values, nil
}

// parseColumnDefinitions 解析列定义
func parseColumnDefinitions(columnsDefStr string) []string {
	columns := []string{}
	for _, colDef := range strings.Split(columnsDefStr, ",") {
		// 提取列名（忽略类型信息）
		colParts := strings.Fields(strings.TrimSpace(colDef))
		if len(colParts) > 0 {
			columns = append(columns, colParts[0])
		}
	}
	return columns
}

// parseCondition 解析WHERE条件
func parseCondition(conditionStr string) (*Condition, error) {
	// 支持复杂条件解析，包括AND、OR、各种比较运算符

	// 检查是否有AND或OR
	andIndex := strings.Index(strings.ToUpper(conditionStr), " AND ")
	orIndex := strings.Index(strings.ToUpper(conditionStr), " OR ")

	if andIndex >= 0 || orIndex >= 0 {
		// 确定哪个逻辑运算符先出现
		var splitIndex, logicLen int
		var nextLogic string

		if (andIndex >= 0 && orIndex >= 0 && andIndex < orIndex) || (andIndex >= 0 && orIndex < 0) {
			splitIndex = andIndex
			logicLen = 5 // " AND "
			nextLogic = "AND"
		} else {
			splitIndex = orIndex
			logicLen = 4 // " OR "
			nextLogic = "OR"
		}

		// 解析第一个条件
		firstCondStr := conditionStr[:splitIndex]
		firstCond, err := parseSimpleCondition(firstCondStr)
		if err != nil {
			return nil, err
		}

		// 解析剩余条件
		remainCondStr := conditionStr[splitIndex+logicLen:]
		nextCond, err := parseCondition(remainCondStr)
		if err != nil {
			return nil, err
		}

		// 设置逻辑关系
		firstCond.NextLogic = nextLogic
		firstCond.Next = nextCond

		return firstCond, nil
	}

	// 如果没有逻辑运算符，解析简单条件
	return parseSimpleCondition(conditionStr)
}

// parseSimpleCondition 解析简单条件（无AND/OR）
func parseSimpleCondition(condStr string) (*Condition, error) {
	// 支持的运算符
	operators := []string{"=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"}

	// 查找运算符
	var operator string
	var splitIndex int

	// 先检查是否有LIKE或IN操作符（不区分大小写）
	likeIndex := strings.Index(strings.ToUpper(condStr), " LIKE ")
	inIndex := strings.Index(strings.ToUpper(condStr), " IN ")
	if likeIndex >= 0 {
		operator = "LIKE"
		splitIndex = likeIndex
	} else if inIndex >= 0 {
		operator = "IN"
		splitIndex = inIndex
	} else {
		// 查找其他运算符
		for _, op := range operators[:len(operators)-1] { // 排除LIKE，因为已经单独处理
			if idx := strings.Index(condStr, op); idx >= 0 {
				// 找到最先出现的运算符
				if operator == "" || idx < splitIndex {
					operator = op
					splitIndex = idx
				}
			}
		}
	}

	if operator == "" {
		return nil, ErrInvalidSyntax
	}

	// 提取列名和值
	column := strings.TrimSpace(condStr[:splitIndex])
	valueStr := strings.TrimSpace(condStr[splitIndex+len(operator):])

	// 处理IN操作符的值列表
	if operator == "IN" {
		// 检查是否是有效的IN语法 (value1, value2, ...)
		if !strings.HasPrefix(valueStr, "(") || !strings.HasSuffix(valueStr, ")") {
			return nil, ErrInvalidSyntax
		}
		// 去除括号
		valueStr = valueStr[1 : len(valueStr)-1]
		// 分割值
		values := []string{}
		for _, v := range strings.Split(valueStr, ",") {
			v = strings.TrimSpace(v)
			// 去除引号
			if (strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")) ||
				(strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"")) {
				v = v[1 : len(v)-1]
			}
			values = append(values, v)
		}
		return &Condition{
			Column:   column,
			Operator: operator,
			Values:   values,
		}, nil
	}

	// 处理其他操作符的单个值
	value := valueStr
	// 去除引号
	if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
		(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
		value = value[1 : len(value)-1]
	}

	return &Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	}, nil
}

// extractLimitOffset 从SQL语句中提取LIMIT和OFFSET子句
func extractLimitOffset(sql string) (string, int, int) {
	// 默认值
	limit := -1
	offset := 0

	// 提取LIMIT子句
	limitRegex := regexp.MustCompile(`(?i)\s+LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\s*$`)
	limitMatches := limitRegex.FindStringSubmatch(sql)

	if len(limitMatches) > 1 {
		// 提取LIMIT值
		limitVal, _ := strconv.Atoi(limitMatches[1])
		limit = limitVal

		// 提取OFFSET值（如果有）
		if len(limitMatches) > 2 && limitMatches[2] != "" {
			offsetVal, _ := strconv.Atoi(limitMatches[2])
			offset = offsetVal
		}

		// 从SQL中移除LIMIT子句
		sql = limitRegex.ReplaceAllString(sql, "")
	} else {
		// 检查单独的OFFSET子句
		offsetRegex := regexp.MustCompile(`(?i)\s+OFFSET\s+(\d+)\s*$`)
		offsetMatches := offsetRegex.FindStringSubmatch(sql)

		if len(offsetMatches) > 1 {
			offsetVal, _ := strconv.Atoi(offsetMatches[1])
			offset = offsetVal

			// 从SQL中移除OFFSET子句
			sql = offsetRegex.ReplaceAllString(sql, "")
		}
	}

	return sql, limit, offset
}

// extractOrderBy 从SQL语句中提取ORDER BY子句
func extractOrderBy(sql string) (string, string) {
	// 提取ORDER BY子句
	orderByRegex := regexp.MustCompile(`(?i)\s+ORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s+OFFSET|\s*$)`)
	orderByMatches := orderByRegex.FindStringSubmatch(sql)

	if len(orderByMatches) > 1 {
		orderByStr := orderByMatches[1]

		// 从SQL中移除ORDER BY子句
		orderByRemoveRegex := regexp.MustCompile(`(?i)\s+ORDER\s+BY\s+` + regexp.QuoteMeta(orderByStr))
		sql = orderByRemoveRegex.ReplaceAllString(sql, "")

		return sql, orderByStr
	}

	return sql, ""
}

// extractGroupBy 从SQL语句中提取GROUP BY子句
func extractGroupBy(sql string) (string, string) {
	// 提取GROUP BY子句
	groupByRegex := regexp.MustCompile(`(?i)\s+GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|\s*$)`)
	groupByMatches := groupByRegex.FindStringSubmatch(sql)

	if len(groupByMatches) > 1 {
		groupByStr := groupByMatches[1]

		// 从SQL中移除GROUP BY子句
		groupByRemoveRegex := regexp.MustCompile(`(?i)\s+GROUP\s+BY\s+` + regexp.QuoteMeta(groupByStr))
		sql = groupByRemoveRegex.ReplaceAllString(sql, "")

		return sql, groupByStr
	}

	return sql, ""
}

// parseOrderBy 解析ORDER BY子句
func parseOrderBy(orderByStr string) []*OrderByItem {
	result := []*OrderByItem{}

	// 分割多个排序项
	for _, item := range strings.Split(orderByStr, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		// 检查是否有DESC或ASC关键字
		isDesc := false
		if strings.HasSuffix(strings.ToUpper(item), " DESC") {
			isDesc = true
			item = strings.TrimSuffix(strings.TrimSpace(item), " DESC")
			item = strings.TrimSuffix(strings.TrimSpace(item), " desc")
		} else if strings.HasSuffix(strings.ToUpper(item), " ASC") {
			item = strings.TrimSuffix(strings.TrimSpace(item), " ASC")
			item = strings.TrimSuffix(strings.TrimSpace(item), " asc")
		}

		result = append(result, &OrderByItem{
			Column: item,
			Desc:   isDesc,
		})
	}

	return result
}

// parseCreateIndex 解析CREATE INDEX语句
func (p *Parser) parseCreateIndex(sql string) (*Statement, error) {
	// 检查是否是唯一索引
	isUnique := false
	if strings.HasPrefix(strings.ToUpper(sql), "CREATE UNIQUE INDEX") {
		isUnique = true
	}

	// 使用正则表达式匹配CREATE INDEX语句的各个部分
	// 格式: CREATE [UNIQUE] INDEX index_name ON table_name (column1, column2, ...)
	var createRegex *regexp.Regexp
	if isUnique {
		createRegex = regexp.MustCompile(`(?i)CREATE\s+UNIQUE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+?)\)$`)
	} else {
		createRegex = regexp.MustCompile(`(?i)CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+?)\)$`)
	}

	matches := createRegex.FindStringSubmatch(sql)
	if len(matches) < 4 {
		return nil, ErrInvalidSyntax
	}

	// 获取索引名称、表名和列名
	indexName := matches[1]
	tableName := matches[2]
	columnsStr := matches[3]
	columns := parseColumns(columnsStr)

	// 创建语句对象
	return &Statement{
		Type:      CreateIndexStmt,
		Table:     tableName,
		Columns:   columns,
		IndexName: indexName,
		IndexType: 0, // 默认使用B树索引
		Unique:    isUnique,
	}, nil
}

// parseDropIndex 解析DROP INDEX语句
func (p *Parser) parseDropIndex(sql string) (*Statement, error) {
	// 使用正则表达式匹配DROP INDEX语句的各个部分
	// 格式: DROP INDEX index_name ON table_name
	dropRegex := regexp.MustCompile(`(?i)DROP\s+INDEX\s+(\w+)\s+ON\s+(\w+)$`)
	matches := dropRegex.FindStringSubmatch(sql)

	if len(matches) < 3 {
		return nil, ErrInvalidSyntax
	}

	// 获取索引名称和表名
	indexName := matches[1]
	tableName := matches[2]

	// 创建语句对象
	return &Statement{
		Type:      DropIndexStmt,
		Table:     tableName,
		IndexName: indexName,
	}, nil
}

// parseGroupBy 解析GROUP BY子句
func parseGroupBy(groupByStr string) []string {
	result := []string{}

	// 分割多个分组列
	for _, col := range strings.Split(groupByStr, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			result = append(result, col)
		}
	}

	return result
}
