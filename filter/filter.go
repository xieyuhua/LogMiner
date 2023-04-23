
package filter

// 表过滤接口
type Filter interface {
	// MatchTable 检查表是否匹配
	MatchTable(table string) bool
}

// tableFilter Filter 接口具体实现
type tableFilter []tableRule

// Parse 序列化 tableFilter 规则列表的 tableFilter
func Parse(args []string) (Filter, error) {
	p := tableRulesParser{make([]tableRule, 0, len(args))}

	for _, arg := range args {
		if err := p.parse(arg); err != nil {
			return nil, err
		}
	}

	return tableFilter(p.rules), nil
}

// MatchTable 检查应用 tableFilter `f` 是否匹配
func (f tableFilter) MatchTable(table string) bool {
	for _, rule := range f {
		if rule.table.matchString(table) {
			return true
		}
	}
	return false
}
