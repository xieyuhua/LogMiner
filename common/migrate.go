
package common

// 数据全量/实时同步 Oracle 版本要求
// 要求 oracle 11g 及以上
const RequireOracleDBVersion = "11"

// Oracle Redo 同步操作类型
const (
	MigrateOperationUpdate   = "UPDATE"
	MigrateOperationInsert   = "INSERT"
	MigrateOperationDelete   = "DELETE"
	MigrateOperationTruncate = "TRUNCATE"
	MigrateOperationDrop     = "DROP"

	MigrateOperationDDL           = "DDL"
	MigrateOperationTruncateTable = "TRUNCATE TABLE"
	MigrateOperationDropTable     = "DROP TABLE"
)

// 用于控制当程序消费追平到当前 CURRENT 重做日志，
// 当值 == 0 启用 filterOracleIncrRecord 大于或者等于逻辑
// 当值 == 1 启用 filterOracleIncrRecord 大于逻辑，避免已被消费得日志一直被重复消费
var MigrateCurrentResetFlag = 0
