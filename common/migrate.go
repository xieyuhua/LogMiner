/*
 * @Author: "xieyuhua" "1130
 * @Date: 2023-04-21 15:10:24
 * @LastEditors: "xieyuhua" "1130
 * @LastEditTime: 2024-03-04 08:35:19
 * @FilePath: \LogMiner\common\migrate.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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
