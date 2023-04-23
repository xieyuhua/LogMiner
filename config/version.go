
package config

import (
	"fmt"
	"runtime"

	"go.uber.org/zap"
)

// 版本信息
var (
	Version   = "None"
	BuildTS   = "None"
	GitHash   = "None"
	GitBranch = "None"
)

// 版本信息输出重定向到日志
func RecordAppVersion(app string, cfg *Config) {
	zap.L().Info("Welcome to "+app,
		zap.String("Release Version", Version),
		zap.String("Git Commit Hash", GitHash),
		zap.String("Git Branch", GitBranch),
		zap.String("UTC Build Time", BuildTS),
		zap.String("Go Version", runtime.Version()),
		zap.Stringer("config", cfg),
	)
}

func GetRawVersionInfo() string {
	info := ""
	info += fmt.Sprintf("Release Version: %s\n", Version)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", runtime.Version())
	return info
}
