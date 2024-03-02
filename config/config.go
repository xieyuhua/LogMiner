package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"gominerlog/common"
	"os"

	"github.com/BurntSushi/toml"
)

// 程序配置文件
type Config struct {
	*flag.FlagSet `json:"-"`
	AppConfig     AppConfig    `toml:"app" json:"app"`
	OracleConfig  OracleConfig `toml:"oracle" json:"oracle"`
	LogConfig     LogConfig    `toml:"log" json:"log"`
	ConfigFile    string       `json:"config-file"`
	AllConfig     AllConfig    `toml:"all" json:"all"`
	PrintVersion  bool
	TaskMode      string `json:"task-mode"`
	DBTypeS       string `json:"db-type-s"`
	DBTypeT       string `json:"db-type-t"`
}

type AllConfig struct {
	LogminerQueryTimeout int `toml:"logminer-query-timeout" json:"logminer-query-timeout"`
	FilterThreads        int `toml:"filter-threads" json:"filter-threads"`
	ApplyThreads         int `toml:"apply-threads" json:"apply-threads"`
	WorkerQueue          int `toml:"worker-queue" json:"worker-queue"`
	WorkerThreads        int `toml:"worker-threads" json:"worker-threads"`
}

type AppConfig struct {
	InsertBatchSize  int    `toml:"insert-batch-size" json:"insert-batch-size"`
	SlowlogThreshold int    `toml:"slowlog-threshold" json:"slowlog-threshold"`
	PprofPort        string `toml:"pprof-port" json:"pprof-port"`
}

type OracleConfig struct {
	Username      string   `toml:"username" json:"username"`
	Password      string   `toml:"password" json:"password"`
	Host          string   `toml:"host" json:"host"`
	Port          int      `toml:"port" json:"port"`
	ServiceName   string   `toml:"service-name" json:"service-name"`
	PDBName       string   `toml:"pdb-name" json:"pdb-name"`
	LibDir        string   `toml:"lib-dir" json:"lib-dir"`
	NLSLang       string   `toml:"nls-lang" json:"nls-lang"`
	ConnectParams string   `toml:"connect-params" json:"connect-params"`
	SessionParams []string `toml:"session-params" json:"session-params"`
	SchemaName    string   `toml:"schema-name" json:"schema-name"`
	IncludeTable  []string `toml:"include-table" json:"include-table"`
	ExcludeTable  []string `toml:"exclude-table" json:"exclude-table"`
}

type LogConfig struct {
	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("transferdb", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of transferdb:")
		fs.
			PrintDefaults()
	}
	fs.BoolVar(&cfg.PrintVersion, "V", false, "print version information and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "./config.toml", "path to the configuration file")
	fs.StringVar(&cfg.TaskMode, "mode", "", "specify the program running mode: [full、incr、all = full + incr]")
	fs.StringVar(&cfg.DBTypeS, "source", "oracle", "specify the source db type")
	fs.StringVar(&cfg.DBTypeT, "target", "mysql", "specify the target db type")
	return cfg
}

func (c *Config) Parse(args []string) error {
	err := c.FlagSet.Parse(args)
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	if c.PrintVersion {
		fmt.Println(GetRawVersionInfo())
		os.Exit(0)
	}

	if c.ConfigFile != "" {
		if err = c.configFromFile(c.ConfigFile); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no config file")
	}

	c.AdjustConfig()

	return nil
}

// 加载配置文件并解析
func (c *Config) configFromFile(file string) error {
	if _, err := toml.DecodeFile(file, c); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	return nil
}

func (c *Config) AdjustConfig() {
	c.DBTypeS = common.StringUPPER(c.DBTypeS)
	c.DBTypeT = common.StringUPPER(c.DBTypeT)
	c.TaskMode = common.StringUPPER(c.TaskMode)
	c.OracleConfig.SchemaName = common.StringUPPER(c.OracleConfig.SchemaName)
	c.OracleConfig.PDBName = common.StringUPPER(c.OracleConfig.PDBName)
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
