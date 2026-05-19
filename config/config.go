package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

// Config holds runtime configuration values.
type Config struct {
	Symbol           string
	DepthLimit       int
	DataPath         string
	WSBaseURL        string
	RESTBaseURL      string
	FlushIntervalMS  int
	LogLevel         string
	ReconnectDelayMS int
}

// Load reads configuration from environment variables and optional config files.
func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("symbol", "BTCUSDT")
	v.SetDefault("depth_limit", 1000)
	v.SetDefault("data_path", "./data/depth.json")
	v.SetDefault("ws_base_url", "wss://stream.binance.com:9443")
	v.SetDefault("rest_base_url", "https://api.binance.com")
	v.SetDefault("flush_interval_ms", 500)
	v.SetDefault("log_level", "info")
	v.SetDefault("reconnect_delay_ms", 2000)

	v.AutomaticEnv()
	_ = v.BindEnv("symbol", "SYMBOL")
	_ = v.BindEnv("depth_limit", "DEPTH_LIMIT")
	_ = v.BindEnv("data_path", "DATA_PATH")
	_ = v.BindEnv("ws_base_url", "WS_BASE_URL")
	_ = v.BindEnv("rest_base_url", "REST_BASE_URL")
	_ = v.BindEnv("flush_interval_ms", "FLUSH_INTERVAL_MS")
	_ = v.BindEnv("log_level", "LOG_LEVEL")
	_ = v.BindEnv("reconnect_delay_ms", "RECONNECT_DELAY_MS")

	v.SetConfigName("config")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, err
		}
	}

	cfg := &Config{
		Symbol:           strings.ToUpper(v.GetString("symbol")),
		DepthLimit:       v.GetInt("depth_limit"),
		DataPath:         v.GetString("data_path"),
		WSBaseURL:        v.GetString("ws_base_url"),
		RESTBaseURL:      v.GetString("rest_base_url"),
		FlushIntervalMS:  v.GetInt("flush_interval_ms"),
		LogLevel:         strings.ToLower(v.GetString("log_level")),
		ReconnectDelayMS: v.GetInt("reconnect_delay_ms"),
	}

	if cfg.DepthLimit <= 0 {
		cfg.DepthLimit = 5000
	}
	if cfg.FlushIntervalMS <= 0 {
		cfg.FlushIntervalMS = 500
	}
	if cfg.ReconnectDelayMS <= 0 {
		cfg.ReconnectDelayMS = 2000
	}

	return cfg, nil
}
