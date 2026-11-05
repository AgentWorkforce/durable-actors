package redisliveexporter

import (
	"errors"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config locates the Redis that control-plane replicas subscribe to for live telemetry.
type Config struct {
	// Endpoint is a redis:// or rediss:// URL; it may carry the password.
	Endpoint configopaque.String `mapstructure:"endpoint"`
	// CAFile optionally trusts a private CA for rediss:// endpoints.
	CAFile string `mapstructure:"ca_file"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	_, err := redis.ParseURL(string(c.Endpoint))
	return err
}
