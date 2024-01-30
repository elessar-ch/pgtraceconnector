package pgtraceconnector

import "fmt"

type Config struct {
	AttributeName string `mapstructure:"attribute_name"`
}

func (c *Config) Validate() error {
	if c.AttributeName == "" {
		return fmt.Errorf("attribute_name must not be empty")
	}
	return nil
}
