package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Name     string `yaml:"name"`
	} `yaml:"database"`
	// ... outras configurações
}

func Load() *Config {
	cfg := &Config{}

	f, err := os.Open("config/config.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(cfg); err != nil {
		panic(err)
	}

	return cfg
}
