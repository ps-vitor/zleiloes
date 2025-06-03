package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	Scraping ScrapingConfig `yaml:"scraping"`
	Database DatabaseConfig `yaml:"database"`
}

type AppConfig struct {
	Name     string `yaml:"name"`
	Env      string `yaml:"env"`
	Debug    bool   `yaml:"debug"`
	Port     int    `yaml:"port"`
	Timezone string `yaml:"timezone"`
}

type ScrapingConfig struct {
	Portalzuk PortalzukConfig `yaml:"portalzuk"`
}

type PortalzukConfig struct {
	BaseURL     string            `yaml:"base_url"`
	Endpoints   map[string]string `yaml:"endpoints"`
	RateLimit   RateLimitConfig   `yaml:"rate_limit"`
	RetryPolicy RetryPolicyConfig `yaml:"retry_policy"`
	UserAgent   string            `yaml:"user_agent"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}

	// Carrega arquivo YAML base
	basePath := filepath.Join("configs", "app.yaml")
	yamlFile, err := os.ReadFile(basePath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(yamlFile, cfg); err != nil {
		return nil, err
	}

	// Carrega configurações específicas de scraping
	scrapingPath := filepath.Join("configs", "scraping.yaml")
	scrapingFile, err := os.ReadFile(scrapingPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(scrapingFile, &cfg.Scraping); err != nil {
		return nil, err
	}

	return cfg, nil
}
