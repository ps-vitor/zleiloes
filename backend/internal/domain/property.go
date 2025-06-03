type Config struct {
	Database struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		// ... other fields
	} `yaml:"database"`
	// ... other config sections
}

func LoadConfig() (*Config, error) {
	// implement config loading
}