package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Credentials holds the authentication credentials
type Credentials struct {
	ClientID     string
	ClientSecret string
	ApiUrl       string
}

// Loader handles loading configuration from various sources
type Loader struct {
	envFiles []string
	// Command line overrides
	flagClientID     string
	flagClientSecret string
	flagApiUrl       string
}

// NewLoader creates a new configuration loader
func NewLoader(envFiles ...string) *Loader {
	return &Loader{
		envFiles: envFiles,
	}
}

// SetFlagCredentials sets credentials from command line flags
func (l *Loader) SetFlagCredentials(clientID, clientSecret, apiUrl string) {
	l.flagClientID = clientID
	l.flagClientSecret = clientSecret
	l.flagApiUrl = apiUrl
}

// LoadCredentials loads client credentials with priority: command line flags > environment variables > .env files
func (l *Loader) LoadCredentials() (*Credentials, error) {
	// First, try to load from .env files
	if err := l.loadEnvFiles(); err != nil {
		return nil, fmt.Errorf("failed to load .env files: %w", err)
	}

	// Load credentials with priority order
	creds := &Credentials{}

	// Priority 1: Command line flags (highest priority)
	if l.flagClientID != "" {
		creds.ClientID = l.flagClientID
	} else {
		creds.ClientID = os.Getenv("SYNQ_CLIENT_ID")
	}

	if l.flagClientSecret != "" {
		creds.ClientSecret = l.flagClientSecret
	} else {
		creds.ClientSecret = os.Getenv("SYNQ_CLIENT_SECRET")
	}

	if l.flagApiUrl != "" {
		creds.ApiUrl = l.flagApiUrl
	} else {
		creds.ApiUrl = os.Getenv("SYNQ_API_URL")
	}

	// Validate credentials
	if err := l.validateCredentials(creds); err != nil {
		return nil, err
	}

	return creds, nil
}

// loadEnvFiles loads environment variables from .env files
func (l *Loader) loadEnvFiles() error {
	if len(l.envFiles) == 0 {
		// Default to .env in current directory
		if err := godotenv.Load(); err != nil {
			// It's okay if .env doesn't exist, just log it
			return nil
		}
		return nil
	}

	// Load specified .env files
	for _, envFile := range l.envFiles {
		if err := godotenv.Load(envFile); err != nil {
			return fmt.Errorf("failed to load %s: %w", envFile, err)
		}
	}

	return nil
}

// validateCredentials ensures all required credentials are present
func (l *Loader) validateCredentials(creds *Credentials) error {
	var missing []string

	if strings.TrimSpace(creds.ClientID) == "" {
		missing = append(missing, "SYNQ_CLIENT_ID")
	}

	if strings.TrimSpace(creds.ClientSecret) == "" {
		missing = append(missing, "SYNQ_CLIENT_SECRET")
	}

	if strings.TrimSpace(creds.ApiUrl) == "" {
		missing = append(missing, "SYNQ_API_URL")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required credentials: %s", strings.Join(missing, ", "))
	}

	return nil
}

// MustLoadCredentials loads credentials and panics if there's an error
func (l *Loader) MustLoadCredentials() *Credentials {
	creds, err := l.LoadCredentials()
	if err != nil {
		panic(fmt.Sprintf("failed to load credentials: %v", err))
	}
	return creds
}
