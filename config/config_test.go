package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadCredentials(t *testing.T) {
	// Test with environment variables
	t.Run("from_environment_variables", func(t *testing.T) {
		// Set environment variables
		os.Setenv("SYNQ_CLIENT_ID", "test_client_id")
		os.Setenv("SYNQ_CLIENT_SECRET", "test_client_secret")
		os.Setenv("SYNQ_API_URL", "test_api_url")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
			os.Unsetenv("SYNQ_API_URL")
		}()

		loader := NewLoader()
		creds, err := loader.LoadCredentials()

		assert.NoError(t, err)
		assert.Equal(t, "test_client_id", creds.ClientID)
		assert.Equal(t, "test_client_secret", creds.ClientSecret)
		assert.Equal(t, "test_api_url", creds.ApiUrl)
	})

	t.Run("from_command_line_flags", func(t *testing.T) {
		// Ensure environment variables are not set
		os.Unsetenv("SYNQ_CLIENT_ID")
		os.Unsetenv("SYNQ_CLIENT_SECRET")
		os.Unsetenv("SYNQ_API_URL")

		loader := NewLoader()
		loader.SetFlagCredentials("flag_client_id", "flag_client_secret", "flag_api_url")

		creds, err := loader.LoadCredentials()

		assert.NoError(t, err)
		assert.Equal(t, "flag_client_id", creds.ClientID)
		assert.Equal(t, "flag_client_secret", creds.ClientSecret)
		assert.Equal(t, "flag_api_url", creds.ApiUrl)
	})

	t.Run("command_line_flags_override_environment_variables", func(t *testing.T) {
		// Set environment variables
		os.Setenv("SYNQ_CLIENT_ID", "env_client_id")
		os.Setenv("SYNQ_CLIENT_SECRET", "env_client_secret")
		os.Setenv("SYNQ_API_URL", "env_api_url")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
			os.Unsetenv("SYNQ_API_URL")
		}()

		loader := NewLoader()
		loader.SetFlagCredentials("flag_client_id", "flag_client_secret", "flag_api_url")

		creds, err := loader.LoadCredentials()

		assert.NoError(t, err)
		// Flags should override environment variables
		assert.Equal(t, "flag_client_id", creds.ClientID)
		assert.Equal(t, "flag_client_secret", creds.ClientSecret)
		assert.Equal(t, "flag_api_url", creds.ApiUrl)
	})

	t.Run("partial_flag_override", func(t *testing.T) {
		// Set environment variables
		os.Setenv("SYNQ_CLIENT_ID", "env_client_id")
		os.Setenv("SYNQ_CLIENT_SECRET", "env_client_secret")
		os.Setenv("SYNQ_API_URL", "env_api_url")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
			os.Unsetenv("SYNQ_API_URL")
		}()

		loader := NewLoader()
		loader.SetFlagCredentials("flag_client_id", "", "flag_api_url") // Only override ClientID

		creds, err := loader.LoadCredentials()

		assert.NoError(t, err)
		// Flag should override environment variable for ClientID
		assert.Equal(t, "flag_client_id", creds.ClientID)
		// ClientSecret should come from environment variable
		assert.Equal(t, "env_client_secret", creds.ClientSecret)
		assert.Equal(t, "flag_api_url", creds.ApiUrl)
	})

	t.Run("missing_credentials", func(t *testing.T) {
		// Ensure environment variables are not set
		os.Unsetenv("SYNQ_CLIENT_ID")
		os.Unsetenv("SYNQ_CLIENT_SECRET")
		os.Unsetenv("SYNQ_API_URL")

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		assert.Error(t, err)
		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET, SYNQ_API_URL"
		assert.Equal(t, expectedErr, err.Error())
	})

	t.Run("empty_credentials", func(t *testing.T) {
		// Set empty environment variables
		os.Setenv("SYNQ_CLIENT_ID", "")
		os.Setenv("SYNQ_CLIENT_SECRET", "")
		os.Setenv("SYNQ_API_URL", "")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
			os.Unsetenv("SYNQ_API_URL")
		}()

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		assert.Error(t, err)
		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET, SYNQ_API_URL"
		assert.Equal(t, expectedErr, err.Error())
	})

	t.Run("whitespace_only_credentials", func(t *testing.T) {
		// Set whitespace-only environment variables
		os.Setenv("SYNQ_CLIENT_ID", "   ")
		os.Setenv("SYNQ_CLIENT_SECRET", "\t\n")
		os.Setenv("SYNQ_API_URL", "   ")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
			os.Unsetenv("SYNQ_API_URL")
		}()

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		assert.Error(t, err)
		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET, SYNQ_API_URL"
		assert.Equal(t, expectedErr, err.Error())
	})
}

func TestNewLoader(t *testing.T) {
	t.Run("default_loader", func(t *testing.T) {
		loader := NewLoader()
		assert.NotNil(t, loader)
	})

	t.Run("loader_with_env_files", func(t *testing.T) {
		envFiles := []string{".env.test", ".env.local"}
		loader := NewLoader(envFiles...)
		assert.NotNil(t, loader)
		assert.Len(t, loader.envFiles, 2)
	})
}

func TestSetFlagCredentials(t *testing.T) {
	t.Run("set_flag_credentials", func(t *testing.T) {
		loader := NewLoader()
		loader.SetFlagCredentials("test_id", "test_secret", "test_api_url")

		assert.Equal(t, "test_id", loader.flagClientID)
		assert.Equal(t, "test_secret", loader.flagClientSecret)
		assert.Equal(t, "test_api_url", loader.flagApiUrl)
	})

	t.Run("set_empty_flag_credentials", func(t *testing.T) {
		loader := NewLoader()
		loader.SetFlagCredentials("", "", "")

		assert.Empty(t, loader.flagClientID)
		assert.Empty(t, loader.flagClientSecret)
		assert.Empty(t, loader.flagApiUrl)
	})
}
