package config

import (
	"os"
	"testing"
)

func TestLoadCredentials(t *testing.T) {
	// Test with environment variables
	t.Run("from environment variables", func(t *testing.T) {
		// Set environment variables
		os.Setenv("SYNQ_CLIENT_ID", "test_client_id")
		os.Setenv("SYNQ_CLIENT_SECRET", "test_client_secret")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
		}()

		loader := NewLoader()
		creds, err := loader.LoadCredentials()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if creds.ClientID != "test_client_id" {
			t.Errorf("Expected ClientID 'test_client_id', got '%s'", creds.ClientID)
		}

		if creds.ClientSecret != "test_client_secret" {
			t.Errorf("Expected ClientSecret 'test_client_secret', got '%s'", creds.ClientSecret)
		}
	})

	t.Run("missing credentials", func(t *testing.T) {
		// Ensure environment variables are not set
		os.Unsetenv("SYNQ_CLIENT_ID")
		os.Unsetenv("SYNQ_CLIENT_SECRET")

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		if err == nil {
			t.Error("Expected error for missing credentials, got nil")
		}

		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET"
		if err.Error() != expectedErr {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})

	t.Run("empty credentials", func(t *testing.T) {
		// Set empty environment variables
		os.Setenv("SYNQ_CLIENT_ID", "")
		os.Setenv("SYNQ_CLIENT_SECRET", "")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
		}()

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		if err == nil {
			t.Error("Expected error for empty credentials, got nil")
		}

		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET"
		if err.Error() != expectedErr {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})

	t.Run("whitespace only credentials", func(t *testing.T) {
		// Set whitespace-only environment variables
		os.Setenv("SYNQ_CLIENT_ID", "   ")
		os.Setenv("SYNQ_CLIENT_SECRET", "\t\n")
		defer func() {
			os.Unsetenv("SYNQ_CLIENT_ID")
			os.Unsetenv("SYNQ_CLIENT_SECRET")
		}()

		loader := NewLoader()
		_, err := loader.LoadCredentials()

		if err == nil {
			t.Error("Expected error for whitespace-only credentials, got nil")
		}

		expectedErr := "missing required credentials: SYNQ_CLIENT_ID, SYNQ_CLIENT_SECRET"
		if err.Error() != expectedErr {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, err.Error())
		}
	})
}

func TestNewLoader(t *testing.T) {
	t.Run("default loader", func(t *testing.T) {
		loader := NewLoader()
		if loader == nil {
			t.Error("Expected loader to be created, got nil")
		}
	})

	t.Run("loader with env files", func(t *testing.T) {
		envFiles := []string{".env.test", ".env.local"}
		loader := NewLoader(envFiles...)
		if loader == nil {
			t.Error("Expected loader to be created, got nil")
		}
		if len(loader.envFiles) != 2 {
			t.Errorf("Expected 2 env files, got %d", len(loader.envFiles))
		}
	})
}
