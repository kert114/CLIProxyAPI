package cursorusage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadFileCredentialsRejectsUnreadableTeamSelection(t *testing.T) {
	dir := t.TempDir()
	authPath, configPath := filepath.Join(dir, "auth.json"), filepath.Join(dir, "cli-config.json")
	if err := os.WriteFile(authPath, []byte(`{"accessToken":"test-access-token"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"authInfo":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFileCredentials(authPath, configPath); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want unavailable rather than silently selecting another account", err)
	}
}

func TestReadFileCredentialsPreservesCLIState(t *testing.T) {
	dir := t.TempDir()
	authPath, configPath := filepath.Join(dir, "auth.json"), filepath.Join(dir, "cli-config.json")
	auth := []byte(`{"accessToken":"test-access-token","refreshToken":"do-not-use"}`)
	config := []byte(`{"authInfo":{"activeTeamId":123},"unrelated":"keep"}`)
	for path, data := range map[string][]byte{authPath: auth, configPath: config} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	credentials, err := ReadFileCredentials(authPath, configPath)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.AccessToken != "test-access-token" || credentials.TeamID != 123 {
		t.Fatal("CLI access token or active team was not loaded")
	}
	for path, want := range map[string][]byte{authPath: auth, configPath: config} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != string(want) {
			t.Fatal("Cursor CLI state changed while reading usage credentials")
		}
	}
}
