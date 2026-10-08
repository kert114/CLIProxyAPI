package cursorusage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func ReadFileCredentials(authPath, configPath string) (Credentials, error) {
	data, err := os.ReadFile(authPath)
	if err != nil || len(data) > 1024*1024 {
		return Credentials{}, ErrLoginRequired
	}
	var auth struct {
		AccessToken string `json:"accessToken"`
	}
	if json.Unmarshal(data, &auth) != nil || strings.TrimSpace(auth.AccessToken) == "" {
		return Credentials{}, ErrLoginRequired
	}
	teamID, errTeam := readActiveTeam(configPath)
	if errTeam != nil {
		return Credentials{}, errTeam
	}
	return Credentials{AccessToken: auth.AccessToken, TeamID: teamID}, nil
}

func readActiveTeam(path string) (int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil || len(data) > 1024*1024 {
		return 0, ErrUnavailable
	}
	var config struct {
		AuthInfo struct {
			ActiveTeamID int `json:"activeTeamId"`
		} `json:"authInfo"`
	}
	if json.Unmarshal(data, &config) != nil || config.AuthInfo.ActiveTeamID < 0 || config.AuthInfo.ActiveTeamID > 2147483647 {
		return 0, ErrUnavailable
	}
	return config.AuthInfo.ActiveTeamID, nil
}

func LoadCLICredentials(ctx context.Context) (Credentials, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Credentials{}, ErrLoginRequired
	}
	configDir := os.Getenv("CURSOR_CONFIG_DIR")
	if configDir == "" {
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			configDir = filepath.Join(xdg, "cursor")
		} else {
			configDir = filepath.Join(home, ".cursor")
		}
	}
	configPath := filepath.Join(configDir, "cli-config.json")
	backend := os.Getenv("AGENT_CLI_CREDENTIAL_STORE")
	if backend == "memory" {
		return Credentials{}, ErrLoginRequired
	}
	if runtime.GOOS == "darwin" && backend != "file" {
		credentialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		output, err := exec.CommandContext(credentialCtx, "/usr/bin/security", "find-generic-password", "-a", "cursor-user", "-s", "cursor-access-token", "-w").Output()
		if err != nil || strings.TrimSpace(string(output)) == "" {
			return Credentials{}, ErrLoginRequired
		}
		teamID, errTeam := readActiveTeam(configPath)
		if errTeam != nil {
			return Credentials{}, errTeam
		}
		return Credentials{AccessToken: strings.TrimSpace(string(output)), TeamID: teamID}, nil
	}
	var authPath string
	switch runtime.GOOS {
	case "darwin":
		authPath = filepath.Join(home, ".cursor", "auth.json")
	case "windows":
		if appData := os.Getenv("APPDATA"); appData != "" {
			authPath = filepath.Join(appData, "Cursor", "auth.json")
		} else {
			return Credentials{}, ErrLoginRequired
		}
	default:
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			authPath = filepath.Join(xdg, "cursor", "auth.json")
		} else {
			authPath = filepath.Join(home, ".config", "cursor", "auth.json")
		}
	}
	return ReadFileCredentials(authPath, configPath)
}
