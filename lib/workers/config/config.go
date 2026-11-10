package config

import (
	"errors"
	"os"
)

// Config is what every worker reads from its environment.
type Config struct {
	TemporalAddress   string
	TemporalNamespace string
	TaskQueue         string
	HealthAddress     string
}

func FromEnv() (*Config, error) {
	c := &Config{
		TemporalAddress:   envOr("TEMPORAL_ADDRESS", "localhost:7233"),
		TemporalNamespace: envOr("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         os.Getenv("TEMPORAL_TASK_QUEUE"),
		HealthAddress:     envOr("HEALTH_CHECK_ADDRESS", ":8080"),
	}
	if c.TaskQueue == "" {
		return nil, errors.New("TEMPORAL_TASK_QUEUE is required")
	}
	return c, nil
}

// KubernetesConfig is read by the worker that holds cluster credentials.
// An empty KubeconfigPath means in-cluster credentials.
type KubernetesConfig struct {
	KubeconfigPath string
}

func KubernetesFromEnv() *KubernetesConfig {
	return &KubernetesConfig{KubeconfigPath: os.Getenv("KUBECONFIG")}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
