package tigris

import (
	"errors"
	"fmt"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
)

func (s *Storer) Config() (*config.Config, error) {
	raw, err := s.fetchSmall(s.prefix + configKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return config.NewConfig(), nil
	default:
		return nil, fmt.Errorf("tigris: load config: %w", err)
	}

	cfg := config.NewConfig()
	if uerr := cfg.Unmarshal(raw); uerr != nil {
		return nil, fmt.Errorf("tigris: parse config: %w", uerr)
	}
	return cfg, nil
}

func (s *Storer) SetConfig(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("tigris: validate config: %w", err)
	}
	raw, err := cfg.Marshal()
	if err != nil {
		return fmt.Errorf("tigris: marshal config: %w", err)
	}
	if err := s.putBytes(s.prefix+configKey, raw); err != nil {
		return fmt.Errorf("tigris: store config: %w", err)
	}
	return nil
}

// Module refuses submodule storers: they would each need their own bucket (or
// a scheme for nesting prefixes) before the daemon ever serves submodule
// traffic. Explicit beats surprising.
func (s *Storer) Module(name string) (storage.Storer, error) {
	return nil, fmt.Errorf("%w: %q", ErrModulesNotSupported, name)
}
