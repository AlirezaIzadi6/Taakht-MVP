// Package eligibility loads the static list of allowed categories and neighborhoods.
package eligibility

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config is the subset of config/eligibility.json the ad service validates against.
type Config struct {
	Categories    []string       `json:"categories"`
	Neighborhoods []Neighborhood `json:"neighborhoods"`

	categories    map[string]struct{}
	neighborhoods map[string]struct{}
}

// Neighborhood is an allowed location.
type Neighborhood struct {
	ID string `json:"id"`
}

// New builds a Config from explicit lists (used by tests).
func New(categories, neighborhoodIDs []string) *Config {
	c := &Config{Categories: categories}
	for _, id := range neighborhoodIDs {
		c.Neighborhoods = append(c.Neighborhoods, Neighborhood{ID: id})
	}
	c.index()
	return c
}

// Load reads the eligibility file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("eligibility: %w", err)
	}
	c := &Config{}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("eligibility: parse %s: %w", path, err)
	}
	c.index()
	return c, nil
}

func (c *Config) index() {
	c.categories = make(map[string]struct{}, len(c.Categories))
	for _, v := range c.Categories {
		c.categories[v] = struct{}{}
	}
	c.neighborhoods = make(map[string]struct{}, len(c.Neighborhoods))
	for _, n := range c.Neighborhoods {
		c.neighborhoods[n.ID] = struct{}{}
	}
}

// HasCategory reports whether id is an allowed category.
func (c *Config) HasCategory(id string) bool { _, ok := c.categories[id]; return ok }

// HasNeighborhood reports whether id is an allowed neighborhood.
func (c *Config) HasNeighborhood(id string) bool { _, ok := c.neighborhoods[id]; return ok }
