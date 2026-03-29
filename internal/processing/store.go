package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	jobKeyPrefix = "job:"
	JobTTL       = 24 * time.Hour
)

// ErrJobNotFound is returned when a job does not exist in the store (expired or never created).
var ErrJobNotFound = errors.New("job not found")

// JobStore abstracts persistence of JobRecord values.
type JobStore interface {
	Save(ctx context.Context, record *JobRecord) error
	Get(ctx context.Context, project, jobID string) (*JobRecord, error)
}

func jobRedisKey(project, jobID string) string {
	return jobKeyPrefix + project + ":" + jobID
}

// RedisJobStore is the production implementation of JobStore backed by Redis.
type RedisJobStore struct {
	rdb *redis.Client
}

// NewRedisJobStore creates a RedisJobStore from an existing *redis.Client.
func NewRedisJobStore(rdb *redis.Client) *RedisJobStore {
	return &RedisJobStore{rdb: rdb}
}

// Save marshals a JobRecord and writes it to Redis with a 24-hour TTL.
func (s *RedisJobStore) Save(ctx context.Context, r *JobRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshaling job record: %w", err)
	}
	if err := s.rdb.Set(ctx, jobRedisKey(r.Project, r.JobID), string(data), JobTTL).Err(); err != nil {
		return fmt.Errorf("redis set job: %w", err)
	}
	return nil
}

// Get retrieves and unmarshals a JobRecord.
// Returns ErrJobNotFound when the key does not exist (expired or never written).
func (s *RedisJobStore) Get(ctx context.Context, project, jobID string) (*JobRecord, error) {
	val, err := s.rdb.Get(ctx, jobRedisKey(project, jobID)).Result()
	if err == redis.Nil {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis get job: %w", err)
	}
	var r JobRecord
	if err := json.Unmarshal([]byte(val), &r); err != nil {
		return nil, fmt.Errorf("unmarshal job record: %w", err)
	}
	return &r, nil
}
