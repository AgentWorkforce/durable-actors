package redisliveexporter

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// A claim lives while its sandbox reports usage, and a sandbox serves one actor for its life.
const sandboxTTL = time.Hour

// liveStore is the Redis that control-plane replicas subscribe to, which also holds the actor each
// sandbox was claimed for, so every collector replica and signal reads the same claims.
type liveStore interface {
	publish(ctx context.Context, messages map[string][]byte) error
	remember(ctx context.Context, actors map[string]string) error
	recall(ctx context.Context, sandboxes []string) (map[string]string, error)
	close() error
}

func sandboxKey(sandbox string) string {
	return "durable-actors:v1:sandbox:" + sandbox
}

type redisStore struct {
	client *redis.Client
}

func (s redisStore) publish(ctx context.Context, messages map[string][]byte) error {
	pipeline := s.client.Pipeline()
	for channel, payload := range messages {
		pipeline.Publish(ctx, channel, payload)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func (s redisStore) remember(ctx context.Context, actors map[string]string) error {
	if len(actors) == 0 {
		return nil
	}
	pipeline := s.client.Pipeline()
	for sandbox, actor := range actors {
		pipeline.Set(ctx, sandboxKey(sandbox), actor, sandboxTTL)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

// recall refreshes each claim it finds, so a sandbox that keeps reporting keeps its claim.
func (s redisStore) recall(ctx context.Context, sandboxes []string) (map[string]string, error) {
	pipeline := s.client.Pipeline()
	reads := make(map[string]*redis.StringCmd, len(sandboxes))
	for _, sandbox := range sandboxes {
		reads[sandbox] = pipeline.GetEx(ctx, sandboxKey(sandbox), sandboxTTL)
	}
	if _, err := pipeline.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	found := make(map[string]string, len(reads))
	for sandbox, read := range reads {
		if actor, err := read.Result(); err == nil {
			found[sandbox] = actor
		}
	}
	return found, nil
}

func (s redisStore) close() error {
	return s.client.Close()
}
