package locus

import (
	"github.com/Brijeshlakkad/goutube/internal/log"
	"github.com/go-redis/redis"
	"github.com/pelletier/go-toml"
)

// RedisPointStore manages points and its last offset
type RedisPointStore struct {
	redis *redis.Client
	log   *log.Log // Backup to warm up the cache
}

type redisLogPoint struct {
	Point  string
	Offset uint64
}

type redisPointValue struct {
	PointOffset uint64
	LogOffset   uint64
}

func (rps *RedisPointStore) AddPointEvent(pointId string, offset uint64) error {
	logOffset, err := rps.log.Append(&log.Record{Value: &redisLogPoint{Point: pointId, Offset: offset}})

	// Store both log and point offsets.
	pointValue := &redisPointValue{PointOffset: offset, LogOffset: logOffset}
	t, err := toml.Marshal(pointValue)
	if err != nil {
		return err
	}

	return rps.redis.Set(pointId, t, 0).Err()
}

func (rps *RedisPointStore) GetPointEvent(pointId string) (uint64, error) {
	t, err := rps.redis.Get(pointId).Result()
	if err != nil {
		return 0, err
	}
	var pointValue redisPointValue
	err = toml.Unmarshal([]byte(t), &pointValue)
	if err != nil {
		return 0, err
	}
	return pointValue.PointOffset, err
}

func NewRedisPointStore(address string, dir string) (*RedisPointStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // use empty password for simplicity. should come from a secret in production
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	log, err := log.NewLog(dir, log.Config{})
	if err != nil {
		return nil, err
	}

	return &RedisPointStore{
		redis: client,
		log:   log,
	}, nil
}
