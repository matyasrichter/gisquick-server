package cache

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"
)

type record[T any] struct {
	Val       T
	Timestamp int64
}

type JSONFileReader[V any] struct {
	sync.RWMutex
	cache      *ttlcache.Cache[string, record[V]]
	loaderLock *singleflight.Group
}

func NewJSONFileReader[V any](ttl time.Duration) *JSONFileReader[V] {
	loaderLock := &singleflight.Group{}
	cache := ttlcache.New(ttlcache.WithTTL[string, record[V]](ttl))
	go cache.Start()
	return &JSONFileReader[V]{cache: cache, loaderLock: loaderLock}
}

func (r *JSONFileReader[V]) load(filename string, timestamp int64) (V, error) {
	res, err, _ := r.loaderLock.Do(filename, func() (any, error) {
		content, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		var data V
		err = json.Unmarshal(content, &data)
		if err != nil {
			return nil, err
		}
		rec := record[V]{Val: data, Timestamp: timestamp}
		r.cache.Set(filename, rec, ttlcache.DefaultTTL)
		return data, nil
	})
	var v V
	if err != nil {
		return v, err
	}
	v = res.(V)
	return v, nil
}

func (r *JSONFileReader[V]) Get(filename string) (V, error) {
	fStat, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.cache.Delete(filename)
		}
		var v V
		return v, err
	}
	updated := fStat.ModTime()
	timestamp := updated.Unix()

	item := r.cache.Get(filename)
	if item == nil {
		res, err, _ := r.loaderLock.Do(filename, func() (any, error) {
			content, err := os.ReadFile(filename)
			if err != nil {
				return nil, err
			}
			var data V
			err = json.Unmarshal(content, &data)
			if err != nil {
				return nil, err
			}
			rec := record[V]{Val: data, Timestamp: timestamp}
			r.cache.Set(filename, rec, ttlcache.DefaultTTL)
			return data, nil
		})
		var v V
		if err != nil {
			return v, err
		}
		v = res.(V)
		return v, nil
	}
	rec := item.Value()
	if rec.Timestamp != timestamp {
		return r.load(filename, timestamp)
	}
	return rec.Val, nil
}

func (r *JSONFileReader[V]) Extend(filename string, data V) (V, error) {
	fStat, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.cache.Delete(filename)
		}
		return data, err
	}
	updated := fStat.ModTime()
	timestamp := updated.Unix()

	item := r.cache.Get(filename)
	if item == nil {
		res, err, _ := r.loaderLock.Do(filename, func() (any, error) {
			content, err := os.ReadFile(filename)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(content, &data)
			if err != nil {
				return nil, err
			}
			rec := record[V]{Val: data, Timestamp: timestamp}
			r.cache.Set(filename, rec, ttlcache.DefaultTTL)
			return data, nil
		})
		if err != nil {
			return data, err
		}
		v := res.(V)
		return v, nil
	}
	rec := item.Value()
	if rec.Timestamp != timestamp {
		return r.load(filename, timestamp)
	}
	return rec.Val, nil
}

func (r *JSONFileReader[V]) Close() {
	r.cache.Stop()
	r.cache.DeleteAll()
}
