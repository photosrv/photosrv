// Package storage handles loading a photosrv storage config
package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/dchest/siphash"
	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/weighted"
	"github.com/dgryski/go-simpleconf"
)

// Shard is a photostorage shard
type Shard struct {
	Name      string
	index     int
	Weight    int
	Endpoints []string
}

type shardList []Shard

func (s shardList) Len() int           { return len(s) }
func (s shardList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s shardList) Less(i, j int) bool { return s[i].index < s[j].index }

// LoadShards extracts a chooser and list of shards from the provided config
func LoadShards(conf simpleconf.Config, configBase string, dc string) (chooser shardedkv.Chooser, shards []Shard, err error) {

	photoShards := make(map[string]string)
	simpleconf.UnmarshalConfig(conf, configBase+dc+">shards", &photoShards)

	weights := make(map[string]int)

	for name, endpoints := range photoShards {

		shard := Shard{Name: name}

		if idx := strings.Index(name, ","); idx != -1 {
			weight := name[idx+1:]
			name = name[:idx]
			shard.Name = name
			n, err := strconv.Atoi(weight)
			if err != nil {
				return nil, nil, fmt.Errorf("bad weight %q for shard %s>%s", weight, configBase, name)
			}
			shard.Weight = n
		}

		if !strings.HasPrefix(name, "shard") {
			return nil, nil, fmt.Errorf("shard name %q must begin with `shard`", name)
		}

		sidx := strings.TrimPrefix(name, "shard")
		var err error
		shard.index, err = strconv.Atoi(sidx)
		if err != nil {
			return nil, nil, fmt.Errorf("bad shard index `%s`", sidx)
		}

		for _, endpoint := range strings.Fields(endpoints) {
			shard.Endpoints = append(shard.Endpoints, endpoint)
		}

		shards = append(shards, shard)
	}

	sort.Sort(shardList(shards))

	var chooserType string
	simpleconf.UnmarshalConfig(conf, configBase+"chooser", &chooserType)
	var ch shardedkv.Chooser
	switch chooserType {
	case "chash", "":
		// use chash if no chooser specified
		ch = chash.New()
	case "jump":
		ch = jump.New(func(b []byte) uint64 { return siphash.Hash(0, 0, b) })
	default:
		return nil, nil, fmt.Errorf("bad chooser type %q", chooserType)
	}

	if len(weights) != 0 {

		if len(weights) != len(shards) {
			return nil, nil, fmt.Errorf("some shards missing weights: len(weights)=%d len(shards)=%d", len(weights), len(shards))
		}

		ch = weighted.New(ch, func(b string) int { return weights[b] })
	}

	var keys []string
	for _, s := range shards {
		keys = append(keys, s.Name)
	}
	ch.SetBuckets(keys)

	return ch, shards, nil
}
