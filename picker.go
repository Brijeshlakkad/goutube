package goutube

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*Picker)(nil)

type Picker struct {
	mu            sync.RWMutex
	loadbalancers []balancer.SubConn
	current       uint64
}

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()
	var followers []balancer.SubConn
	for sc, _ := range buildInfo.ReadySCs {
		followers = append(followers, sc)
	}
	p.loadbalancers = followers
	return p
}

func (p *Picker) Pick(info balancer.PickInfo) (
	balancer.PickResult,
	error,
) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result balancer.PickResult
	result.SubConn = p.nextLoadBalancer()
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextLoadBalancer() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	followerCount := uint64(len(p.loadbalancers))
	if followerCount == 0 {
		return nil
	}
	idx := int(cur % followerCount)
	return p.loadbalancers[idx]
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(TestName, &Picker{}, base.Config{}),
	)
}
