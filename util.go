package goutube

import (
	"os"
	"path/filepath"

	"github.com/Brijeshlakkad/ring"
)

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// createDirectory creates the directory under parentDir.
func createDirectory(parentDir, dataDir string) (string, error) {
	dir := filepath.Join(parentDir, dataDir)
	// Create a hierarchy of directories if necessary
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

// toRingMemberType gets the member type in ring.MemberType from the rule.
func toRingMemberType(rule ParticipationRule) (ring.MemberType, bool) {
	if rule == LeaderFollowerRule || rule == LeaderRule || rule == StandaloneLeaderRule {
		return ring.ShardMember, true
	} else if rule == LoadBalancerRule {
		return ring.LoadBalancerMember, true
	}
	return 0, false
}

// shouldImplementFollowerResolver if the server should implement the Follower Resolver service.
func shouldImplementFollowerResolver(rule ParticipationRule) bool {
	return rule != FollowerRule && rule != StandaloneLeaderRule
}

// shouldImplementLoci if the server should implement the DistributedLoci.
func shouldImplementLoci(rule ParticipationRule) bool {
	return rule != LoadBalancerRule
}

// shouldImplementReplicationCluster if the server should be the part of a replication cluster.
func shouldImplementReplicationCluster(rule ParticipationRule) bool {
	return rule != LoadBalancerRule
}

// shouldImplementLoadBalancer if the server should implement the load balancer server. If yes, the server will not implement streaming server and vice versa.
func shouldImplementLoadBalancer(rule ParticipationRule) bool {
	return rule == LoadBalancerRule
}
