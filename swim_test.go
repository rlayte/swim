package swim

import (
	"testing"
	"time"
)

func checkMembers(t *testing.T, node *Node, cluster []*Node) {
	for _, member := range cluster {
		if member.Host == node.Host {
			continue
		} else if !node.Members[member.Host] {
			t.Error("Missing member", node.Host, member)
		}
	}
}

func checkCluster(t *testing.T, cluster []*Node) {
	for _, node := range cluster {
		checkMembers(t, node, cluster)
	}
}

func checkFailures(t *testing.T, cluster []*Node, failed []int) {
}

func createCluster(size int) []*Node {
	cluster := []*Node{NewNode(0)}

	for i := 1; i < size; i++ {
		n := NewNode(i)
		n.join(cluster[0].Host)
		cluster = append(cluster, n)
	}

	return cluster
}

func TestNoFailures(t *testing.T) {
	cluster := createCluster(10)

	time.Sleep(T * 10)

	checkCluster(t, cluster)
}
