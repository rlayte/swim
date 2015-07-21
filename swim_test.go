package swim

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func checkMembers(t *testing.T, node *Node, cluster []*Node) {
	for _, member := range cluster {
		if member.Host == node.Host {
			continue
		} else if !node.Members[member.Host] {
			t.Error("Missing member", node.Host, member.Host)
		}
	}
}

func checkCluster(t *testing.T, cluster []*Node) {
	for _, node := range cluster {
		checkMembers(t, node, cluster)
	}
}

func checkFailures(t *testing.T, cluster []*Node, failed map[string]bool) {
	for _, node := range cluster {
		if node.dead {
			continue
		}

		for host, _ := range node.Members {
			if failed[host] {
				t.Error("Node has failed", node.Host, host, node.Members)
			}
		}
	}
}

func createCluster(name string, size int) []*Node {
	cluster := []*Node{NewNode(name, 0)}

	for i := 1; i < size; i++ {
		n := NewNode(name, i)
		n.join(cluster[0].Host)
		cluster = append(cluster, n)
	}

	return cluster
}

func destroyCluster(cluster []*Node) {
	for _, node := range cluster {
		node.stopRPC()
	}
}

func partition(cluster []*Node, name string, a int, b int) {
	cluster[a].partitioned[fmt.Sprintf("%s-%d", name, b)] = true
	cluster[b].partitioned[fmt.Sprintf("%s-%d", name, a)] = true
}

func TestSwim(t *testing.T) {
	cluster := createCluster("nofails", 10)
	defer destroyCluster(cluster)

	log.Println("Test no failures")
	time.Sleep(T * 10)

	checkCluster(t, cluster)
	log.Println("Passed")
	log.Println("Test failed nodes are removed")

	cluster[3].dead = true
	cluster[7].dead = true

	time.Sleep(T * 10)

	checkFailures(t, cluster, map[string]bool{"basic-3": true, "basic-7": true})

	log.Println("Passed")
	log.Println("Test cluster is healed")

	cluster[3].dead = false
	cluster[7].dead = false

	time.Sleep(T * 10)

	checkCluster(t, cluster)

	log.Println("Passed")
	log.Println("Test partitions")

	partition(cluster, "partitions", 2, 5)
	partition(cluster, "partitions", 2, 6)
	partition(cluster, "partitions", 2, 7)
	partition(cluster, "partitions", 3, 5)
	partition(cluster, "partitions", 3, 6)
	partition(cluster, "partitions", 3, 7)

	time.Sleep(T * 10)

	checkCluster(t, cluster)
	log.Println("Passed")
}
