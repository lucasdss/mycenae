package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/billhathaway/consistentHash"
)

func TestSortNodesN1older(t *testing.T) {

	now := time.Now().Unix()

	n0 := "node0"
	n1 := "node1"

	m := map[string]int64{
		n0: now,
		n1: now - 300,
	}

	node := sortNodes(m)
	if n1 != node {
		t.Fatalf("received: %s expected: %s", node, n1)
	}
}

func TestSortNodesN0older(t *testing.T) {

	now := time.Now().Unix()

	n0 := "node0"
	n1 := "node1"

	m := map[string]int64{
		n0: now - 1,
		n1: now,
	}

	node := sortNodes(m)
	if n0 != node {
		t.Fatalf("received: %s expected: %s", node, n0)
	}
}

func TestSortNodesN0Zerod(t *testing.T) {

	now := time.Now().Unix()

	n0 := "node0"
	n1 := "node1"

	m := map[string]int64{
		n0: 0,
		n1: now - 300,
	}

	node := sortNodes(m)
	if n1 != node {
		t.Fatalf("received: %s expected: %s", node, n1)
	}
}

func TestSortNodesN1Zerod(t *testing.T) {

	now := time.Now().Unix()

	n0 := "node0"
	n1 := "node1"

	m := map[string]int64{
		n0: now,
		n1: 0,
	}

	node := sortNodes(m)
	if n0 != node {
		t.Fatalf("received: %s expected: %s", node, n0)
	}
}

func TestClusterRead1Node(t *testing.T) {

	ch := consistentHash.New()

	nodes := []string{
		"node1",
		"node2",
		"node3",
		"node4",
		"node5",
	}

	for _, id := range nodes {
		ch.Add(id)
	}

	data := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	for i := 0; i < 10; i++ {
		for _, s := range data {
			ns, _ := ch.GetN([]byte(s), 2)

			for _, n := range ns {
				fmt.Printf("|%s -> %s ", s, n)
			}
		}
		fmt.Println()
	}

}
