package swim

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"time"
)

const (
	T       time.Duration = time.Second
	Timeout time.Duration = T / 4
	k       int           = 3
)

type Node struct {
	Members map[string]bool
	Host    string
}

type PingArgs struct {
	Host string
}

type PingReply struct {
	Ack bool
}

func (n *Node) Join(args *PingArgs, reply *PingReply) error {
	if !n.Members[args.Host] {
		n.Members[args.Host] = true
	}

	reply.Ack = true
	return nil
}

func (n *Node) Ping(args *PingArgs, reply *PingReply) error {
	if !n.Members[args.Host] {
		go n.broadcast("Node.Join", PingArgs{args.Host}, PingReply{})
		n.Members[args.Host] = true
	}

	reply.Ack = true
	return nil
}

func (n *Node) PingReq(args *PingArgs, reply *PingReply) error {
	return nil
}

func (n *Node) pickMember() string {
	if len(n.Members) == 0 {
		return ""
	}

	index := rand.Intn(len(n.Members))
	count := 0

	for host, _ := range n.Members {
		if count == index {
			return host
		}

		count++
	}

	return ""
}

func (n *Node) broadcast(method string, args PingArgs, reply PingReply) {
	for member, _ := range n.Members {
		n.call(member, method, &args, &reply)
	}
}

func (n *Node) pingMember(host string) {
	args := PingArgs{n.Host}
	reply := PingReply{}
	n.call(host, "Node.Ping", &args, &reply)
}

func (n *Node) heartbeat() {
	for {
		host := n.pickMember()
		go n.pingMember(host)
		time.Sleep(T)
	}
}

func (n *Node) join(seed string) {
	n.Members[seed] = true
}

func (n *Node) call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	for errx != nil {
		c, errx = rpc.Dial("unix", srv)
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	for err != nil {
		err = c.Call(rpcname, args, reply)
	}

	return err == nil
}

func (n *Node) startRPC() {
	rpcs := rpc.NewServer()
	rpcs.Register(n)

	os.Remove(n.Host)
	l, e := net.Listen("unix", n.Host)
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Println("Error", err)
		}

		go rpcs.ServeConn(conn)
	}
}

func NewNode(id int) *Node {
	n := &Node{
		Host:    fmt.Sprintf("gossip-%d", id),
		Members: map[string]bool{},
	}

	go n.heartbeat()
	go n.startRPC()

	return n
}
