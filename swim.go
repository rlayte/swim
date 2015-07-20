package swim

import (
	"fmt"
	"log"
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
	Host    string
	Members map[string]bool
}

type PingReply struct {
	Ack bool
}

func (n *Node) Ping(args *PingArgs, reply *PingReply) error {
	n.Members[args.Host] = true

	for member, _ := range args.Members {
		if member != n.Host {
			n.Members[member] = true
		}
	}

	return nil
}

func (n *Node) PingReq(args *PingArgs, reply *PingReply) error {
	return nil
}

func (n *Node) heartbeat() {
	for {
		for host, _ := range n.Members {
			args := PingArgs{n.Host, n.Members}
			reply := PingReply{}
			n.call(host, "Node.Ping", &args, &reply)

			if reply.Ack {
				n.Members[host] = true
			}
		}

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
