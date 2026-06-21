package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/MaxDillon/daemonizer/daemon"
)

type MathClient struct {
	Add   func(a, b int) (int, error)
	Greet func(name string) (string, error)
	Inc   func() (int, error)
}

var client = daemon.Client[MathClient]("my-service", func(ctx context.Context, impl *MathClient) (daemon.CleanupFunc, error) {
	counter := 0

	impl.Add = func(a, b int) (int, error) {
		daemon.Logger().Printf("Adding %d and %d", a, b)
		return a + b, nil
	}
	impl.Greet = func(name string) (string, error) {
		daemon.Logger().Printf("Greeting %s", name)
		return fmt.Sprintf("Hello, %s!", name), nil
	}
	impl.Inc = func() (int, error) {
		counter++
		return counter, nil
	}

	return func() {
		daemon.Logger().Printf("shutting down, final counter: %d", counter)
	}, nil
})

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <start|stop|restart|add|greet> [args...]\n", os.Args[0])
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "start":
		if err := daemon.Start(client, nil); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("daemon started")
		return

	case "stop":
		if err := daemon.Stop(client); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("daemon stopped")
		return

	case "restart":
		if err := daemon.Stop(client); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		if err := daemon.Start(client, nil); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("daemon restarted")
		return
	}

	if !daemon.IsRunning(client) {
		fmt.Fprintf(os.Stderr, "daemon is not running — use '%s start' to start it\n", os.Args[0])
		os.Exit(1)
	}

	switch cmd {
	case "add":
		if len(os.Args) != 4 {
			fmt.Fprintf(os.Stderr, "usage: %s add <a> <b>\n", os.Args[0])
			os.Exit(1)
		}
		a, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid argument %q: %v\n", os.Args[2], err)
			os.Exit(1)
		}
		b, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid argument %q: %v\n", os.Args[3], err)
			os.Exit(1)
		}
		result, err := client.Add(a, b)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(result)

	case "greet":
		if len(os.Args) != 3 {
			fmt.Fprintf(os.Stderr, "usage: %s greet <name>\n", os.Args[0])
			os.Exit(1)
		}
		msg, err := client.Greet(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(msg)

	case "inc":
		count, err := client.Inc()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(count)

	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		fmt.Fprintf(os.Stderr, "usage: %s <start|stop|restart|add|greet> [args...]\n", os.Args[0])
		os.Exit(1)
	}
}
