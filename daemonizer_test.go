package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/MaxDillon/daemonizer/daemon"
)

type MyClient struct {
	GetRecord    func(key string) (*string, error)
	PrintRecords func(out daemon.Writer) error
}

func TestExampleClient(t *testing.T) {
	serviceName := t.Name()

	client := daemon.Client(serviceName, func(ctx context.Context, impl *MyClient) (daemon.CleanupFunc, error) {
		var data = map[string]string{"foo": "bar"}

		impl.GetRecord = func(key string) (*string, error) {
			if s, ok := data[key]; ok {
				return &s, nil
			}
			return nil, nil
		}

		impl.PrintRecords = func(out daemon.Writer) error {
			for key, value := range data {
				fmt.Fprintf(out, "%s: %s\n", key, value)
			}
			return nil
		}

		return func() {
			// cleanup resources
		}, nil
	})

	err := daemon.Start(client, &daemon.StartupOptions{})
	defer daemon.Stop(client)
	if err != nil {
		t.Error(err)
	}

	if daemon.IsRunning(client) == false {
		t.Error("Daemon isnt running")
	}

	if record, err := client.GetRecord("foo"); err != nil {
		t.Error(err)
	} else if record == nil {
		t.Error("record is nil")
	} else if *record != "bar" {
		t.Errorf("record is incorect: %s", *record)
	}

	var buf bytes.Buffer
	err = client.PrintRecords(daemon.Wrap(&buf))
	if err != nil {
		t.Error(err)
	}

	if buf.String() != "foo: bar\n" {
		t.Error("incorrect print")
	}

}
