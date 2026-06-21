package daemonizer_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/f4vzvy99f7-sys/daemonizer"
)

type MyClient struct {
	GetRecord    func(key string) (*string, error)
	PrintRecords func(out daemonizer.Writer) error
}

func TestExampleClient(t *testing.T) {
	serviceName := t.Name()

	daemon := daemonizer.Client(serviceName, func(ctx context.Context, impl *MyClient, _ any) (daemonizer.CleanupFunc, error) {
		var data = map[string]string{"foo": "bar"}

		impl.GetRecord = func(key string) (*string, error) {
			if s, ok := data[key]; ok {
				return &s, nil
			}
			return nil, nil
		}

		impl.PrintRecords = func(out daemonizer.Writer) error {
			for key, value := range data {
				fmt.Fprintf(out, "%s: %s\n", key, value)
			}
			return nil
		}

		return func() {
			// cleanup resources
		}, nil
	})

	err := daemon.Start(map[string]string{}, nil)
	defer daemon.Stop()
	if err != nil {
		t.Error(err)
	}

	if daemon.IsRunning() == false {
		t.Error("Daemon isnt running")
	}

	if record, err := daemon.Client.GetRecord("foo"); err != nil {
		t.Error(err)
	} else if record == nil {
		t.Error("record is nil")
	} else if *record != "bar" {
		t.Errorf("record is incorect: %s", *record)
	}

	var buf bytes.Buffer
	err = daemon.Client.PrintRecords(daemonizer.Wrap(&buf))
	if err != nil {
		t.Error(err)
	}

	if buf.String() != "foo: bar\n" {
		t.Error("incorrect print")
	}

}
