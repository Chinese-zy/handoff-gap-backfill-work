package cmd

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/kuper-tech/protokaf/internal/gap"
	"github.com/spf13/cobra"
)

const defaultRelayAddr = ":9101"

// NewRelayCmd exposes the gap-backfill stream over a fixed TCP port.
// The server side accepts fixed 32-byte payloads from stdin and retains
// unconfirmed entries; the client side reconnects to the same address
// and resumes from its last confirmed sequence.
func NewRelayCmd() *cobra.Command {
	var addr string

	cmd := &cobra.Command{
		Use:   "relay [serve|tail]",
		Short: "Ordered byte stream with reconnect gap backfill",
	}

	serve := &cobra.Command{
		Use:   "serve",
		Short: "Listen and retain unconfirmed records",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			srv, err := gap.Listen(ctx, addr, 1024)
			if err != nil {
				return fmt.Errorf("listen %s: %w", addr, err)
			}
			defer srv.Close()
			log.Infof("relay serving on %s", srv.Addr())

			reader := bufio.NewReader(cmd.InOrStdin())
			buf := make([]byte, gap.PayloadSize)
			for {
				if _, err := readFull(reader, buf); err != nil {
					return nil // stdin closed
				}
				if _, err := srv.Publish(ctx, buf); err != nil {
					return err
				}
			}
		},
	}

	tail := &cobra.Command{
		Use:   "tail",
		Short: "Connect, resume from the last confirmation on reconnect",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			client := &gapClient{addr: addr, out: cmd.OutOrStdout()}
			return client.run(ctx)
		},
	}

	cmd.PersistentFlags().StringVar(&addr, "addr", defaultRelayAddr, "TCP intake address")
	cmd.AddCommand(serve, tail)
	return cmd
}

type gapClient struct {
	addr string
	out  interface{ Write([]byte) (int, error) }
}

func (c *gapClient) run(ctx context.Context) error {
	var client *gap.Client
	handler := func(conn net.Conn, e *gap.Entry) error {
		if _, err := c.out.Write(e.Payload[:]); err != nil {
			return err
		}
		return client.Ack(conn, e.Seq)
	}
	var err error
	client, err = gap.Dial(ctx, c.addr, handler)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		if err := client.Resume(ctx); err != nil {
			return err
		}
	}
	return nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
