// Command dlq inspects and replays the dead_letter table of one service database.
//
//	dlq [--db URL] list [--limit N] [--full]
//	dlq [--db URL] show <event_id> [--consumer C]
//	dlq [--db URL] replay <event_id> [--consumer C] [--key K] [--delete] [--yes]
//	dlq [--db URL] purge --older-than 30d [--yes]
//
// The database comes from --db or DATABASE_URL (use 127.0.0.1, not localhost). Replay publishes to
// KAFKA_BROKERS (default 127.0.0.1:9094). Replay and purge change data and ask for confirmation
// unless --yes is given; without a terminal and without --yes they refuse.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/taakht/taakht/libs/goplatform/housekeeping"
)

const usage = `usage:
  dlq [--db URL] list [--limit N] [--full]
  dlq [--db URL] show <event_id> [--consumer C]
  dlq [--db URL] replay <event_id> [--consumer C] [--key K] [--delete] [--yes]
  dlq [--db URL] purge --older-than 30d [--yes]

environment: DATABASE_URL (service database), KAFKA_BROKERS (default 127.0.0.1:9094)`

func main() {
	os.Exit(realMain())
}

func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dlq:", err)
		return 1
	}
	return 0
}

// parse parses flags that may appear before or after positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, out io.Writer) error {
	global := flag.NewFlagSet("dlq", flag.ContinueOnError)
	dbURL := global.String("db", os.Getenv("DATABASE_URL"), "service database URL (default $DATABASE_URL)")
	global.SetOutput(io.Discard)
	if err := global.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	rest := global.Args()
	if len(rest) == 0 {
		return errors.New(usage)
	}
	cmd, rest := rest[0], rest[1:]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", 50, "list: newest N rows (0 = all)")
	full := fs.Bool("full", false, "list: do not truncate errors")
	consumer := fs.String("consumer", "", "restrict to one consumer group")
	key := fs.String("key", "", "replay: Kafka key (default: the envelope's aggregate id)")
	del := fs.Bool("delete", false, "replay: delete the dead_letter row after a successful produce")
	yes := fs.Bool("yes", false, "do not ask for confirmation")
	olderThan := fs.String("older-than", "", "purge: delete rows older than this (e.g. 30d)")
	pos, err := parse(fs, rest)
	if err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	switch cmd {
	case "list", "show", "replay", "purge":
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
	if *dbURL == "" {
		return errors.New("no database: pass --db or set DATABASE_URL")
	}
	conn, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	switch cmd {
	case "list":
		entries, err := List(ctx, conn, *limit)
		if err != nil {
			return err
		}
		printList(out, entries, *full)
		return nil
	case "show":
		if len(pos) != 1 {
			return errors.New("show needs exactly one <event_id>")
		}
		entries, err := Find(ctx, conn, pos[0], *consumer)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return fmt.Errorf("no dead letter with event id %s", pos[0])
		}
		for i, e := range entries {
			if i > 0 {
				fmt.Fprintln(out, "---")
			}
			fmt.Fprint(out, Describe(e))
		}
		return nil
	case "replay":
		return runReplay(ctx, conn, pos, *consumer, *key, *del, *yes, stdin, out)
	default: // purge
		if len(pos) != 0 {
			return errors.New("purge takes no positional arguments")
		}
		if *olderThan == "" {
			return errors.New("purge needs --older-than (for example 30d)")
		}
		d, err := housekeeping.ParseDuration(*olderThan)
		if err != nil {
			return err
		}
		if d < time.Minute {
			return errors.New("--older-than must be at least 1m")
		}
		if err := confirm(fmt.Sprintf("Delete dead letters older than %s", d), *yes, stdin, out); err != nil {
			return err
		}
		n, err := Purge(ctx, conn, d)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "deleted %d dead letter(s)\n", n)
		return nil
	}
}

func runReplay(ctx context.Context, db Querier, pos []string, consumer, key string, del, yes bool, stdin io.Reader, out io.Writer) error {
	if len(pos) != 1 {
		return errors.New("replay needs exactly one <event_id>")
	}
	entries, err := Find(ctx, db, pos[0], consumer)
	if err != nil {
		return err
	}
	switch len(entries) {
	case 0:
		return fmt.Errorf("no dead letter with event id %s", pos[0])
	case 1:
	default:
		return fmt.Errorf("event %s is parked by %d consumers; choose one with --consumer", pos[0], len(entries))
	}
	e := entries[0]
	fmt.Fprint(out, Describe(e))
	prompt := fmt.Sprintf("Replay %s to topic %s (consumer %s will handle it again)", e.EventID, e.Topic, e.Consumer)
	if del {
		prompt += " and delete the dead_letter row"
	}
	if err := confirm(prompt, yes, stdin, out); err != nil {
		return err
	}
	brokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9094"), ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowAutoTopicCreation())
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()
	if err := Replay(ctx, db, kafkaProducer{cl}, e, key, del); err != nil {
		return err
	}
	fmt.Fprintf(out, "replayed %s to %s\n", e.EventID, e.Topic)
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func printList(out io.Writer, entries []Entry, full bool) {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CONSUMER\tEVENT_ID\tTOPIC\tTYPE\tCREATED_AT\tERROR")
	for _, e := range entries {
		msg := strings.Join(strings.Fields(e.Error), " ")
		if !full && len(msg) > 80 {
			msg = msg[:77] + "..."
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", e.Consumer, e.EventID, e.Topic, TypeOf(e.Payload), e.CreatedAt.UTC().Format(time.RFC3339), msg)
	}
	_ = w.Flush()
	fmt.Fprintf(out, "%d dead letter(s)\n", len(entries))
}

// confirm proceeds when yes is set or the user types y on an interactive terminal.
func confirm(prompt string, yes bool, stdin io.Reader, out io.Writer) error {
	if yes {
		return nil
	}
	if f, ok := stdin.(*os.File); ok {
		if fi, err := f.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
			return errors.New("not a terminal: pass --yes to confirm")
		}
	}
	fmt.Fprintf(out, "%s? [y/N] ", prompt)
	line, _ := bufio.NewReader(stdin).ReadString('\n')
	if a := strings.ToLower(strings.TrimSpace(line)); a != "y" && a != "yes" {
		return errors.New("aborted")
	}
	return nil
}

type kafkaProducer struct{ cl *kgo.Client }

func (p kafkaProducer) Produce(ctx context.Context, topic, key string, value []byte) error {
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr()
}
