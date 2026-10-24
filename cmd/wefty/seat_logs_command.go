package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func loadSeatCredential(id string) (string, error) {
	roots, err := seatCredentialRoots()
	if err != nil {
		return "", err
	}
	for _, directory := range roots {
		root, e := openSeatCredentialDirectory(directory, false)
		if e != nil {
			if os.IsNotExist(e) || seatStorageDenied(e) {
				continue
			}
			return "", e
		}
		data, e := root.ReadFile(id + ".json")
		root.Close()
		if e != nil {
			if os.IsNotExist(e) {
				continue
			}
			return "", e
		}
		var value struct {
			SeatID     string `json:"seat_id"`
			Credential string `json:"credential"`
		}
		if err := json.Unmarshal(data, &value); err != nil {
			return "", err
		}
		if value.SeatID != id || value.Credential == "" {
			return "", fmt.Errorf("invalid Seat credential file")
		}
		return value.Credential, nil
	}
	return "", nil // The original caller can read its log before a Seat is live.
}

func executeSeatLogs(ctx context.Context, clients *apiClients, jsonOutput bool, id, cursor string, limit, tail int, tailSet, follow, all bool, interval time.Duration, stdout, stderr io.Writer) error {
	if !seatCLIIdentifier.MatchString(id) {
		return usageError("invalid Seat ID")
	}
	secret, err := loadSeatCredential(id)
	if err != nil {
		return err
	}
	headers := http.Header{}
	if secret != "" {
		headers.Set(l1.SeatCredentialHeader, secret)
	}
	path := "/v1/seats/" + id + "/logs"
	walking := all || tailSet
	if walking || follow {
		ctx = withoutClientRetry(ctx)
	}
	var buffered []l1.SeatLogEvent
	next := 0
	writeEvents := func(events []l1.SeatLogEvent) error {
		for _, event := range events {
			if jsonOutput {
				if err := writeJSONLine(stdout, event); err != nil {
					return err
				}
				continue
			}
			suffix := ""
			if event.Reason != "" {
				suffix = ": " + event.Reason
			}
			if event.Note != "" {
				suffix += ": " + event.Note
			}
			if event.RunID != "" {
				suffix += " Run " + event.RunID
			}
			if event.ExitCode != nil {
				suffix += fmt.Sprintf(" exit %d", *event.ExitCode)
			}
			if event.Path != "" {
				suffix += " " + event.Path
			}
			if event.SizeBytes != nil {
				suffix += fmt.Sprintf(" (%d bytes)", *event.SizeBytes)
			}
			if event.Audit != nil && event.Audit.Reason != "" {
				suffix += ": " + string(event.Audit.Reason)
			}
			if _, err := fmt.Fprintf(stdout, "%s %s%s\n", event.OccurredAt.Format(time.RFC3339), event.Kind, suffix); err != nil {
				return err
			}
		}
		return nil
	}
	pages, retries := 0, 0
	for {
		query := url.Values{"limit": {strconv.Itoa(limit)}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page l1.SeatLogPage
		err := clients.l1.do(ctx, http.MethodGet, path+"?"+query.Encode(), nil, headers, &page, http.StatusOK)
		if err != nil {
			if (follow && isRetryableObservation(err)) || (walking && isRetryableL1Answer(err)) {
				retries++
				if !follow && retries > logWalkRetryLimit {
					return err
				}
				if sleepFollowRetry(ctx, interval) {
					continue
				}
			}
			if follow && ctx.Err() != nil {
				return fmt.Errorf("stopped following Seat %s", id)
			}
			return err
		}
		retries = 0
		if page.SeatID != id {
			return fmt.Errorf("Seat log response changed its ID")
		}
		if jsonOutput && !follow && !walking {
			if err := writeJSON(stdout, page); err != nil {
				return err
			}
		} else if walking && tailSet {
			for _, event := range page.Events {
				if len(buffered) < tail {
					buffered = append(buffered, event)
				} else {
					buffered[next] = event
					next = (next + 1) % tail
				}
			}
		} else if err := writeEvents(page.Events); err != nil {
			return err
		}
		cursor = page.NextCursor
		// Acknowledge only delivered pages. Tail acknowledges the walked history
		// once its selected suffix has been printed successfully.
		acknowledge := func() error {
			if cursor == "" {
				return nil
			}
			return clients.l1.do(ctx, http.MethodPost, path+"/read", l1.SeatLogRead{Cursor: cursor}, headers, nil, http.StatusOK)
		}
		if !tailSet || !walking {
			if err := acknowledge(); err != nil {
				return err
			}
		}
		if walking {
			if len(page.Events) > 0 && cursor != "" {
				pages++
				if pages >= logWalkPageCap {
					return fmt.Errorf("Seat log walk stopped after %d pages; resume with --cursor %s", logWalkPageCap, cursor)
				}
				continue
			}
			if tailSet {
				if err := writeEvents(buffered[next:]); err != nil {
					return err
				}
				if err := writeEvents(buffered[:next]); err != nil {
					return err
				}
				if err := acknowledge(); err != nil {
					return err
				}
				buffered = nil
			}
			walking = false
			if !follow {
				return nil
			}
		} else if !follow {
			if !jsonOutput && len(page.Events) > 0 && cursor != "" {
				_, err := fmt.Fprintf(stderr, "wefty: resume with --cursor %s, use --tail 50 for the end, or --all for every retained event\n", cursor)
				return err
			}
			return nil
		}
		if page.Ended && len(page.Events) == 0 {
			// The final line always explains the end, including a follow resumed at
			// the terminal cursor or with a tail that omitted the original end event.
			if jsonOutput {
				return writeJSONLine(stdout, map[string]any{"seat_id": id, "kind": "ended", "reason": page.Reason})
			}
			_, err := fmt.Fprintf(stdout, "Seat %s ended: %s\n", id, page.Reason)
			return err
		}
		wait := clients.wait
		if wait == nil {
			wait = waitForContext
		}
		if err := wait(ctx, interval); err != nil {
			return fmt.Errorf("stopped following Seat %s: %w", id, err)
		}
	}
}

// withSeatReply is the one reply path shared by Seat commands. Callers pass
// the count from their L1 read snapshot; formatting never changes read state.
func withSeatReply(stdout io.Writer, jsonOutput bool, id string, unread int, reply func(io.Writer) error) error {
	var staged bytes.Buffer
	output := stdout
	if jsonOutput && unread > 0 {
		output = &staged
	}
	err := reply(output)
	if unread == 0 {
		return err
	}
	noun := "events"
	if unread == 1 {
		noun = "event"
	}
	line := fmt.Sprintf("%d new Seat %s: wefty logs %s", unread, noun, id)
	if err != nil {
		var api *apiResponseError
		if errors.As(err, &api) {
			copy := *api
			copy.APIError.Message += "; " + line
			return &copy
		}
		return fmt.Errorf("%w; %s", err, line)
	}
	if jsonOutput {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(staged.Bytes(), &value); err != nil {
			return err
		}
		value["unread_events_line"], err = json.Marshal(line)
		if err != nil {
			return err
		}
		return writeJSON(stdout, value)
	}
	_, err = fmt.Fprintln(stdout, line)
	return err
}
