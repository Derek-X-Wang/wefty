package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Derek-X-Wang/wefty/l1"
)

type fleetListOptions struct {
	Cursor string
	Limit  int
	All    bool
	Query  url.Values
}

func parseFleetListOptions(kind string, args []string) (fleetListOptions, error) {
	flags := flag.NewFlagSet(kind+" list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	options := fleetListOptions{Query: url.Values{}}
	flags.StringVar(&options.Cursor, "cursor", "", "opaque cursor from the previous page")
	flags.IntVar(&options.Limit, "limit", l1.DefaultJobPageLimit, "items per page")
	flags.BoolVar(&options.All, "all", false, "walk all remaining pages")
	var state, capability, claims string
	if kind == "nodes" {
		flags.StringVar(&state, "state", "", "exact node state")
		flags.StringVar(&capability, "capability", "", "exact advertised capability key whose value is true")
		flags.StringVar(&claims, "claims-enabled", "", "exact claims intent: true or false")
	}
	if err := flags.Parse(args); err != nil {
		return options, usageError(err.Error())
	}
	if flags.NArg() != 0 {
		return options, usageError(kind + " list does not accept positional arguments")
	}
	if options.Limit < 1 || options.Limit > l1.MaxJobPageLimit {
		return options, usageError(fmt.Sprintf("--limit must be between 1 and %d", l1.MaxJobPageLimit))
	}
	var invalid string
	flags.Visit(func(f *flag.Flag) {
		if (f.Name == "state" || f.Name == "capability" || f.Name == "claims-enabled") && f.Value.String() == "" {
			invalid = f.Name
		}
	})
	if invalid != "" {
		return options, usageError("--" + invalid + " must not be empty")
	}
	if claims != "" && claims != "true" && claims != "false" {
		return options, usageError("--claims-enabled must be true or false")
	}
	for key, value := range map[string]string{"state": state, "capability": capability, "claims_enabled": claims} {
		if value != "" {
			options.Query.Set(key, value)
		}
	}
	return options, nil
}

func executeNodesList(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	options, err := parseFleetListOptions("nodes", args)
	if err != nil {
		return err
	}
	result := l1.NodeList{Nodes: []l1.Node{}}
	cursor := options.Cursor
	for {
		query := options.Query
		query.Set("limit", strconv.Itoa(options.Limit))
		query.Set("cursor", cursor)
		var page l1.NodeList
		if err := clients.l1.do(ctx, http.MethodGet, "/v1/nodes?"+query.Encode(), nil, nil, &page, http.StatusOK); err != nil {
			return err
		}
		result.Nodes = append(result.Nodes, page.Nodes...)
		result.NextCursor = page.NextCursor
		if !options.All || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if jsonOutput {
		return writeJSON(stdout, result)
	}
	if err := writeNodesTable(stdout, result.Nodes); err != nil {
		return err
	}
	return writeFleetCursor(stdout, result.NextCursor)
}

func executeComputers(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] != "list" {
		return usageError("usage: wefty computers list [--cursor CURSOR] [--limit N] [--all]")
	}
	options, err := parseFleetListOptions("computers", args[1:])
	if err != nil {
		return err
	}
	result := l1.ComputerList{Computers: []l1.Computer{}}
	cursor := options.Cursor
	for {
		page, err := clients.listComputers(ctx, cursor, options.Limit)
		if err != nil {
			return err
		}
		result.Computers = append(result.Computers, page.Computers...)
		result.NextCursor = page.NextCursor
		if !options.All || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if jsonOutput {
		return writeJSON(stdout, result)
	}
	projections := make([]computerOperatorProjection, 0, len(result.Computers))
	for _, computer := range result.Computers {
		projections = append(projections, newComputerProjection(computer, nil, nil))
	}
	if err := writeComputersTable(stdout, projections); err != nil {
		return err
	}
	return writeFleetCursor(stdout, result.NextCursor)
}

func writeFleetCursor(writer io.Writer, cursor string) error {
	if cursor == "" {
		return nil
	}
	_, err := fmt.Fprintf(writer, "NEXT CURSOR\t%s\n", cursor)
	return err
}
