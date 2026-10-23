package ocihelper

import "sync"

const seatCommandPerSeatLimit = 2
const seatCommandPerAttemptLimit = 4
const seatCommandGlobalLimit = 16
const DetailSeatCommandLimit ErrorDetail = "seat_command_limit"
const DetailAttemptCommandLimit ErrorDetail = "attempt_command_limit"
const DetailCommandStreamLimit ErrorDetail = "command_stream_limit"

type seatCommandBudgetKey struct {
	authority AttemptAuthority
	place     int
}
type seatCommandBudget struct {
	mu       sync.Mutex
	seats    map[seatCommandBudgetKey]int
	attempts map[AttemptAuthority]int
	total    int
}

func (server *Server) admitSeatCommand(request ExecComputerSeatRequest) (func(), *RPCError) {
	budget := &server.seatCommands
	budget.mu.Lock()
	defer budget.mu.Unlock()
	key := seatCommandBudgetKey{request.Authority, request.Place}
	detail := ErrorDetail("")
	switch {
	case budget.seats[key] >= seatCommandPerSeatLimit:
		detail = DetailSeatCommandLimit
	case budget.attempts[request.Authority] >= seatCommandPerAttemptLimit:
		detail = DetailAttemptCommandLimit
	case budget.total >= min(seatCommandGlobalLimit, cap(server.dataStreams)/2):
		detail = DetailCommandStreamLimit
	}
	if detail != "" {
		return nil, &RPCError{Code: CodeConnectionLimit, Detail: detail, Message: "Seat command stream limit reached; retry after a command ends"}
	}
	if budget.seats == nil {
		budget.seats = map[seatCommandBudgetKey]int{}
		budget.attempts = map[AttemptAuthority]int{}
	}
	budget.seats[key]++
	budget.attempts[request.Authority]++
	budget.total++
	var once sync.Once
	return func() {
		once.Do(func() {
			budget.mu.Lock()
			defer budget.mu.Unlock()
			budget.seats[key]--
			if budget.seats[key] == 0 {
				delete(budget.seats, key)
			}
			budget.attempts[request.Authority]--
			if budget.attempts[request.Authority] == 0 {
				delete(budget.attempts, request.Authority)
			}
			budget.total--
		})
	}, nil
}
