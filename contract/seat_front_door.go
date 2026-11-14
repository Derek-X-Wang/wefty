package contract

import "time"

const SeatFrontDoorPort = 80
const SeatFrontDoorPortReserved = "published port 80 is reserved for wefty's Seat front door; choose another port"

// Copy leases use L1's boot-relative clock. Renewal runs independently of I/O.
const SeatCopyHoldLease = 60 * time.Second
const SeatCopyHoldRenewal = 20 * time.Second

// SeatInputPersonDrivingClose is the WebSocket close code a node sends when a
// person's take ends an AI agent input stream that is already open. Its close
// reason is "person_driving"; the CLI reports it as person_driving (#1680).
const SeatInputPersonDrivingClose = 4409
