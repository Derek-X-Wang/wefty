package contract

import "time"

const SeatFrontDoorPort = 80
const SeatFrontDoorPortReserved = "published port 80 is reserved for wefty's Seat front door; choose another port"

// Copy leases use L1's boot-relative clock. Renewal runs independently of I/O.
const SeatCopyHoldLease = 60 * time.Second
const SeatCopyHoldRenewal = 20 * time.Second
