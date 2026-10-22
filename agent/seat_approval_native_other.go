//go:build !darwin

package agent

import (
	"context"
	"fmt"
)

type NativeSeatApprovalDialog struct{}

func (NativeSeatApprovalDialog) Show(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
	return SeatApprovalUnavailable, fmt.Errorf("native Seat approval dialog requires macOS")
}
