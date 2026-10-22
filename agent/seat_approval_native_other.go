//go:build !darwin

package agent

import "context"

type NativeSeatApprovalDialog struct{}

func (NativeSeatApprovalDialog) Show(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
	return SeatApprovalUnavailable, nil
}
