package fabric

// PrivateNetworkPolicy is guidance, never evidence that the owner's network
// currently enforces it. Snippet contains the selected adapter's policy format.
type PrivateNetworkPolicy struct {
	Requirements []string `json:"requirements"`
	Snippet      string   `json:"snippet"`
	Notes        []string `json:"notes"`
}

// RequiredPrivateNetworkAccess describes Wefty's policy without provider syntax.
func RequiredPrivateNetworkAccess() []string {
	return []string{
		"Clients and node agents may reach L1 and L3.",
		"L1 and L3 may reach each other.",
		"The owner's own untagged identity may reach L1 for person commands and Computer front doors and published application ports on node agents.",
		"Client participants may reach the fixed Seat front door on node port 80; other machines' published application ports remain forbidden to machine participants.",
		"Agent-tag ownership must remain restricted to trusted administrative roles.",
	}
}
