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
		"The owner's own untagged identity may reach L1 for person commands; only that identity may reach Computer front doors and published ports on node agents.",
		"Machine participants must not reach other machines' published ports.",
		"Agent-tag ownership must remain restricted to trusted administrative roles.",
	}
}
