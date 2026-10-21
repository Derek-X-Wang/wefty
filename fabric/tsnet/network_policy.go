package tsnet

import "github.com/Derek-X-Wang/wefty/fabric"

// PrivateNetworkPolicy uses the policy format documented at
// https://tailscale.com/docs/reference/syntax/policy-file and its role selectors
// https://tailscale.com/docs/reference/targets-and-selectors . Tagged machine
// identities do not inherit their enrolling user's untagged owner identity.
// All provider syntax stays here, including autogroups and tag ownership.
func PrivateNetworkPolicy() fabric.PrivateNetworkPolicy {
	return fabric.PrivateNetworkPolicy{
		Requirements: fabric.RequiredPrivateNetworkAccess(),
		Notes: []string{
			"Review the complete private network policy before applying this snippet. Existing overlapping allow rules, including a default allow-all rule, can defeat published-port isolation; this snippet cannot revoke another allow rule.",
			"The owner selector means the network's Owner role on untagged devices. Tagged machine participants do not acquire that identity. Owners, Admins and Network admins implicitly own all tags; tagOwners alone does not limit tagging to admins. Keep all of these privileged roles restricted to trusted administrators.",
			"Any device signed in to the Owner account can act as the fleet admin until L1 pins the person device. Do not sign the control-plane Mac's network app, or any machine where AI agents run as you, into the Owner account.",
			"The snippet permits only TCP port 80 for control traffic. Computer front doors and published ports on node agents are reachable only by the owner's untagged identity. Attended peer tests must confirm both allowed and refused connections.",
			"Doctor prints guidance and observed evidence; it never reads or stores an admin credential and never changes the network policy.",
		},
		Snippet: `{
  "tagOwners": {
    "tag:wefty-control": ["autogroup:admin"],
    "tag:wefty-client": ["autogroup:admin"],
    "tag:wefty-agent": ["autogroup:admin"]
  },
  "acls": [
    {"action": "accept", "proto": "tcp", "src": ["tag:wefty-client", "tag:wefty-agent"], "dst": ["tag:wefty-control:80", "tag:wefty-client:80"]},
    {"action": "accept", "proto": "tcp", "src": ["tag:wefty-control"], "dst": ["tag:wefty-client:80"]},
    {"action": "accept", "proto": "tcp", "src": ["autogroup:owner"], "dst": ["tag:wefty-control:80", "tag:wefty-agent:*"]}
  ]
}
`,
	}
}
