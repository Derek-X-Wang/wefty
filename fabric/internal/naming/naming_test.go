package naming

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		address   string
		wantName  bool
		want      string
		transport string
		wantErr   bool
	}{
		{address: "127.0.0.1:1234"},
		{address: "wefty://control-plane", wantName: true, want: "wefty://control-plane", transport: "control-plane:80"},
		{address: "wefty://node/runner-1", wantName: true, want: "wefty://node/runner-1", transport: "node-a28bbc963ccacc17:80"},
		{address: "wefty://node/", wantName: true, wantErr: true},
		{address: "wefty://node/Runner_1", wantName: true, want: "wefty://node/Runner_1", transport: "node-cc2e08699ba0f120:80"},
		{address: "wefty://other", wantName: true, wantErr: true},
		{address: "wefty://control-plane:443", wantName: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			got, isName, err := Parse(tt.address)
			if isName != tt.wantName {
				t.Fatalf("Parse() isName = %v, want %v", isName, tt.wantName)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil || !isName {
				return
			}
			if got.String() != tt.want {
				t.Errorf("String() = %q, want %q", got.String(), tt.want)
			}
			if got.TransportAddress() != tt.transport {
				t.Errorf("TransportAddress() = %q, want %q", got.TransportAddress(), tt.transport)
			}
		})
	}
}
