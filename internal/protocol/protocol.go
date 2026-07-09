// Package protocol defines the WebSocket frame types and a few shared close
// codes used between susan and tray.
//
// Wire shape: each non-binary frame is a JSON object encoded as one TEXT
// WebSocket message. Binary frames are the raw bytes of the active request's
// captured stdout+stderr.
package protocol

// HardMaxTimeoutSecs is the wire-contract ceiling on a request's timeout,
// enforced independently by relay and client. It stays under the ~300s request
// timeout of typical fronting proxies so the relay can answer before the edge
// gives up.
const HardMaxTimeoutSecs = 290

// Frame types.
const (
	TypeRegister  = "register"
	TypeWelcome   = "welcome"
	TypeRequest   = "request"
	TypeEndHeader = "end_header"
	TypeEndFooter = "end_footer"
	TypeBye       = "bye"
	TypeError     = "error"
)

// Error codes carried in frames of type "error".
const (
	ErrBadRegister       = "bad_register"
	ErrUnauthorized      = "unauthorized"
	ErrConcurrentRequest = "concurrent_request"
	ErrExecFailed        = "exec_failed"
	ErrCmdTooLarge       = "cmd_too_large"
	ErrBadTimeout        = "bad_timeout"
)

// WebSocket close codes used by this protocol.
const (
	CloseNormal           = 1000
	CloseRelayDraining    = 1001
	CloseProtocolError    = 1002
	CloseFlooding         = 1008
	CloseKeepaliveTimeout = 1011
)

// X-Susan-Reason values.
const (
	ReasonCommandFailed        = "command_failed"
	ReasonWSClientDisconnected = "wsclient_disconnected"
	ReasonProtocolError        = "protocol_error"
	ReasonCommandTimeout       = "command_timeout"
	ReasonDeadlineExceeded     = "deadline_exceeded"
	// ReasonReconnecting marks the retryable 503 a caller gets when its permit's
	// client is momentarily disconnected but still within its resume window.
	ReasonReconnecting = "reconnecting"
)

// Envelope is used to peek at the "type" field of any incoming frame.
type Envelope struct {
	Type  string `json:"type"`
	ReqID string `json:"req_id,omitempty"`
}

// Register: client -> relay, first frame.
type Register struct {
	Type          string `json:"type"`
	Token         string `json:"token"`
	ClientVersion string `json:"client_version"`
	// RelayKey authenticates the client to the relay when the relay sets
	// SUSAN_RELAY_KEY. Empty for an open/public relay. Distinct from Token, the
	// per-caller secret carried in the X-Susan-Permit header as
	// <machine-id>:<call-secret>.
	RelayKey string `json:"relay_key,omitempty"`
	// ResumeID, when set, asks the relay to rebind this registration to the
	// machine id from a prior welcome (so a reconnecting client keeps its
	// permit valid) instead of allocating a fresh one. The relay honors it only
	// if Token matches the retained conn's token; on any mismatch it silently
	// falls back to a fresh id, so a wrong guess never reveals or reclaims
	// another client's id. Empty on a first registration.
	ResumeID string `json:"resume_id,omitempty"`
}

// Welcome: relay -> client.
type Welcome struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	PublicURL string `json:"public_url"`
}

// Request: relay -> client.
type Request struct {
	Type      string `json:"type"`
	ReqID     string `json:"req_id"`
	Body      string `json:"body"`
	TimeoutMs int64  `json:"timeout_ms"`
}

// EndHeader: client -> relay, first response frame.
type EndHeader struct {
	Type       string `json:"type"`
	ReqID      string `json:"req_id"`
	Exit       int    `json:"exit"`
	DurationMs int64  `json:"duration_ms"`
	TimedOut   bool   `json:"timed_out"`
	Truncated  bool   `json:"truncated"`
	SizeBytes  int64  `json:"size_bytes"`
}

// EndFooter: client -> relay, terminates response.
type EndFooter struct {
	Type  string `json:"type"`
	ReqID string `json:"req_id"`
}

// Bye: relay -> client, drain signal.
type Bye struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// Error: either side, request-scoped error.
type ErrorFrame struct {
	Type    string `json:"type"`
	ReqID   string `json:"req_id,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// ValidateHeaderField returns true if s contains only printable ASCII (no CR,
// LF, or non-printables) and is no longer than max bytes. We use this on every
// field that ends up in an HTTP response header.
func ValidateHeaderField(s string, max int) bool {
	if len(s) > max {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}
