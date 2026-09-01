package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"github.com/ugent-library/catbird"
)

// ErrInvalidToken is returned by Verify for any string Token did not mint
// with the same secret.
var ErrInvalidToken = errors.New("wire: invalid token")

// Wire holds what every call needs — the pool, the renderer, the token
// secret. One per process, built at startup. It is configuration, not
// machinery: it owns no goroutine.
type Wire struct {
	db     catbird.Conn
	rd     *Renderer
	secret []byte
	limit  int
	logger *slog.Logger
}

// Options configures a Wire.
type Options struct {
	Secret []byte       // signs tokens; required
	Limit  int          // messages per poll; default 50
	Logger *slog.Logger // default slog.Default()
}

// New returns a Wire on db that dispatches through rd. It panics on an empty
// secret, because a Wire is startup configuration like a renderer rule:
// HMAC accepts an empty key, so without the check the process would mint
// tokens anyone can forge and nothing would ever look wrong.
func New(db catbird.Conn, rd *Renderer, opts Options) *Wire {
	if len(opts.Secret) == 0 {
		panic("wire: New called without a secret")
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Wire{db: db, rd: rd, secret: opts.Secret, limit: opts.Limit, logger: opts.Logger}
}

// Token is what one page may read: the topics, and the cursor it acks when
// it has one.
type Token struct {
	Topics []string `json:"topics,omitempty"` // in the stream's pattern grammar
	Cursor string   `json:"cursor,omitempty"` // the cb_cursors row to read from and ack, "" for none
}

// Token signs what a page may read: the cursor it acks ("" for none) and the
// topics. The cursor comes first because the topics are variadic. The token
// is URL-safe, so where it travels is the route's decision — a query
// parameter, a header, a cookie. There is no expiry: the poll route sits
// behind the application's authentication like every other route, so who may
// ask is the session's question and the token only narrows what this page
// reads.
func (w *Wire) Token(cursor string, topics ...string) string {
	// A struct of strings cannot fail to marshal.
	payload, _ := json.Marshal(Token{Topics: topics, Cursor: cursor})
	return base64.RawURLEncoding.EncodeToString(payload) +
		"." + base64.RawURLEncoding.EncodeToString(w.sign(payload))
}

// Verify checks the signature and returns what the token grants. Everything
// else — a forged signature, an edited grant, a string that is no token at
// all — is ErrInvalidToken, one answer on purpose: the caller turns it into
// one 401, and which check failed is nothing a browser should learn.
func (w *Wire) Verify(s string) (Token, error) {
	payload64, signature64, ok := strings.Cut(s, ".")
	if !ok {
		return Token{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(payload64)
	if err != nil {
		return Token{}, ErrInvalidToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(signature64)
	if err != nil {
		return Token{}, ErrInvalidToken
	}
	if !hmac.Equal(signature, w.sign(payload)) {
		return Token{}, ErrInvalidToken
	}
	// The payload is parsed only after the signature checks, so bytes an
	// attacker chose never reach the decoder.
	var t Token
	if err := json.Unmarshal(payload, &t); err != nil {
		return Token{}, ErrInvalidToken
	}
	return t, nil
}

// sign is the HMAC-SHA256 of payload under the Wire's secret.
func (w *Wire) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, w.secret)
	mac.Write(payload)
	return mac.Sum(nil)
}
