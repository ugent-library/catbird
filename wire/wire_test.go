package wire_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/ugent-library/catbird/wire"
)

// newWire builds a Wire with no database: minting and verifying tokens
// touches nothing but the secret.
func newWire(secret string) *wire.Wire {
	return wire.New(nil, wire.NewRenderer(), wire.Options{Secret: []byte(secret)})
}

func TestTokenRoundTrips(t *testing.T) {
	w := newWire("secret")
	tok, err := w.Verify(w.Token("user:42", "user.42.#", "record.work.7.#"))
	if err != nil {
		t.Fatal(err)
	}
	if tok.Cursor != "user:42" {
		t.Fatalf("cursor %q, want %q", tok.Cursor, "user:42")
	}
	if want := []string{"user.42.#", "record.work.7.#"}; !slices.Equal(tok.Topics, want) {
		t.Fatalf("topics %q, want %q", tok.Topics, want)
	}
}

func TestTokenWithoutCursorRoundTrips(t *testing.T) {
	w := newWire("secret")
	tok, err := w.Verify(w.Token("", "orders.#"))
	if err != nil {
		t.Fatal(err)
	}
	if tok.Cursor != "" {
		t.Fatalf("cursor %q, want none", tok.Cursor)
	}
}

func TestTokenSurvivesAQueryParameter(t *testing.T) {
	// The page usually puts the token in the poll URL, so the encoding must
	// come back from a query string byte for byte, with no escaping.
	w := newWire("secret")
	s := w.Token("user:42", "user.42.#")
	values, err := url.ParseQuery(url.Values{"token": {s}}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got := values.Get("token"); got != s {
		t.Fatalf("token came back as %q, want %q", got, s)
	}
	if _, err := w.Verify(values.Get("token")); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRejectsAnotherSecretsToken(t *testing.T) {
	s := newWire("one").Token("", "orders.#")
	if _, err := newWire("two").Verify(s); !errors.Is(err, wire.ErrInvalidToken) {
		t.Fatalf("err %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsAWidenedGrant(t *testing.T) {
	// The failure the signature exists to stop: a browser that edits its
	// token's grant — here adding another user's topics — and keeps the
	// original signature.
	w := newWire("secret")
	payload64, signature64, _ := strings.Cut(w.Token("user:42", "user.42.#"), ".")
	payload, err := base64.RawURLEncoding.DecodeString(payload64)
	if err != nil {
		t.Fatal(err)
	}
	var tok wire.Token
	if err := json.Unmarshal(payload, &tok); err != nil {
		t.Fatal(err)
	}
	tok.Topics = append(tok.Topics, "user.1.#")
	widened, err := json.Marshal(tok)
	if err != nil {
		t.Fatal(err)
	}
	forged := base64.RawURLEncoding.EncodeToString(widened) + "." + signature64
	if _, err := w.Verify(forged); !errors.Is(err, wire.ErrInvalidToken) {
		t.Fatalf("err %v, want ErrInvalidToken", err)
	}
}

func TestVerifyRejectsWhatIsNoTokenAtAll(t *testing.T) {
	w := newWire("secret")
	for _, s := range []string{
		"",
		"no-separator",
		"a.b",       // too short to be base64
		"!!!.???",   // outside the base64 alphabet
		".signed",   // empty payload half
		"payload.",  // empty signature half
		"a.b.c.d.e", // more halves than a token has
	} {
		if _, err := w.Verify(s); !errors.Is(err, wire.ErrInvalidToken) {
			t.Fatalf("Verify(%q) err %v, want ErrInvalidToken", s, err)
		}
	}
}

func TestVerifyRejectsASignedNonToken(t *testing.T) {
	// A correctly signed payload that does not decode is still invalid: the
	// signature says who minted it, not that it grants anything.
	payload := []byte("not a token")
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write(payload)
	s := base64.RawURLEncoding.EncodeToString(payload) +
		"." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, err := newWire("secret").Verify(s); !errors.Is(err, wire.ErrInvalidToken) {
		t.Fatalf("err %v, want ErrInvalidToken", err)
	}
}

func TestNewPanicsWithoutASecret(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New with no secret did not panic")
		}
	}()
	wire.New(nil, wire.NewRenderer(), wire.Options{})
}
