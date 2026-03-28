package catbird

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// TokenOpts configures token minting.
type TokenOpts struct {
	Identity string
	ValidFor time.Duration
}

type tokenPayload struct {
	Topics   []string `json:"t"`
	Identity string   `json:"i,omitempty"`
	Expiry   int64    `json:"e,omitempty"`
}

// Token mints a signed, URL-safe SSE connection token for the given topics.
// The token is encrypted with AES-256-GCM and encoded as base64url (no padding).
// Panics if the secret is invalid or the system's random source fails.
func (w *Wire) Token(topics []string, opts ...TokenOpts) string {
	var resolved TokenOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	p := tokenPayload{Topics: topics, Identity: resolved.Identity}
	if resolved.ValidFor > 0 {
		p.Expiry = time.Now().Add(resolved.ValidFor).Unix()
	}

	b, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Sprintf("catbird: cannot marshal token: %v", err))
	}

	ct, err := wireEncrypt(w.secret, b)
	if err != nil {
		panic(fmt.Sprintf("catbird: cannot encrypt token: %v", err))
	}

	return base64.RawURLEncoding.EncodeToString(ct)
}

func (w *Wire) verifyToken(token string) (*tokenPayload, error) {
	ct, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, errors.New("catbird: invalid token encoding")
	}

	b, err := wireDecrypt(w.secret, ct)
	if err != nil {
		return nil, errors.New("catbird: invalid token")
	}

	var p tokenPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, errors.New("catbird: invalid token payload")
	}

	if p.Expiry > 0 && time.Now().Unix() > p.Expiry {
		return nil, errors.New("catbird: token expired")
	}

	if len(p.Topics) == 0 {
		return nil, errors.New("catbird: token has no topics")
	}

	return &p, nil
}

// wireEncrypt encrypts plaintext using AES-256-GCM with the given 32-byte key.
// The returned ciphertext includes a random nonce prefix.
func wireEncrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// wireDecrypt decrypts ciphertext produced by wireEncrypt using the given 32-byte key.
func wireDecrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}
	return gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
}
