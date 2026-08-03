package wire

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalid         = errors.New("catbird: invalid argument")
	ErrNotDefined      = errors.New("catbird: not defined")
	ErrStreamsRequired = errors.New("catbird: stream schema required")
)

// wrapErr translates the SQL surface's raised errors into the package
// sentinels so callers can use errors.Is. The SQLSTATE codes are shared
// across the modules: IRD01 invalid argument, IRD02 not defined, IRD03 a
// required module is not installed.
func wrapErr(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	var sentinel error
	switch pgErr.Code {
	case "IRD01":
		sentinel = ErrInvalid
	case "IRD02":
		sentinel = ErrNotDefined
	case "IRD03":
		sentinel = ErrStreamsRequired
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel,
		strings.TrimPrefix(pgErr.Message, "catbird: "))
}
