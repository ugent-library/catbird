-- +goose up

-- The kernel's shared pure functions. Every module's schema uses these, so
-- the migration runner applies this unit before any module's migrations.
-- Errors raise with SQLSTATE IRD01 (invalid argument), the code convention
-- shared by all module schemas.

CREATE TYPE cb_backoff_kind AS ENUM ('none', 'fixed', 'full_jitter');

-- +goose statementbegin
CREATE FUNCTION cb_valid_name(name text)
RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT name IS NOT NULL
       AND name ~ '^[a-z][a-z0-9_]*$'
       AND octet_length(name) <= 20
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_forever() RETURNS interval LANGUAGE sql IMMUTABLE AS $$
    SELECT interval '-1 second'
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_backoff(kind cb_backoff_kind, base_delay interval, max_delay interval, attempt int)
RETURNS interval LANGUAGE plpgsql AS $$
DECLARE
    _cap interval;
BEGIN
    CASE cb_backoff.kind
    WHEN 'none' THEN
        RETURN '0';
    WHEN 'fixed' THEN
        RETURN least(cb_backoff.base_delay, cb_backoff.max_delay);
    WHEN 'full_jitter' THEN
        _cap := least(cb_backoff.base_delay * (2 ^ least(cb_backoff.attempt - 1, 20)),
                      cb_backoff.max_delay);
        RETURN _cap * random();
    ELSE
        RAISE EXCEPTION 'catbird: unknown backoff kind %', cb_backoff.kind USING ERRCODE = 'IRD01';
    END CASE;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_backoff(cb_backoff_kind, interval, interval, int);
DROP FUNCTION cb_forever();
DROP FUNCTION cb_valid_name(text);
DROP TYPE cb_backoff_kind;
