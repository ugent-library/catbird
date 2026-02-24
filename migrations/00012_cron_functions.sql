-- Cron parsing and next-tick calculation functions (PL/pgSQL, no pg_cron extension)

-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cron_expand_spec(cron_spec text)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    _spec text := lower(trim(cron_spec));
BEGIN
    IF _spec IS NULL OR _spec = '' THEN
        RAISE EXCEPTION 'cb: cron spec must not be empty';
    END IF;

    CASE _spec
        WHEN '@yearly', '@annually' THEN RETURN '0 0 1 1 *';
        WHEN '@monthly' THEN RETURN '0 0 1 * *';
        WHEN '@weekly' THEN RETURN '0 0 * * 0';
        WHEN '@daily', '@midnight' THEN RETURN '0 0 * * *';
        WHEN '@hourly' THEN RETURN '0 * * * *';
        ELSE
            RETURN regexp_replace(_spec, '\s+', ' ', 'g');
    END CASE;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cron_parse_field(field text, min_value int, max_value int, allow_seven_as_sunday boolean DEFAULT false)
RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
    _field text := lower(trim(field));
    _token text;
    _result int[] := ARRAY[]::int[];
    _vals int[];
    _step int;
    _start int;
    _end int;
    _value int;
BEGIN
    IF _field IS NULL OR _field = '' THEN
        RAISE EXCEPTION 'cb: cron field must not be empty';
    END IF;

    IF _field = '*' THEN
        SELECT array_agg(i ORDER BY i)
        INTO _result
        FROM generate_series(min_value, max_value) AS g(i);
        RETURN _result;
    END IF;

    FOREACH _token IN ARRAY regexp_split_to_array(_field, ',')
    LOOP
        _token := trim(_token);

        IF _token = '*' THEN
            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(min_value, max_value) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSIF _token ~ '^\*/\d+$' THEN
            _step := split_part(_token, '/', 2)::int;
            IF _step <= 0 THEN
                RAISE EXCEPTION 'cb: invalid cron step value in token "%"', _token;
            END IF;

            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(min_value, max_value, _step) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSIF _token ~ '^\d+$' THEN
            _value := _token::int;
            IF allow_seven_as_sunday AND _value = 7 THEN
                _value := 0;
            END IF;
            IF _value < min_value OR _value > max_value THEN
                RAISE EXCEPTION 'cb: cron value % out of range [% - %] in token "%"', _value, min_value, max_value, _token;
            END IF;
            _result := _result || ARRAY[_value];
        ELSIF _token ~ '^\d+-\d+(\/\d+)?$' THEN
            _start := split_part(split_part(_token, '/', 1), '-', 1)::int;
            _end := split_part(split_part(_token, '/', 1), '-', 2)::int;

            IF position('/' in _token) > 0 THEN
                _step := split_part(_token, '/', 2)::int;
            ELSE
                _step := 1;
            END IF;

            IF _step <= 0 THEN
                RAISE EXCEPTION 'cb: invalid cron step value in token "%"', _token;
            END IF;
            IF _start > _end THEN
                RAISE EXCEPTION 'cb: invalid cron range in token "%" (start > end)', _token;
            END IF;

            -- Keep day-of-week handling explicit: 7 is only accepted as a single value.
            IF allow_seven_as_sunday AND (_start = 7 OR _end = 7) THEN
                RAISE EXCEPTION 'cb: use day-of-week 0 or single value 7 for Sunday in token "%"', _token;
            END IF;

            IF _start < min_value OR _end > max_value THEN
                RAISE EXCEPTION 'cb: cron range %-% out of bounds [% - %] in token "%"', _start, _end, min_value, max_value, _token;
            END IF;

            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(_start, _end, _step) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSE
            RAISE EXCEPTION 'cb: unsupported cron token "%"', _token;
        END IF;
    END LOOP;

    IF cardinality(_result) = 0 THEN
        RAISE EXCEPTION 'cb: cron field "%" produced no values', _field;
    END IF;

    SELECT array_agg(v ORDER BY v)
    INTO _result
    FROM (
        SELECT DISTINCT unnest(_result) AS v
    ) x;

    RETURN _result;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cron_matches(cron_spec text, ts timestamptz)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _expanded text;
    _parts text[];
    _minute_values int[];
    _hour_values int[];
    _dom_values int[];
    _month_values int[];
    _dow_values int[];
    _ts_utc timestamp;
    _minute int;
    _hour int;
    _dom int;
    _month int;
    _dow int;
    _dom_field text;
    _dow_field text;
    _dom_is_star boolean;
    _dow_is_star boolean;
    _day_matches boolean;
BEGIN
    _expanded := cb_cron_expand_spec(cron_spec);
    _parts := regexp_split_to_array(_expanded, ' ');

    IF array_length(_parts, 1) <> 5 THEN
        RAISE EXCEPTION 'cb: cron spec must contain 5 fields, got "%"', _expanded;
    END IF;

    _minute_values := cb_cron_parse_field(_parts[1], 0, 59, false);
    _hour_values := cb_cron_parse_field(_parts[2], 0, 23, false);
    _dom_values := cb_cron_parse_field(_parts[3], 1, 31, false);
    _month_values := cb_cron_parse_field(_parts[4], 1, 12, false);
    _dow_values := cb_cron_parse_field(_parts[5], 0, 6, true);

    _ts_utc := ts AT TIME ZONE 'UTC';

    _minute := extract(minute FROM _ts_utc)::int;
    _hour := extract(hour FROM _ts_utc)::int;
    _dom := extract(day FROM _ts_utc)::int;
    _month := extract(month FROM _ts_utc)::int;
    _dow := extract(dow FROM _ts_utc)::int;

    _dom_field := _parts[3];
    _dow_field := _parts[5];
    _dom_is_star := (_dom_field = '*');
    _dow_is_star := (_dow_field = '*');

    IF _dom_is_star AND _dow_is_star THEN
        _day_matches := true;
    ELSIF _dom_is_star THEN
        _day_matches := (_dow = ANY(_dow_values));
    ELSIF _dow_is_star THEN
        _day_matches := (_dom = ANY(_dom_values));
    ELSE
        -- Standard cron semantics: when both DOM and DOW are restricted,
        -- the date matches if either field matches.
        _day_matches := (_dom = ANY(_dom_values)) OR (_dow = ANY(_dow_values));
    END IF;

    RETURN
        _minute = ANY(_minute_values)
        AND _hour = ANY(_hour_values)
        AND _month = ANY(_month_values)
        AND _day_matches;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_next_cron_tick(cron_spec text, from_time timestamptz)
RETURNS timestamptz
LANGUAGE plpgsql AS $$
DECLARE
    _candidate timestamp;
    _candidate_tz timestamptz;
    _limit int := 5 * 366 * 24 * 60; -- search up to 5 years minute-by-minute
    _i int;
BEGIN
    _candidate := date_trunc('minute', from_time AT TIME ZONE 'UTC');

    IF _candidate <= (from_time AT TIME ZONE 'UTC') THEN
        _candidate := _candidate + interval '1 minute';
    END IF;

    FOR _i IN 1.._limit LOOP
        _candidate_tz := _candidate AT TIME ZONE 'UTC';

        IF cb_cron_matches(cron_spec, _candidate_tz) THEN
            RETURN _candidate_tz;
        END IF;

        _candidate := _candidate + interval '1 minute';
    END LOOP;

    RAISE EXCEPTION 'cb: no cron tick found within % minutes for spec "%"', _limit, cron_spec;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_next_cron_tick(text, timestamptz);
DROP FUNCTION IF EXISTS cb_cron_matches(text, timestamptz);
DROP FUNCTION IF EXISTS cb_cron_parse_field(text, int, int, boolean);
DROP FUNCTION IF EXISTS cb_cron_expand_spec(text);
