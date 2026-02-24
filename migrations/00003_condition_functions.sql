-- +goose up

-- +goose statementbegin
-- cb_parse_condition: Canonical condition parser for all clients (go, python, js, ruby, java, etc.)
-- Takes a condition expression and returns jsonb with parsed components: {field, operator, value}
-- Raises an exception if the expression is invalid
CREATE OR REPLACE FUNCTION cb_parse_condition(expr text)
RETURNS jsonb AS $$
DECLARE
    _expr text := trim(expr);
    _negated boolean := false;
    _op text;
    _op_idx int;
    _field text;
    _value_str text;
    _value jsonb;
    _operators text[] := ARRAY['exists', 'contains', 'lte', 'gte', 'eq', 'ne', 'gt', 'lt', 'in'];
BEGIN
    IF _expr = '' THEN
        RAISE EXCEPTION 'cb: empty condition expression';
    END IF;

    IF _expr LIKE 'not %' THEN
        _negated := true;
        _expr := trim(substring(_expr FROM 5));

        IF _expr = '' THEN
            RAISE EXCEPTION 'cb: empty condition expression after not';
        END IF;
    END IF;

    -- Find operator (check longer operators first to avoid "gte" matching "gt")
    FOR _op IN SELECT unnest(_operators)
    LOOP
        _op_idx := position(' ' || _op || ' ' IN ' ' || _expr || ' ');
        IF _op_idx > 0 THEN
            _op := _op;
            _op_idx := _op_idx - 1; -- Adjust for leading space we added
            EXIT;
        END IF;
        
        -- Check at beginning: "operator "
        IF _expr LIKE _op || ' %' THEN
            _op_idx := 0;
            EXIT;
        END IF;

        -- For "exists" operator at end: "field exists"
        IF _op = 'exists' AND _expr LIKE '% ' || _op THEN
            _op_idx := length(_expr) - length(_op) - 1;
            _field := trim(substring(_expr FROM 1 FOR _op_idx));
            RETURN jsonb_build_object(
                'field', _field,
                'operator', _op,
                'value', NULL
            );
        END IF;
    END LOOP;

    IF _op IS NULL THEN
        RAISE EXCEPTION 'cb: no valid operator found in condition: %', _expr;
    END IF;

    -- Extract field and value
    IF _op_idx = 0 THEN
        -- Operator at beginning
        _field := '';
        _value_str := trim(substring(_expr FROM length(_op) + 1));
    ELSE
        -- Operator in middle
        _field := trim(substring(_expr FROM 1 FOR _op_idx - 1));
        _value_str := trim(substring(_expr FROM _op_idx + length(_op) + 2));
    END IF;

    IF _field = '' AND _op != 'exists' THEN
        RAISE EXCEPTION 'cb: missing field in condition: %', _expr;
    END IF;

    -- Validate field (alphanumeric, dots, underscores, brackets)
    IF _field ~ '[^a-zA-Z0-9_.\[\]]' THEN
        RAISE EXCEPTION 'cb: invalid field name: %', _field;
    END IF;

    -- Parse value
    _value := cb_parse_condition_value(_value_str, _op);

    RETURN jsonb_build_object(
        'field', _field,
        'operator', _op,
        'value', _value,
        'negated', _negated
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_parse_condition_value: Parse a condition value string into appropriate JSON type
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_parse_condition_value(val_str text, op text)
RETURNS jsonb AS $$
DECLARE
    _val_str text := trim(val_str);
    _num numeric;
BEGIN
    IF op = 'exists' THEN
        RETURN NULL;
    END IF;

    IF _val_str = '' THEN
        RAISE EXCEPTION 'cb: empty value in condition';
    END IF;

    -- Try JSON array first (for "in" operator or literal arrays)
    IF _val_str LIKE '[%' THEN
        BEGIN
            RETURN _val_str::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'cb: invalid JSON array value: %', _val_str;
        END;
    END IF;

    -- Try as boolean
    IF _val_str = 'true' THEN
        RETURN 'true'::jsonb;
    END IF;
    IF _val_str = 'false' THEN
        RETURN 'false'::jsonb;
    END IF;

    -- Try as number
    BEGIN
        _num := _val_str::numeric;
        RETURN to_jsonb(_num);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    -- Try as JSON string (quoted)
    IF _val_str LIKE '"%' THEN
        BEGIN
            RETURN _val_str::jsonb;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END IF;

    -- Accept as unquoted string identifier (letters, digits, underscore, hyphen)
    IF _val_str ~ '^[a-zA-Z_][a-zA-Z0-9_-]*$' THEN
        RETURN to_jsonb(_val_str);
    END IF;

    RAISE EXCEPTION 'cb: unable to parse condition value: %', _val_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_evaluate_condition: Evaluate a parsed condition against step output
-- Condition jsonb format: {field, operator, value, negated}
-- Returns true if condition is satisfied, false otherwise
-- Used to determine if conditions allow step execution
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_evaluate_condition(condition jsonb, output jsonb)
RETURNS boolean AS $$
DECLARE
    _field text := condition->>'field';
    _operator text := condition->>'operator';
    _value jsonb := condition->'value';
    _field_val jsonb;
    _negated boolean := coalesce((condition->>'negated')::boolean, false);
    _result boolean;
BEGIN
    IF condition IS NULL OR output IS NULL THEN
        RETURN false;
    END IF;

    IF _field IS NULL OR _operator IS NULL THEN
        RETURN false;
    END IF;

    -- Extract the field value from output using jsonb path
    _field_val := cb_get_jsonb_field(output, _field);

    -- Evaluate based on operator
    CASE _operator
        WHEN 'exists' THEN
            _result := _field_val IS NOT NULL;
        
        WHEN 'eq' THEN
            -- Handle NULL: if field doesn't exist, return false
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := _field_val = _value;
            END IF;
        
        WHEN 'ne' THEN
            IF _field_val IS NULL THEN
                _result := true; -- NULL != value
            ELSE
                _result := _field_val <> _value;
            END IF;
        
        WHEN 'gt' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) > (_value::text::numeric);
            END IF;
        
        WHEN 'gte' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) >= (_value::text::numeric);
            END IF;
        
        WHEN 'lt' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) < (_value::text::numeric);
            END IF;
        
        WHEN 'lte' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) <= (_value::text::numeric);
            END IF;
        
        WHEN 'in' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                -- Check if _field_val is in the array _value
                _result := _field_val = ANY(jsonb_array_elements(_value));
            END IF;
        
        WHEN 'contains' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                -- For strings, check substring
                -- For arrays/objects, check containment
                IF jsonb_typeof(_field_val) = 'string' THEN
                    _result := (_field_val#>>'{}') LIKE '%' || (_value#>>'{}') || '%';
                ELSE
                    _result := _field_val @> _value;
                END IF;
            END IF;
        
        ELSE
            _result := false;
    END CASE;

    IF _negated THEN
        RETURN NOT _result;
    END IF;

    RETURN _result;
EXCEPTION WHEN OTHERS THEN
    -- If evaluation fails, return false (condition not met)
    RETURN false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_get_jsonb_field: Extract a value from jsonb using a field path
-- Supports nested paths: "user.age" or array access: "items[0]"
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_get_jsonb_field(obj jsonb, field_path text)
RETURNS jsonb AS $$
DECLARE
    _parts text[];
    _part text;
    _current jsonb := obj;
    _i int;
BEGIN
    IF obj IS NULL OR field_path IS NULL OR field_path = '' THEN
        RETURN NULL;
    END IF;

    -- Split path by dots (simple split, doesn't handle complex escaping)
    _parts := string_to_array(field_path, '.');
    
    FOREACH _part IN ARRAY _parts
    LOOP
        IF _current IS NULL THEN
            RETURN NULL;
        END IF;

        -- Check for array index notation like "items[0]"
        IF _part LIKE '%[%]%' THEN
            _current := _current -> split_part(_part, '[', 1) -> split_part(split_part(_part, '[', 2), ']', 1)::int;
        ELSE
            -- Regular object key access
            _current := _current -> _part;
        END IF;
    END LOOP;

    RETURN _current;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- +goose statementbegin
-- cb_evaluate_condition_expr: Wrapper that takes text condition
-- Parses the condition first, then evaluates it
-- This is useful for testing and when conditions are stored as text
CREATE OR REPLACE FUNCTION cb_evaluate_condition_expr(
    condition_expr text,
    output jsonb
)
RETURNS boolean AS $$
DECLARE
    _parsed_condition jsonb;
BEGIN
    IF condition_expr IS NULL OR output IS NULL THEN
        RETURN false;
    END IF;

    -- Parse the condition
    _parsed_condition := cb_parse_condition(condition_expr);
    
    IF _parsed_condition IS NULL THEN
        RETURN false;
    END IF;

    -- Evaluate using the JSONB version
    RETURN cb_evaluate_condition(_parsed_condition, output);
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_evaluate_condition_expr(text, jsonb);
DROP FUNCTION IF EXISTS cb_get_jsonb_field(jsonb, text);
DROP FUNCTION IF EXISTS cb_evaluate_condition(jsonb, jsonb);
DROP FUNCTION IF EXISTS cb_parse_condition_value(text, text);
DROP FUNCTION IF EXISTS cb_parse_condition(text);
