CREATE SCHEMA IF NOT EXISTS message_store;

CREATE TABLE IF NOT EXISTS message_store.messages (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    stream_name text NOT NULL,
    type text NOT NULL,
    position bigint NOT NULL,
    global_position bigserial NOT NULL,
    data jsonb,
    metadata jsonb,
    time TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc') NOT NULL
    );
ALTER TABLE
    message_store.messages
    ADD PRIMARY KEY (global_position) NOT DEFERRABLE INITIALLY IMMEDIATE;

DO $$
BEGIN
DROP TYPE IF EXISTS message_store.message CASCADE;

CREATE TYPE message_store.message AS (
    id varchar,
    stream_name varchar,
    type varchar,
    position bigint,
    global_position bigint,
    data varchar,
    metadata varchar,
    time timestamp
    );
END$$;


CREATE OR REPLACE FUNCTION message_store.acquire_lock(
  stream_name varchar
)
RETURNS bigint
AS $$
DECLARE
_category varchar;
  _category_name_hash bigint;
BEGIN
_category := category(acquire_lock.stream_name);
_category_name_hash := hash_64(_category);
  PERFORM pg_advisory_xact_lock(_category_name_hash);

  IF current_setting('message_store.debug_write', true) = 'on' OR current_setting('message_store.debug', true) = 'on' THEN
    RAISE NOTICE '» acquire_lock';
    RAISE NOTICE 'stream_name: %', acquire_lock.stream_name;
    RAISE NOTICE '_category: %', _category;
    RAISE NOTICE '_category_name_hash: %', _category_name_hash;
END IF;

  RETURN _category_name_hash;
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.cardinal_id(
  stream_name varchar
)
RETURNS varchar
AS $$
DECLARE
_id varchar;
BEGIN
_id := id(cardinal_id.stream_name);

  IF _id IS NULL THEN
    RETURN NULL;
END IF;

RETURN SPLIT_PART(_id, '+', 1);
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION message_store.category(
  stream_name varchar
)
RETURNS varchar
AS $$
BEGIN
RETURN SPLIT_PART(category.stream_name, '-', 1);
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION message_store.get_category_messages(
  category varchar,
  "position" bigint DEFAULT 1,
  batch_size bigint DEFAULT 1000,
  correlation varchar DEFAULT NULL,
  consumer_group_member bigint DEFAULT NULL,
  consumer_group_size bigint DEFAULT NULL,
  condition varchar DEFAULT NULL
)
RETURNS SETOF message_store.message
AS $$
DECLARE
_command text;
BEGIN
  IF NOT is_category(get_category_messages.category) THEN
    RAISE EXCEPTION
      'Must be a category: %',
      get_category_messages.category;
END IF;

position := COALESCE(position, 1);
  batch_size := COALESCE(batch_size, 1000);

_command := '
    SELECT
      id::varchar,
      stream_name::varchar,
      type::varchar,
      position::bigint,
      global_position::bigint,
      data::varchar,
      metadata::varchar,
      time::timestamp
    FROM
      messages
    WHERE
      category(stream_name) = $1 AND
      global_position >= $2';

  IF get_category_messages.correlation IS NOT NULL THEN
    IF position('-' IN get_category_messages.correlation) > 0 THEN
      RAISE EXCEPTION
        'Correlation must be a category (Correlation: %)',
        get_category_messages.correlation;
END IF;

_command := _command || ' AND
      category(metadata->>''correlationStreamName'') = $4';
END IF;

  IF (get_category_messages.consumer_group_member IS NOT NULL AND
      get_category_messages.consumer_group_size IS NULL) OR
      (get_category_messages.consumer_group_member IS NULL AND
      get_category_messages.consumer_group_size IS NOT NULL) THEN

    RAISE EXCEPTION
      'Consumer group member and size must be specified (Consumer Group Member: %, Consumer Group Size: %)',
      get_category_messages.consumer_group_member,
      get_category_messages.consumer_group_size;
END IF;

  IF get_category_messages.consumer_group_member IS NOT NULL AND
      get_category_messages.consumer_group_size IS NOT NULL THEN

    IF get_category_messages.consumer_group_size < 1 THEN
      RAISE EXCEPTION
        'Consumer group size must not be less than 1 (Consumer Group Member: %, Consumer Group Size: %)',
        get_category_messages.consumer_group_member,
        get_category_messages.consumer_group_size;
END IF;

    IF get_category_messages.consumer_group_member < 0 THEN
      RAISE EXCEPTION
        'Consumer group member must not be less than 0 (Consumer Group Member: %, Consumer Group Size: %)',
        get_category_messages.consumer_group_member,
        get_category_messages.consumer_group_size;
END IF;

    IF get_category_messages.consumer_group_member >= get_category_messages.consumer_group_size THEN
      RAISE EXCEPTION
        'Consumer group member must be less than the group size (Consumer Group Member: %, Consumer Group Size: %)',
        get_category_messages.consumer_group_member,
        get_category_messages.consumer_group_size;
END IF;

_command := _command || ' AND
      MOD(@hash_64(cardinal_id(stream_name)), $6) = $5';
END IF;

  IF get_category_messages.condition IS NOT NULL THEN
    IF current_setting('message_store.sql_condition', true) IS NULL OR
        current_setting('message_store.sql_condition', true) = 'off' THEN
      RAISE EXCEPTION
        'Retrieval with SQL condition is not activated';
END IF;

_command := _command || ' AND
      (%s)';
_command := format(_command, get_category_messages.condition);
END IF;

_command := _command || '
    ORDER BY
      global_position ASC';

  IF get_category_messages.batch_size != -1 THEN
    _command := _command || '
      LIMIT
        $3';
END IF;

  IF current_setting('message_store.debug_get', true) = 'on' OR current_setting('message_store.debug', true) = 'on' THEN
    RAISE NOTICE '» get_category_messages';
    RAISE NOTICE 'category ($1): %', get_category_messages.category;
    RAISE NOTICE 'position ($2): %', get_category_messages.position;
    RAISE NOTICE 'batch_size ($3): %', get_category_messages.batch_size;
    RAISE NOTICE 'correlation ($4): %', get_category_messages.correlation;
    RAISE NOTICE 'consumer_group_member ($5): %', get_category_messages.consumer_group_member;
    RAISE NOTICE 'consumer_group_size ($6): %', get_category_messages.consumer_group_size;
    RAISE NOTICE 'condition: %', get_category_messages.condition;
    RAISE NOTICE 'Generated Command: %', _command;
END IF;

RETURN QUERY EXECUTE _command USING
    get_category_messages.category,
    get_category_messages.position,
    get_category_messages.batch_size,
    get_category_messages.correlation,
    get_category_messages.consumer_group_member,
    get_category_messages.consumer_group_size::smallint;
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.get_last_stream_message(
  stream_name varchar
)
RETURNS SETOF message_store.message
AS $$
DECLARE
_command text;
BEGIN
_command := '
    SELECT
      id::varchar,
      stream_name::varchar,
      type::varchar,
      position::bigint,
      global_position::bigint,
      data::varchar,
      metadata::varchar,
      time::timestamp
    FROM
      messages
    WHERE
      stream_name = $1
    ORDER BY
      position DESC
    LIMIT
      1';

  IF current_setting('message_store.debug_get', true) = 'on' OR current_setting('message_store.debug', true) = 'on' THEN
    RAISE NOTICE '» get_last_message';
    RAISE NOTICE 'stream_name ($1): %', get_last_stream_message.stream_name;
    RAISE NOTICE 'Generated Command: %', _command;
END IF;

RETURN QUERY EXECUTE _command USING get_last_stream_message.stream_name;
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.get_stream_messages(
  stream_name varchar,
  "position" bigint DEFAULT 0,
  batch_size bigint DEFAULT 1000,
  condition varchar DEFAULT NULL
)
RETURNS SETOF message_store.message
AS $$
DECLARE
_command text;
  _setting text;
BEGIN
  IF is_category(get_stream_messages.stream_name) THEN
    RAISE EXCEPTION
      'Must be a stream name: %',
      get_stream_messages.stream_name;
END IF;

position := COALESCE(position, 0);
  batch_size := COALESCE(batch_size, 1000);

_command := '
    SELECT
      id::varchar,
      stream_name::varchar,
      type::varchar,
      position::bigint,
      global_position::bigint,
      data::varchar,
      metadata::varchar,
      time::timestamp
    FROM
      messages
    WHERE
      stream_name = $1 AND
      position >= $2';

  IF get_stream_messages.condition IS NOT NULL THEN
    IF current_setting('message_store.sql_condition', true) IS NULL OR
        current_setting('message_store.sql_condition', true) = 'off' THEN
      RAISE EXCEPTION
        'Retrieval with SQL condition is not activated';
END IF;

_command := _command || ' AND
      (%s)';
_command := format(_command, get_stream_messages.condition);
END IF;

_command := _command || '
    ORDER BY
      position ASC';

  IF get_stream_messages.batch_size != -1 THEN
    _command := _command || '
      LIMIT
        $3';
END IF;

  IF current_setting('message_store.debug_get', true) = 'on' OR current_setting('message_store.debug', true) = 'on' THEN
    RAISE NOTICE '» get_stream_messages';
    RAISE NOTICE 'stream_name ($1): %', get_stream_messages.stream_name;
    RAISE NOTICE 'position ($2): %', get_stream_messages.position;
    RAISE NOTICE 'batch_size ($3): %', get_stream_messages.batch_size;
    RAISE NOTICE 'condition ($4): %', get_stream_messages.condition;
    RAISE NOTICE 'Generated Command: %', _command;
END IF;

RETURN QUERY EXECUTE _command USING
    get_stream_messages.stream_name,
    get_stream_messages.position,
    get_stream_messages.batch_size;
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.hash_64(
  value varchar
)
RETURNS bigint
AS $$
DECLARE
_hash bigint;
BEGIN
SELECT left('x' || md5(hash_64.value), 17)::bit(64)::bigint INTO _hash;
return _hash;
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION message_store.id(
  stream_name varchar
)
RETURNS varchar
AS $$
DECLARE
_id_separator_position integer;
BEGIN
_id_separator_position := STRPOS(id.stream_name, '-');

  IF _id_separator_position = 0 THEN
    RETURN NULL;
END IF;

RETURN SUBSTRING(id.stream_name, _id_separator_position + 1);
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION message_store.is_category(
  stream_name varchar
)
RETURNS boolean
AS $$
BEGIN
  IF NOT STRPOS(is_category.stream_name, '-') = 0 THEN
    RETURN FALSE;
END IF;

RETURN TRUE;
END;
$$ LANGUAGE plpgsql
IMMUTABLE;

CREATE OR REPLACE FUNCTION message_store.message_store_version()
RETURNS varchar
AS $$
BEGIN
RETURN '1.2.6';
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.stream_version(
  stream_name varchar
)
RETURNS bigint
AS $$
DECLARE
_stream_version bigint;
BEGIN
SELECT
    max(position) into _stream_version
FROM
    messages
WHERE
    messages.stream_name = stream_version.stream_name;

RETURN _stream_version;
END;
$$ LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION message_store.write_message(
  id varchar,
  stream_name varchar,
  "type" varchar,
  data jsonb,
  metadata jsonb DEFAULT NULL,
  expected_version bigint DEFAULT NULL
)
RETURNS bigint
AS $$
DECLARE
_message_id uuid;
  _stream_version bigint;
  _next_position bigint;
BEGIN
  PERFORM acquire_lock(write_message.stream_name);

_stream_version := stream_version(write_message.stream_name);

  IF _stream_version IS NULL THEN
    _stream_version := -1;
END IF;

  IF write_message.expected_version IS NOT NULL THEN
    IF write_message.expected_version != _stream_version THEN
      RAISE EXCEPTION
        'Wrong expected version: % (Stream: %, Stream Version: %)',
        write_message.expected_version,
        write_message.stream_name,
        _stream_version;
END IF;
END IF;

_next_position := _stream_version + 1;

_message_id = uuid(write_message.id);

INSERT INTO messages
(
    id,
    stream_name,
    position,
    type,
    data,
    metadata
)
VALUES
    (
        _message_id,
        write_message.stream_name,
        _next_position,
        write_message.type,
        write_message.data,
        write_message.metadata
    )
;

IF current_setting('message_store.debug_write', true) = 'on' OR current_setting('message_store.debug', true) = 'on' THEN
    RAISE NOTICE '» write_message';
    RAISE NOTICE 'id ($1): %', write_message.id;
    RAISE NOTICE 'stream_name ($2): %', write_message.stream_name;
    RAISE NOTICE 'type ($3): %', write_message.type;
    RAISE NOTICE 'data ($4): %', write_message.data;
    RAISE NOTICE 'metadata ($5): %', write_message.metadata;
    RAISE NOTICE 'expected_version ($6): %', write_message.expected_version;
    RAISE NOTICE '_stream_version: %', _stream_version;
    RAISE NOTICE '_next_position: %', _next_position;
END IF;

  RETURN _next_position;
END;
$$ LANGUAGE plpgsql
VOLATILE;

DROP INDEX IF EXISTS message_store.messages_category;

CREATE INDEX messages_category ON message_store.messages (
                                                          message_store.category(stream_name),
                                                          global_position,
                                                          message_store.category(metadata->>'correlationStreamName')
    );

DROP INDEX IF EXISTS message_store.messages_id;

CREATE UNIQUE INDEX messages_id ON message_store.messages (
                                                           id
    );

DROP INDEX IF EXISTS messages_stream;

CREATE UNIQUE INDEX messages_stream ON message_store.messages (
                                                               stream_name,
                                                               position
    );

CREATE OR REPLACE VIEW message_store.category_type_summary AS
  WITH
    type_count AS (
      SELECT
        message_store.category(stream_name) AS category,
        type,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        category,
        type
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    category,
    type,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    type_count,
    total_count
ORDER BY
    category,
    type;

CREATE OR REPLACE VIEW message_store.stream_summary AS
  WITH
    stream_count AS (
      SELECT
        stream_name,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        stream_name
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    stream_name,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    stream_count,
    total_count
ORDER BY
    stream_name;

CREATE OR REPLACE VIEW message_store.stream_type_summary AS
  WITH
    type_count AS (
      SELECT
        stream_name,
        type,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        stream_name,
        type
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    stream_name,
    type,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    type_count,
    total_count
ORDER BY
    stream_name,
    type;

CREATE OR REPLACE VIEW message_store.type_category_summary AS
  WITH
    type_count AS (
      SELECT
        type,
        message_store.category(stream_name) AS category,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        type,
        category
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    type,
    category,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    type_count,
    total_count
ORDER BY
    type,
    category;

CREATE OR REPLACE VIEW message_store.type_stream_summary AS
  WITH
    type_count AS (
      SELECT
        type,
        stream_name,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        type,
        stream_name
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    type,
    stream_name,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    type_count,
    total_count
ORDER BY
    type,
    stream_name;

CREATE OR REPLACE VIEW message_store.type_summary AS
  WITH
    type_count AS (
      SELECT
        type,
        COUNT(id) AS message_count
      FROM
        message_store.messages
      GROUP BY
        type
    ),

    total_count AS (
      SELECT
        COUNT(id)::decimal AS total_count
      FROM
        message_store.messages
    )

SELECT
    type,
    message_count,
    ROUND((message_count / total_count)::decimal * 100, 2) AS percent
FROM
    type_count,
    total_count
ORDER BY
    type;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

   
DO $$
BEGIN
CREATE ROLE message_store WITH LOGIN;
EXCEPTION
  WHEN duplicate_object THEN
      RAISE NOTICE 'The message_store role already exists';
END$$;
 
GRANT EXECUTE ON FUNCTION gen_random_uuid() TO message_store;
GRANT EXECUTE ON FUNCTION message_store.acquire_lock(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.cardinal_id(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.category(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.get_category_messages(varchar, bigint, bigint, varchar, bigint, bigint, varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.get_last_stream_message(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.get_stream_messages(varchar, bigint, bigint, varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.hash_64(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.id(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.is_category(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.message_store_version() TO message_store;
GRANT EXECUTE ON FUNCTION message_store.stream_version(varchar) TO message_store;
GRANT EXECUTE ON FUNCTION message_store.write_message(varchar, varchar, varchar, jsonb, jsonb, bigint) TO message_store;
GRANT USAGE ON SCHEMA message_store TO message_store;
GRANT USAGE, SELECT ON SEQUENCE message_store.messages_global_position_seq TO message_store;
GRANT SELECT, INSERT ON message_store.messages TO message_store;
GRANT SELECT ON message_store.category_type_summary TO message_store;
GRANT SELECT ON message_store.stream_summary TO message_store;
GRANT SELECT ON message_store.stream_type_summary TO message_store;
GRANT SELECT ON message_store.type_category_summary TO message_store;
GRANT SELECT ON message_store.type_stream_summary TO message_store;
GRANT SELECT ON message_store.type_summary TO message_store;