from hashlib import sha256
import json

import psycopg
from psycopg.types.json import Jsonb

from .common import RawResponse

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.responses (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    fetched_at timestamptz NOT NULL,
    endpoint text NOT NULL,
    parameters jsonb NOT NULL,
    content_type text NOT NULL,
    body bytea NOT NULL,
    body_sha256 text NOT NULL,
    request_sha256 text NOT NULL,
    CHECK (window_end > window_start),
    UNIQUE (request_sha256, body_sha256)
);
CREATE INDEX IF NOT EXISTS responses_source_window
    ON raw.responses (source, window_start, window_end);
"""


def initialize(connection: psycopg.Connection) -> None:
    with connection.transaction():
        connection.execute(SCHEMA_SQL)


def save(connection: psycopg.Connection, record: RawResponse) -> bool:
    """Keep revisions; identical request/body pairs are a no-op."""
    request = json.dumps(
        {
            "source": record.source,
            "endpoint": record.endpoint,
            "parameters": record.parameters,
            "window_start": record.window_start.isoformat(),
            "window_end": record.window_end.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    with connection.transaction():
        cursor = connection.execute(
            """
            INSERT INTO raw.responses
                (source, window_start, window_end, fetched_at, endpoint,
                 parameters, content_type, body, body_sha256, request_sha256)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_sha256, body_sha256) DO NOTHING
            RETURNING id
            """,
            (
                record.source, record.window_start, record.window_end,
                record.fetched_at, record.endpoint, Jsonb(record.parameters),
                record.content_type, record.body, sha256(record.body).hexdigest(),
                sha256(request.encode()).hexdigest(),
            ),
        )
        return cursor.fetchone() is not None
