#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import cast

import psycopg
from psycopg.types.json import Jsonb

from b2f import B2FMessage


def init_db(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS message (
                id serial PRIMARY KEY,
                original bytea NOT NULL,
                mid varchar(12) UNIQUE NOT NULL,
                date timestamp with time zone NOT NULL,
                type text,
                "from" text NOT NULL,
                "to" text array NOT NULL,
                cc text array NOT NULL,
                subject text NOT NULL,
                mbo text NOT NULL,
                body text NOT NULL,
                extra_headers jsonb NOT NULL
            )
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS attachment (
                id serial PRIMARY KEY,
                message_id varchar(12) NOT NULL,
                name varchar(255),
                content bytea NOT NULL,
                CONSTRAINT fk_message FOREIGN KEY(message_id) REFERENCES message(mid)
            )
            """)


def parse_file(conn: psycopg.Connection, filepath: Path):
    d = filepath.read_bytes()
    message = B2FMessage.parse(d)

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO message (
                original, mid, date, type, "from", "to", cc, subject, mbo, body, extra_headers
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                d,
                message.mid,
                message.date,
                message.type,
                message.from_,
                message.to,
                message.cc,
                message.subject,
                message.mbo,
                message.body,
                Jsonb(message.extra_headers),
            ),
        )

        for name, content in message.files:
            cur.execute(
                """
                INSERT INTO attachment (message_id, name, content)
                VALUES (%s, %s, %s);
                """,
                (message.mid, name, content),
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "conninfo",
        help="Postgres connection string (see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)",
    )
    parser.add_argument(
        "mailbox_path", help="Mailbox path, containing .b2f files", type=Path
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    mailbox_path = cast(Path, args.mailbox_path)
    imported_dir = mailbox_path / "imported"
    imported_dir.mkdir(exist_ok=True)

    with psycopg.connect(args.conninfo) as conn:
        init_db(conn)

        for f in (mailbox_path).glob("*.b2f"):
            imported_filename = imported_dir / f.name
            if imported_filename.exists():
                raise Exception(
                    f"File name {f.name} exists in both input and output directories"
                )
            parse_file(conn, f)
            f.rename(imported_filename)
