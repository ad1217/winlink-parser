import dataclasses
import datetime
from collections.abc import Iterable
from typing import Self

KNOWN_B2F_HEADERS = [
    "mid",
    "date",
    "type",
    "from",
    "to",
    "cc",
    "subject",
    "mbo",
    "body",
    "file",
]

B2F_DATE_FORMAT = "%Y/%m/%d %H:%M"


@dataclasses.dataclass
class B2FMessage:
    mid: str
    date: datetime.datetime
    type: str | None
    from_: str
    to: list[str]
    cc: list[str]
    subject: str
    mbo: str
    extra_headers: dict[str, list[str]]
    body: str
    files: list[tuple[str, bytes]]

    @classmethod
    def parse(cls, data: bytes) -> Self:
        header_data, _, contents = data.partition(b"\r\n\r\n")

        headers: dict[str, list[str]] = {}
        for line in header_data.decode("ascii").splitlines():
            k, _, v = line.partition(": ")
            if k in headers:
                headers[k].append(v)
            else:
                headers[k] = [v]

        casefolded_headers = {k.casefold(): v for k, v in headers.items()}

        def get_single_header(name: str) -> str:
            header = casefolded_headers[name.casefold()]
            assert len(header) == 1
            return header[0]

        body_length = int(get_single_header("body"))
        content_type = (
            get_single_header("content-type")
            if "content-type" in casefolded_headers
            else "iso-8859-1"
        )
        body = contents[:body_length].decode(content_type)

        # files are separated by "\r\n" (2 bytes)
        file_offset = int(body_length) + 2
        files = []
        for file_entry in casefolded_headers["file"]:
            file_len, _, file_name = file_entry.partition(" ")
            file_end = file_offset + int(file_len)
            files.append((file_name, contents[file_offset:file_end]))
            file_offset = file_end + 2

        extra_headers = {
            k: v for k, v in headers.items() if k.casefold() not in KNOWN_B2F_HEADERS
        }

        return cls(
            mid=get_single_header("mid"),
            date=datetime.datetime.strptime(get_single_header("date"), B2F_DATE_FORMAT),
            type=get_single_header("type") if "type" in casefolded_headers else None,
            from_=get_single_header("from"),
            to=casefolded_headers["to"],
            cc=casefolded_headers.get("cc", []),
            subject=get_single_header("subject"),
            mbo=get_single_header("mbo"),
            extra_headers=extra_headers,
            body=body,
            files=files,
        )

    def to_lines(self) -> Iterable[bytes]:
        body = self.body.encode("ascii")

        headers = {
            "Mid": self.mid,
            "Date": self.date.strftime(B2F_DATE_FORMAT),
            "Type": self.type,
            "From": self.from_,
            "To": self.to,
            "Cc": self.cc,
            "Subject": self.subject,
            "Mbo": self.mbo,
            "Body": len(body),
            "File": [
                f"{len(contents)} {file_name}" for file_name, contents in self.files
            ],
            **self.extra_headers,
        }
        if headers["Type"] is None:
            del headers["Type"]
        for header_name, values in headers.items():
            if isinstance(values, list):
                for value in values:
                    yield f"{header_name}: {value}".encode("ascii")
            else:
                yield f"{header_name}: {values}".encode("ascii")

        yield b"\r\n"
        yield body

        for _, contents in self.files:
            yield contents

    def to_bytes(self) -> bytes:
        return b"\r\n".join(self.to_lines())
