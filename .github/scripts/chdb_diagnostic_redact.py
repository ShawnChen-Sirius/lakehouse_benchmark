"""Scrub bearer-shaped strings before diagnostic output reaches logs or artifacts."""

import re
import sys

JWT = re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*")
BEARER = re.compile(r"(Bearer\s+)[^\s\"']+", re.IGNORECASE)

for line in sys.stdin:
    line = JWT.sub("[REDACTED_JWT]", line)
    line = BEARER.sub(r"\1[REDACTED]", line)
    sys.stdout.write(line)
    sys.stdout.flush()
