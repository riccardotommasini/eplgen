from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Dict, List

from .base import Engine, Event

@dataclass
class EsperCmdEngine:
    """Adapter that calls an external Esper runner command.

    The command must:
    - read a single JSON object from stdin with fields:
        { "statements": [..], "events": { streamName: [ {..event..}, ... ], ... } }
    - write a single JSON object to stdout:
        { "output": [ {..event..}, ... ] }

    This keeps Python independent of Esper version and runtime details.
    """
    cmd: List[str]

    def run(self, statements: List[str], events: Dict[str, List[Event]]) -> List[Event]:
        payload = {"statements": statements, "events": events}
        p = subprocess.run(
            self.cmd,
            input=json.dumps(payload).encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if p.returncode != 0:
            raise RuntimeError(
                "Esper runner failed\n"
                f"cmd={self.cmd}\n"
                f"rc={p.returncode}\n"
                f"stderr=\n{p.stderr.decode('utf-8', errors='replace')}"
            )
        out = json.loads(p.stdout.decode("utf-8"))
        return out.get("output", [])
