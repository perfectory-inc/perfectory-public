#!/usr/bin/env python3
"""The data catalog's state directory, from config/host-layout.contract.json `datahub` (one fact).

    datahub_state.py dir       print state_dir
    datahub_state.py adopt     (root) make state_dir 0700 and copy each file it lacks from the first
                               adopt_from directory that has it: runtime.env 0600, user.props 0644

A file state_dir already has is never replaced, and adopt_from is only read: the copy is the state
from then on, and the old directory stays until someone removes it.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import sys

CONTRACT = pathlib.Path(__file__).resolve().parents[2] / "config" / "host-layout.contract.json"
FILES = {"runtime.env": 0o600, "user.props": 0o644}  # user.props is read inside the container by a non-root user


def load(path: pathlib.Path = CONTRACT) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["datahub"]


def adopt(state_dir: pathlib.Path, sources: list[pathlib.Path], owner: tuple[int, int] | None = (0, 0)) -> list[str]:
    """The files copied, as "<file> from <source>"."""
    state_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(state_dir, 0o700)
    if owner is not None:
        os.chown(state_dir, *owner)
    copied = []
    for name, mode in FILES.items():
        target = state_dir / name
        if target.exists() and target.stat().st_size > 0:
            continue
        source = next((s / name for s in sources if (s / name).is_file() and (s / name).stat().st_size > 0), None)
        if source is None:
            continue
        temporary = state_dir / f".{name}.adopting"
        shutil.copyfile(source, temporary)
        os.chmod(temporary, mode)
        if owner is not None:
            os.chown(temporary, *owner)
        os.replace(temporary, target)
        copied.append(f"{name} from {source.parent}")
    return copied


def main(argv: list[str]) -> int:
    layout = load()
    if argv == ["dir"]:
        print(layout["state_dir"])
    elif argv == ["adopt"]:
        for line in adopt(pathlib.Path(layout["state_dir"]), [pathlib.Path(p) for p in layout["adopt_from"]]):
            print(f"datahub: adopted {line}")
    else:
        print(__doc__.splitlines()[2].strip(), file=sys.stderr)
        return 64
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
