from dataclasses import fields
from pathlib import Path
from src.config import Paths

def test_paths(tmp_path: Path, **overrides) -> Paths:
    values = {}
    for f in fields(Paths):
        if f.name in overrides:
            values[f.name] = overrides[f.name]
        else:
            values[f.name] = tmp_path / f"{f.name}.dummy"
            
    return Paths(**values)
            