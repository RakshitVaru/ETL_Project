import yaml
from dataclasses import dataclass, field
from typing import Dict, Any
import pandas as pd
from .connectors.io import read_any

#class to hold source configuration
@dataclass
class SourceCfg:
    name: str
    kind: str
    path: str
    source_label: str
    kwargs: Dict[str, Any] = field(default_factory=dict)

#function to load yaml configuration file
def load_config(path: str) -> dict:
    with open(path,"r") as f: 
        return yaml.safe_load(f)

#function to extract data from various sources based on configuration
def extract(cfg) -> Dict[str, pd.DataFrame]:
    dfs = {}
    for s in cfg.get("sources", []):
        src = SourceCfg(**s)
        df = read_any(src.path, src.kind, **(src.kwargs or {}))
        df["_source_label"] = src.source_label
        dfs[src.name] = df
    return dfs
