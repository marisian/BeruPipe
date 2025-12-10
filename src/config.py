from pathlib import Path
from dataclasses import dataclass

# ----------- PATHS -----------

# Base directory
#BASE_DIR = Path(__file__).resolve().parent
BASE_DIR = Path("C:/Path/to/Project")

@dataclass(frozen=True)
class Paths:
    data_dir: Path 
    raw_data_dir: Path
    intermediate_data_dir: Path
    processed_data_dir: Path
    
@dataclass(frozen=True)
class Params:
    tag_map: dict[str, str]
    tags_to_extract: list[str]
    core_input_columns: dict[str, str]
    prefix_occdata: str
    prefix_metadata: str
    
@dataclass(frozen=True)
class Config:
    paths: Paths
    params: Params
    
def get_config(base_dir: Path = BASE_DIR) -> Config:
    data_dir = base_dir / "data"
    raw_data_dir = data_dir / "_OCCDATA/test"
    intermediate_data_dir = data_dir / "intermediate"
    processed_data_dir = data_dir / "processed"
    
    for path in [
        raw_data_dir,
        intermediate_data_dir,
        processed_data_dir
        ]:
        path.mkdir(parents=True, exist_ok=True)
        
    paths = Paths(
        # Directories
        data_dir = data_dir,
        raw_data_dir = raw_data_dir,
        intermediate_data_dir = intermediate_data_dir,
        processed_data_dir = processed_data_dir
        )
    
    params = Params(
        tag_map = {
            "b10-1-2": "Trends",
            "b11-0": "Aufgaben und Taetigkeiten kompakt",
            "b11-1": "Aufgaben und Taetigkeiten Beschreibung",
            "b11-2": "Aufgaben und Taetigkeiten im Einzelnen",
            "b12-02": "Arbeitsorte",
            "b12-1": "Branchen im Einzelnen",
            "b15-0": "Arbeitsgegenstaende / Arbeitsmittel",
            "b20-2": "Faehigkeiten, Kenntnisse, Fertigkeiten",
            "b20-32": "Kompetenzen",
            "b40-02": "Digitalisierung",
            "b50-0": "Verdienst"},
        tags_to_extract = ["b11-0", "b11-2"],
        core_input_columns = {
            "id": "dkz_id",
            "date": "year",
            "text_short": "b11-0_text",
            "text_long": "b11-2_text"
            },
        prefix_occdata = "beschreibung_beruf_",
        prefix_metadata = "berufe"
        )
    
    return Config(paths=paths, params=params)

