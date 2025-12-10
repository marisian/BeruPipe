import pytest
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

from src.config import Config, Paths, Params


@pytest.fixture
def mock_config(tmp_path):
    # Paths mock
    mock_paths = Paths(
        data_dir=tmp_path / "Daten",
        raw_data_dir=tmp_path / "data/raw_xml",  # Wichtig: MUSS im tmp_path liegen!
        intermediate_data_dir=tmp_path / "data/intermediate",
        processed_data_dir=tmp_path / "data/processed"
    )

    mock_paths.raw_data_dir.mkdir(parents=True, exist_ok=True)
    mock_paths.intermediate_data_dir.mkdir(parents=True, exist_ok=True)
    mock_paths.processed_data_dir.mkdir(parents=True, exist_ok=True)

    # Params mock
    mock_params = Params(
        tag_map={
            "b11-2": "Tasks",
            "b11-0": "Summary",
            "b20-32": "Competencies"
        },
        tags_to_extract=["b11-0", "b11-2"],
        core_input_columns={"id": "dkz_id", "date": "year"},
        prefix_occdata="beschreibung_beruf_",
        prefix_metadata="berufe"
    )

    # final cdf object
    mock_cfg = Config(paths=mock_paths, params=mock_params)
    return mock_cfg

# XML mocking
@pytest.fixture
def mock_occ_xml_content():
    """Simulates content of a 'beschreibung_beruf_123_2024.xml'-file."""
    return """
    <beruf>
        <b11-2 rev="2020-01-01">
            <listitem>Task A</listitem>
            <listitem>Task B with <b>formatting</b></listitem>
            <irrelevant_tag>Ignore</irrelevant_tag>
        </b11-2>
        <b11-0 rev="2020-01-01">
            <p>This is a short description.</p>
        </b11-0>
        <b20-32 rev="2020-01-01">
             <extsysref matrix="true" idref="100"/>
             <extsysref matrix="true" idref="200"/>
        </b20-32>
    </beruf>
    """


@pytest.fixture
def mock_meta_xml_content():
    """Simulates content of a 'berufe_meta.xml'-file."""
    return """
    <root>
        <beruf id="1000" codenr="B 10000-101" kurzbezeichnung="Beruf A" qualistufe="1">
            <nachfolger id="1001" codenr="B 20000-101" kurzbezeichnung="Beruf B"/>
        </beruf>
        <beruf id="2000" codenr="B 20000-202" kurzbezeichnung="Beruf C" qualistufe="2">
            <vorgaenger id="1999" codenr="B 20000-201" kurzbezeichnung="Beruf D"/>
        </beruf>
    </root>
    """