import pytest
import os
import pandas as pd
from src.xmlprocessor import XMLProcessor

# ----- Initialization-----
def test_xmlprocessor_initialization_sets_attributes(mock_config):
    processor = XMLProcessor(config=mock_config)

    # Assertions based on mock_config
    assert processor.raw_dir == mock_config.paths.raw_data_dir
    assert processor.tag_dict == {"b11-2": "Tasks", "b11-0": "Summary", "b20-32": "Competencies"}
    assert processor.core_cols["id"] == "dkz_id"
    assert processor.bfield_dict == {}


# ----- Data access helper functions -----
def test_get_input_files_finds_matching_files(mock_config, tmp_path):
    """Tests _get_input_files on file search with prefix"""

    # Make files in mock raw dir
    raw_dir = mock_config.paths.raw_data_dir
    (raw_dir / "beschreibung_beruf_1.xml").touch()
    (raw_dir / "beschreibung_beruf_2.xml").touch()
    (raw_dir / "andere_datei.txt").touch()

    processor = XMLProcessor(config=mock_config)

    # Test with default prefix
    files = processor._get_input_files("beschreibung_beruf_")

    assert len(files) == 2
    assert all("beschreibung_beruf_" in f for f in files)


def test_get_input_files_handles_file_not_found_error(mock_config):
    """Tests _get_input_files, when raw_dir nonexistent"""

    # Delete dir
    if mock_config.paths.raw_data_dir.exists():
        os.rmdir(mock_config.paths.raw_data_dir)

    processor = XMLProcessor(config=mock_config)
    files = processor._get_input_files("test")

    assert files == [], \
        "Method _get_input_files expected to return empty list if raw directory does not exist"


# ----- Parsing methods -----
def test_parse_occ_xml_to_dict_extracts_correct_data(mock_config, mock_occ_xml_content, tmp_path):
    """Tests _parse_occ_xml_to_dict with mock XML content"""
    processor = XMLProcessor(config=mock_config)

    # Create mock XML file (Simulation: dkz_id 123, year 2024)
    mock_file = tmp_path / "beschreibung_beruf_123_2024.xml"
    mock_file.write_text(mock_occ_xml_content)
    result = processor._parse_occ_xml_to_dict(mock_file)

    # Base attributes from filename
    assert result["dkz_id"] == 123
    assert result["year"] == 2024

    # Extracted data from XML (b11-0, b11-2, b20-32)
    assert "b11-0_text" in result
    assert "b11-2_text" in result
    assert result["b11-2_text"] == ['Task A', 'Task B with formatting']
    assert result["b20-32_text"] == ["100", "200"]


def test_parse_meta_xml_to_data_frame_creates_correct_df(mock_config, mock_meta_xml_content, tmp_path):
    """Tests metadata parsing with mock metadata XML"""
    processor = XMLProcessor(config=mock_config)
    raw_dir = mock_config.paths.raw_data_dir

    # Create Mock-XML-Datei in raw_dir and fill with content
    mock_file = raw_dir / "berufe_meta_1.xml"
    mock_file.write_text(mock_meta_xml_content)

    df = processor._parse_meta_xml_to_data_frame(prefix="berufe")
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert "dkz_id" in df.index.names

    # Check for parsed "Nachfolger"/"Vorgänger"-columns
    assert df.loc[1000, "nf_dkz_id"] == 1001
    assert df.loc[2000, "vg_kurzbezeichnung"] == "Beruf D"
    assert pd.isna(df.loc[2000, "nf_dkz_id"])  # nan in Int64 is <NA>


# ----- DataFrame methods -----
def test_set_and_clean_index_sets_correct_multiindex(mock_config):
    """Tests index setting and integrity check"""
    processor = XMLProcessor(config=mock_config)

    # Create mock frame with duplicate index
    mock_data = {
        "dkz_id": [1, 2, 1],
        "year": [2020, 2020, 2020],
        "text": ["A", "B", "C"]
    }
    processor.full_occ_df = pd.DataFrame(mock_data)

    # Should be successful (integrity verification)
    processor._set_and_clean_index()

    # New try with non-duplicate index - for testing structure and names
    mock_data_ok = {
        "dkz_id": [1, 2, 3],
        "year": [2020, 2020, 2021],
        "text": ["A", "B", "C"]
    }
    processor.full_occ_df = pd.DataFrame(mock_data_ok)
    processor._set_and_clean_index()

    assert isinstance(processor.full_occ_df.index, pd.MultiIndex)
    assert processor.full_occ_df.index.names == ["dkz_id", "year"]


def test_split_by_bfield_creates_correct_dictionary(mock_config):
    """Tests the separation in bfield_dict"""
    processor = XMLProcessor(config=mock_config)

    mock_data = {
        "dkz_id": [1], "year": [2020],
        "b11-2_text": ["Tasks"],  # b11-2 should be split off
        "b11-0_revd": ["1.0"],  # b11-0 should be split_off
        "other_col": ["xyz"]  # should be ingored
    }
    processor.full_occ_df = pd.DataFrame(mock_data).set_index(["dkz_id", "year"])

    processor._split_by_bfield()

    assert "b11-2" in processor.bfield_dict
    assert "b11-0" in processor.bfield_dict
    assert len(processor.bfield_dict) == 2  # b20-32 hat keine Spalten im Mock
    assert list(processor.bfield_dict["b11-2"].columns) == ["b11-2_text"]
    assert "andere_spalte" not in processor.bfield_dict["b11-2"].columns


def test_explode_tasks_increases_row_count():
    """Tests static method explode_tasks"""
    mock_data = {
        "id": [1, 2],
        "task_list": [['A', 'B'], ['C']]
    }
    df = pd.DataFrame(mock_data)

    exploded_df = XMLProcessor.explode_tasks(df, "task_list")

    assert exploded_df.shape[0] == 3  # 2 rows before, now should be 3
    assert exploded_df.iloc[0]["task_list"] == 'A'
    assert exploded_df.iloc[1]["task_list"] == 'B'
    assert "id" in exploded_df.columns
