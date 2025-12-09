import pandas as pd
import os
import tempfile
from src.raw_data_parsing import get_file_paths, files_to_frame, parse_mapping_xml_to_frame, explode_tasks


def test_directory_not_found_returns_empty_list():
    non_existent_path = "/non/existent/path/12345"
    result = get_file_paths(non_existent_path)
    assert result == [], "Should return an empty list for a non-existent directory"

def test_input_is_file_returns_empty_list():
    # temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        file_path = tmp_file.name
    try:
        result = get_file_paths(file_path)
        assert result == [], "Should return an empty list when input path is a file"
    finally:
        os.remove(file_path)

def test_empty_input_list_returns_empty_frame():
    expected_df = pd.DataFrame({})
    input = []
    result = files_to_frame(input_files=input, tag_dict={"tag1": "a", "tag2": "b"})
    pd.testing.assert_frame_equal(result, expected_df, check_dtype=False)

def test_empty_input_list_returns_empty_meta_frame():
    expected_df = pd.DataFrame({})
    input = []
    result = parse_mapping_xml_to_frame(input_file_paths=input)
    pd.testing.assert_frame_equal(result, expected_df, check_dtype=False)

def test_explode_returns_rows():
    expected_df = pd.DataFrame({
        "id": [1, 1, 2, 2],
        "text": [["task 1-1", "task 1-2"],
                 ["task 1-1", "task 1-2"],
                 ["task 2-1", "task 2-2"],
                 ["task 2-1", "task 2-2"]],
        "task": ["task 1-1", "task 1-2", "task 2-1", "task 2-2"]
    })
    input_df = pd.DataFrame({
        "id": [1, 2],
        "text": [["task 1-1", "task 1-2"], ["task 2-1", "task 2-2"]]
    })
    result = explode_tasks(df=input_df, task_col="text")
    pd.testing.assert_frame_equal(result, expected_df, check_dtype=False)

