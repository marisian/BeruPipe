import os
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_filesystem(mocker):
    MOCK_FILES = [
        "berufe.xml",
        "berufe_1000.xml",
        "beschreibung_beruf_1234_2015.xml",
        "beschreibung_beruf_324523_2019.xml",
        "beschreibung_beruf_124_2020.xml",
        "beschreibung_beruf_124.xml",   # Example of incorrect naming
        "other_file.txt"                # Example of not needed file
    ]
    # mock os.listdir
    mocker.patch.object(os, "listdir", return_value=MOCK_FILES)

    # mock os.path.isdir
    mocker.patch.object(os.path, "isdir", return_value=True)

    # mock os.path.isfile: All data in MOCK_FILES are files
    def mock_isfile(path):
        return os.path.basename(path) in MOCK_FILES

    mocker.patch.object(os.path, "isfile", side_effect=mock_isfile)

    return MOCK_FILES

# todo finalize