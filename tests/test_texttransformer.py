import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.texttransformer import TextTransformer

# ----- Helper functions (data cleaning) -----

def test_dropna_removes_nan_rows(mock_config):
    """Tests if _dropna removes rows with None-values"""
    processor = TextTransformer(config=mock_config)

    # DataFrame with empty value
    df = pd.DataFrame({
        "b99-99_text": ["Text A", None, "Text C"],
        "other_col": [1, 2, 3]
    })

    df_cleaned = processor._dropna(df, "b99-99")

    # Expectation: 2 rows
    assert df_cleaned.shape[0] == 2

    # Expectation: Rows 0 and 2 in index
    assert 0 in df_cleaned.index
    assert 2 in df_cleaned.index

    # Expectation: Row 1 not in index
    assert 1 not in df_cleaned.index


def test_clean_text_columns_removes_special_chars(mock_config):
    """Tests if _clean_text_columns removes special characters but keeps german characters"""
    processor = TextTransformer(config=mock_config)

    df = pd.DataFrame({"b11-0_text": ["This has a special character!", "100% characters.", "A test with äöüß"]})

    df_cleaned = processor._clean_text_columns(df, "b11-0")

    assert df_cleaned.loc[0, "b11-0_text"] == "This has a special character"
    assert df_cleaned.loc[1, "b11-0_text"] == " characters"
    assert df_cleaned.loc[2, "b11-0_text"] == "A test with äöüß"

    # Test if column does not exist
    df_no_col = pd.DataFrame({"other_col": ["a"]})
    assert processor._clean_text_columns(df_no_col, "b11-0").shape == (1, 1)


# ----- Normalization method mocking -----
@patch('src.texttransformer.TextTransformer.normalize')
def test_normalize_columns_creates_new_column(mock_normalize, mock_config):
    """Tests, if _normalize_columns calls the staticmethod and creates column"""
    processor = TextTransformer(config=mock_config)

    df = pd.DataFrame({"b11-0_text": ["Text A", "Text B"]})

    # Simulation of staticmethod normalization result
    mock_normalize.return_value = [["text", "a"], ["text", "b"]]

    df_norm = processor._normalize_columns(df, "b11-0")

    assert "b11-0_normalized" in df_norm.columns
    assert df_norm.loc[0, "b11-0_normalized"] == ["text", "a"]

    # check if normalize method was called with text list
    mock_normalize.assert_called_once()
    assert mock_normalize.call_args[0][0] == ["Text A", "Text B"]

def test_static_normalize_method_removes_stopwords_and_lemmatizes():
    """Tests static normalize-method with spacy mock"""

    # mock of spacy doc/token behavior
    mock_doc = MagicMock()

    # Simulation of simple token list
    # 1. tok.lemma_ = "aufgabe", tok.is_stop = False, len(tok.text) >= 2 -> keep
    # 2. tok.lemma_ = "der", tok.is_stop = True -> remove
    # 3. tok.lemma_ = ".", tok.is_punct = True -> remove
    mock_doc.__iter__.return_value = [
        MagicMock(lemma_="aufgabe", is_punct=False, is_stop=False, is_digit=False, is_space=False, is_currency=False,
                  text="Aufgaben"),
        MagicMock(lemma_="der", is_punct=False, is_stop=True, is_digit=False, is_space=False, is_currency=False,
                  text="der"),
        MagicMock(lemma_=".", is_punct=True, is_stop=False, is_digit=False, is_space=False, is_currency=False,
                  text="."),
        MagicMock(lemma_="2024", is_punct=False, is_stop=False, is_digit=True, is_space=False, is_currency=False,
                  text="2024")
    ]

    # 2. Mock of nlp.pipe-call und nlp.load
    mock_nlp = MagicMock()
    mock_nlp.pipe.return_value = [mock_doc]

    with patch('src.texttransformer.spacy.load', return_value=mock_nlp) as mock_spacy_load:
        texts = ["Testtext mit Aufgaben der."]
        normalized = TextTransformer.normalize(texts, nlp=None)  # nlp=None forces spacy.load

        # Keep only cleaned lemma
        assert normalized == [['aufgabe']]
        mock_spacy_load.assert_called_once_with("de_core_news_lg")

    #todo textlen_test

# ----- Pipeline and logic test -----


    #todo test_run_transfromation_pipeline_calls_steps_returns_dict
