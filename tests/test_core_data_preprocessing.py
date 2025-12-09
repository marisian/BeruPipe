from src.core_data_preprocessing import clean_text, normalize

def test_clean_text():
    expectation = "Theße are not välid"
    input = "Theße? are not välid:?!;*&%"
    result = clean_text(input)
    assert result == expectation, \
        f"Some characters not removed as expected: {expectation}"

def test_normalize_list_of_texts_returns_expected():
    # todo refine normalization test with mock objects
    expectation = [["one", "text"], ["two", "text"], ["three"]]
    input = ["ONE text ", "Two Texts", "Three"]
    result = normalize(input)
    assert result == expectation, \
        f"Output of normalization not as expected"

