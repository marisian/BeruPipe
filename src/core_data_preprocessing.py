import spacy
import re

def clean_text(text: str) -> str:
    text = re.sub(r"[^a-zA-ZäöüßÄÖÜ ]+", "", str(text))
    return text
    
def normalize(
        list_of_texts: list[str],
        nlp = None
        ) -> list[list]:
    if nlp is None:
        nlp = spacy.load("de_core_news_lg")
    normalized_list = []
    for doc in nlp.pipe(list_of_texts):
        normalized_list.append(
            [tok.lemma_.lower() for tok in doc 
             if not (tok.is_punct or tok.is_stop 
                     or tok.is_digit or tok.is_space or tok.is_currency
                     or len(tok.text)<2)]
            )
    return normalized_list


    