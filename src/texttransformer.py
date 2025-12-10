import re
import logging
from typing import Dict
import pandas as pd
import spacy

class TextTransformer:
    def __init__(
            self,
            config
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._config = config

    def run_transformation_pipeline(
            self,
            df_dict: Dict[str, pd.DataFrame],
            save=True
    ):
        self.logger.info("---Started text transformation pipeline---")

        for b_field, df_original in df_dict.items():
            self.logger.info(f"Starting text transformations for b-field: {b_field}")

            # copy df
            df_working = df_original.copy()

            # drop n/a
            df_working = self._dropna(df_working, b_field)

            # text cleaning
            df_working = self._clean_text_columns(df_working, b_field)

            # normalization
            df_working = self._normalize_columns(df_working, b_field)

            # text length
            df_working = self._textlen(df_working, b_field)

            if save:
                self._save_df(df_working, b_field)
                self.logger.info(f"Saved transformed data")

            df_dict[b_field] = df_working
        self.logger.info(f"Completed text transformation pipeline")
        return df_dict

    def _clean_text_columns(
            self,
            df: pd.DataFrame,
            b_field: str
    ) -> pd.DataFrame:
        transform_col = f"{b_field}_text"
        if transform_col in df.columns:
            df.loc[:, transform_col] = (
                df[transform_col]
                .astype(str)
                .apply(lambda text: re.sub(r"[^a-zA-ZäöüßÄÖÜ ]+", "", text))
            )
            self.logger.info(f"Cleaned text in {transform_col}")
        return df

    def _normalize_columns(
            self,
            df: pd.DataFrame,
            b_field: str
    ) -> pd.DataFrame:
        transform_col = f"{b_field}_text"
        norm_col = f"{b_field}_normalized"
        if transform_col in df.columns:
            texts = df[transform_col].astype(str).tolist()
            df[norm_col] = self.normalize(texts)
            self.logger.info(f"Normalized column '{transform_col}' --> '{norm_col}'")
        return df
    #todo fix SettingWithCopyWarning

    @staticmethod
    def normalize(
            list_of_texts: list[str],
            nlp=None
    ) -> list[list]:
        if nlp is None:
            nlp = spacy.load("de_core_news_lg")
        normalized_list = []
        for doc in nlp.pipe(list_of_texts):
            normalized_list.append(
                [tok.lemma_.lower() for tok in doc
                 if not (tok.is_punct or tok.is_stop
                         or tok.is_digit or tok.is_space or tok.is_currency
                         or len(tok.text) < 2)]
            )
        return normalized_list

    def _textlen(
            self,
            df: pd.DataFrame,
            b_field: str
    ) -> pd.DataFrame:
        len_col = f"{b_field}_len"
        if f"{b_field}_normalized" in df.columns:
            df[len_col] = df[f"{b_field}_normalized"].apply(lambda x: len(x))
        return df

    def _dropna(
            self,
            df: pd.DataFrame,
            b_field: str
    ):
        r_before = df.shape[0]
        df = df.dropna(subset=f"{b_field}_text")
        self.logger.info(
            f"Dropped {r_before - df.shape[0]} rows missing '{b_field}_text' values"
        )
        return df

    def _save_df(
            self,
            df: pd.DataFrame,
            b_field: str
    ):
        filename_csv = f"{b_field}.csv"
        filename_pkl = f"{b_field}.pkl"
        df.to_csv(self._config.paths.processed_data_dir / filename_csv, na_rep="NA")
        df.to_pickle(self._config.paths.processed_data_dir / filename_pkl)

