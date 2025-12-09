import pickle
from src.config import get_config
from src.utils import start_logger
from src.core_data_preprocessing import (
    normalize,
    clean_text
)
from tests.plausibility import (
    plausibility_transformed_occ_data,
    plausibility_transformed_task_data
    )

logger = start_logger()

def transformation_pipeline(config):
    # Input: parsed occs, parsed meta
    logger.info("---Started core data transformation pipeline---")
    
    # Load data
    with open(config.paths.intermediate_data_dir / "bfield_dict.pkl", "rb") as f:
        df_dict = pickle.load(f)

    for bfield, df in df_dict.items():
        transform_col = f"{bfield}_text"
        clean_col = f"{transform_col}_clean"
        norm_col = f"{clean_col}_norm"
        len_col = f"{norm_col}_len"
        logger.info(f"Started text transformation loop ({bfield}-level)")

        # Drop N/A
        r_before = df.shape[0]
        df = df.dropna(subset=transform_col)
        logger.info(
            f"{bfield}: Dropped {r_before - df.shape[0]} rows missing {transform_col} values"
            )

        # Preprocess text
        df[clean_col] = df[transform_col].apply(clean_text)
        logger.info(f"{bfield}: Cleaned column '{transform_col}' --> '{clean_col}'")

        texts = df[clean_col].astype(str).tolist()
        df[norm_col] = normalize(texts)
        logger.info(f"{bfield}: Normalized column '{clean_col}' --> '{norm_col}'")

        df[len_col] = df[norm_col].apply(lambda x: len(x))
        logger.info(f"{bfield}: Counted normalized words per task --> '{len_col}'")
        logger.info(df.columns)

        # Sanity checks
        #logger.info("Running sanity checks")
        #plausibility_transformed_occ_data(df_occ, transform_col)
        #logger.info("Sanity checks completed")

        # Save transformed data
        filename_csv = f"{bfield}.csv"
        filename_pkl = f"{bfield}.pkl"
        df.to_csv(config.paths.processed_data_dir / filename_csv, na_rep="NA")
        df.to_pickle(config.paths.processed_data_dir / filename_pkl)
        logger.info(f"Saved transformed data")

    logger.info(f"---Completed core data transformation pipeline---")

    return df_dict

if __name__ == "__main__":
    cfg = get_config()
    df_dict = transformation_pipeline(cfg)
    import pandas as pd

