from src.config import get_config
from src.xmlprocessor import XMLProcessor
from src.texttransformer import TextTransformer
import logging

# Logging config
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("data_processing.log", mode="a"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

if __name__ == "__main__":

    main_logger = setup_logging()
    main_logger.info("--- Starting ---")

    try:
        cfg = get_config()

        main_logger.info(f"Initializing processor for raw data directory: {cfg.paths.raw_data_dir}")
        processor = XMLProcessor(config=cfg)

        meta_df = processor.run_metaparsing_pipeline()

        df_dict_raw = processor.run_occparsing_pipeline()

        if df_dict_raw:
            text_transformer = TextTransformer(config=cfg)

            df_dict_transformed = text_transformer.run_transformation_pipeline(df_dict_raw)

        # todo instantiate final data sanity checks and run sanity check pipeline

    except Exception as e:
        main_logger.critical(f"Critical error in main process: {e}")




