from networksecurity.entity.artifact_entity import DataIngestionArtifacts, DataValidationArtifacts
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from scipy.stats import ks_2samp
import pandas as pd
import os, sys
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifacts,
        data_validation_config: DataValidationConfig
    ):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        """Reads a CSV file into a pandas DataFrame."""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """Validates if number of columns matches schema."""
        try:
            required_columns = len(self.schema_config["columns"])
            dataframe_columns = dataframe.shape[1]

            logging.info(f"Required number of columns: {required_columns}")
            logging.info(f"DataFrame has columns: {dataframe_columns}")

            return dataframe_columns == required_columns
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_required_columns(self, dataframe: pd.DataFrame) -> bool:
        """Ensures all required columns are present in dataframe."""
        try:
            required_columns = list(self.schema_config["columns"].keys())
            dataframe_columns = dataframe.columns
            missing_columns = [col for col in required_columns if col not in dataframe_columns]

            if missing_columns:
                logging.info(f"Missing columns: {missing_columns}")
                return False
            return True
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold=0.05) -> bool:
        """Checks for data drift between training and testing datasets."""
        try:
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]

                test_result = ks_2samp(d1, d2)
                p_value = float(test_result.pvalue)
                drift_found = p_value < threshold

                report[column] = {
                    "p_value": p_value,
                    "drift_status": drift_found
                }

                if drift_found:
                    status = False  # Drift detected in at least one column

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)

            write_yaml_file(file_path=drift_report_file_path, content=report)
            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifacts:
        """Main pipeline entry: validate schema, columns, and drift."""
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Read the train and test data
            train_df = self.read_data(train_file_path)
            test_df = self.read_data(test_file_path)

            # Validate number of columns
            if not self.validate_number_of_columns(train_df):
                raise Exception("Number of columns do not match schema for training data.")
            if not self.validate_number_of_columns(test_df):
                raise Exception("Number of columns do not match schema for testing data.")

            # Validate required columns
            if not self.validate_required_columns(train_df):
                raise Exception("Required columns missing in training data.")
            if not self.validate_required_columns(test_df):
                raise Exception("Required columns missing in testing data.")

            # Detect dataset drift
            validation_status = self.detect_dataset_drift(base_df=train_df, current_df=test_df)

            # Save valid datasets
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_df.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)

            # Prepare artifact
            data_validation_artifact = DataValidationArtifacts(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data validation completed successfully: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)
