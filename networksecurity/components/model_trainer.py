import sys 
import os
import pandas as pd
import numpy as np
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.artifact_entity import DataTransformationArtifacts, ModelTrainerArtifacts
from networksecurity.entity.config_entity import ModelTrainerConfig

from networksecurity.utils.ml_utils.model.estimator import NetworkModel
from networksecurity.utils.main_utils.utils import save_object, load_object, load_numpy_array, evaluate_models
from networksecurity.utils.ml_utils.metrics.classification_metrics import get_classification_score

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import r2_score
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import (
    AdaBoostClassifier,
    GradientBoostingClassifier,
    RandomForestClassifier

)
import mlflow



class ModelTrainer:
    def __init__(self, model_trainer_config:ModelTrainerConfig, data_tranformation_artifacts: DataTransformationArtifacts):
        try:
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifacts = data_tranformation_artifacts
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def track_mlflow(self, best_model, classification_metric):
        with mlflow.start_run():
            f1_score = classification_metric.f1_score
            precision_score = classification_metric.precision_score
            recall_score = classification_metric.recall_score

            mlflow.log_metric("F1_Score", f1_score)
            mlflow.log_metric("Precision_Score", precision_score)
            mlflow.log_metric("Recall_Score", recall_score)
            mlflow.sklearn.log_model(best_model, "model")
    
    def train_model(self, X_train, y_train,X_test, y_test):
        models = {

                "Random Forest" : RandomForestClassifier(verbose=1),
                "Decision Tree" : DecisionTreeClassifier(),
                "Gradient Boosting": GradientBoostingClassifier(verbose=1),
                "Logistic Regression" : LogisticRegression(verbose=1),
                "AdaBoost" : AdaBoostClassifier(),
        }
        params={

            "Decision Tree" : {
                'criterion' : ['gini', 'entropy', 'log_loss'],
                # 'spliter' : ['best', 'random'],
                # 'max_features' : ['sqrt', 'log2'],
            },

            "Random Forest" : {
                # 'criterion' : ['gini', 'entropy', 'log_loss'],
                # 'max_features' : ['sqrt', 'log2', None],
                'n_estimators' : [8, 16, 32, 64, 128, 256]
            },

            "Gradient Boosting" : {
            # 'loss' : ['log_loss', 'exponential'],
            'learning_rate': [.1,.01,.05,.001],
            'subsample' : [0.6, 0.7, 0.75, 0.8, 0.9],
             # 'criterion' : ['squared_error', 'friedman_mse'],
             # 'max_features' : ['auto', 'log2', sqrt],
            'n_estimators' : [8, 16, 32, 64, 128, 256]
            },

            "Logistic Regression" : {},
            "AdaBoost" : {
                'learning_rate' : [.1,.01,0.5,.001],
                'n_estimators' : [8,16,32,64,128,256]
            } 

        }
        model_report:dict = evaluate_models(X_train, y_train, X_test, y_test, models=models, param=params ) 

        ## To get best model score from dict
        best_model_score = max(sorted(model_report.values()))

        ## To get best model name
        best_model_name = list(model_report.keys())[
            list(model_report.values()).index(best_model_score)
        ]

        best_model= models[best_model_name]
        y_train_preds = best_model.predict(X_train)

        classification_train_metric = get_classification_score(y_true=y_train, y_preds=y_train_preds)
        
        ## Track the experiments with mlflow
        self.track_mlflow(best_model, classification_train_metric)

        y_test_preds = best_model.predict(X_test)
        classification_test_metric = get_classification_score(y_true=y_test, y_preds=y_test_preds)
        
        self.track_mlflow(best_model, classification_test_metric)

        preprocessor = load_object(file_path=self.data_transformation_artifacts.transformed_object_file_path)

        model_dir_path = os.path.dirname(self.model_trainer_config.trained_model_file_path)
        os.makedirs(model_dir_path, exist_ok=True)

        network_model = NetworkModel(preprocessor=preprocessor, model=best_model)
        save_object(self.model_trainer_config.trained_model_file_path, network_model)

        ## Model Trainer Artifact
        model_trainer_artifact =ModelTrainerArtifacts(
            trained_model_file_path=self.model_trainer_config.trained_model_file_path,
            train_metric_artifact=classification_train_metric, test_metric_artifact=classification_test_metric
        )

        logging.info(f"Model trianer artifact: {model_trainer_artifact}")
        return model_trainer_artifact


    def initiate_model_trainer(self) -> ModelTrainerArtifacts:
        try:
            train_file_path = self.data_transformation_artifacts.transformed_train_file_path
            test_file_path = self.data_transformation_artifacts.transformed_test_file_path

            #loading training array and testing array
            train_arr = load_numpy_array(train_file_path)
            test_arr = load_numpy_array(test_file_path)

            X_train,y_train,X_test,y_test = (
                train_arr[:, :-1],
                train_arr[:, -1],
                test_arr[:, :-1],
                test_arr[:, -1]
            )

            model_trainer_artifact = self.train_model(X_train, y_train, X_test, y_test)
            return model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)