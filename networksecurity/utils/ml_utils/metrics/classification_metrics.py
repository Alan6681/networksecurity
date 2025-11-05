from networksecurity.entity.artifact_entity import ClassficationMetricArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from sklearn.metrics import f1_score, precision_score, recall_score
import sys


def get_classification_score(y_true, y_preds):
    try:
        model_f1_score = f1_score(y_true, y_preds)
        model_recall_score = recall_score(y_true, y_preds)
        model_precision_score = precision_score(y_true, y_preds)

        classification_metrics = ClassficationMetricArtifact(f1_score=model_f1_score, 
        precision_score=model_precision_score,
        recall_score=model_recall_score)
        return classification_metrics
    except Exception as e:
        raise NetworkSecurityException(e,sys)