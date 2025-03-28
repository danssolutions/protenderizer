from analyzer.ml import detect_outliers
import numpy as np

def test_detect_outliers_simple_case():
    data = np.array([[1], [1], [100]])  # Obvious outlier
    outliers = detect_outliers(data)
    assert len(outliers) == 1
    assert outliers[0] == 2  # Index of outlier
