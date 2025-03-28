def test_outlier_detection_speed(benchmark):
    from src.ml import detect_outliers
    import numpy as np
    data = np.random.rand(1000, 10)

    benchmark(lambda: detect_outliers(data))
