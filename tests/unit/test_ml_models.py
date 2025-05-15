# from analyzer.ml import detect_outliers
# import pandas as pd
# import pytest

# # test for extremely high value (clearly an outlier)


# def test_high_outlier():
#     df = pd.DataFrame({
#         'TVH': [500000, 520000, 510000, 505000, 9800000],
#         'duration-period-value-lot': [12, 12, 12, 12, 12]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])

#     assert 'outlier' in result.columns
#     assert result.loc[df['TVH'] == 9800000, 'outlier'].iloc[0] == -1
#     assert all(result.loc[df['TVH'] < 1000000, 'outlier'] == 1)
# # low within high values


# def test_low_outlier():
#     df = pd.DataFrame({
#         'TVH': [500000, 520000, 510000, 505000, 10000],
#         'duration-period-value-lot': [12, 12, 12, 12, 12]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])
#     assert 'outlier' in result.columns
#     assert result.loc[4, 'outlier'] == 1, "Low-value outlier not detected."


# def test_high_and_low_outliers():
#     df = pd.DataFrame({
#         'TVH': [10000, 500000, 510000, 520000, 9800000],
#         'duration-period-value-lot': [12, 12, 12, 12, 12]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])
#     assert 'outlier' in result.columns
#     assert result.loc[0, 'outlier'] == 1, "Low-value outlier not detected."
#     assert result.loc[4, 'outlier'] == 1, "High-value outlier not detected."

# # Outlier column values are valid


# def test_outlier_column_has_valid_labels():
#     df = pd.DataFrame({
#         'TVH': [100000, 200000, 300000, 400000, 10000000],
#         'duration-period-value-lot': [6, 6, 6, 6, 6]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])
#     assert set(result['outlier'].unique()).issubset({-1, 1})


# # Test for empty df raise error
# def test_empty_dataframe_raises_error():
#     df = pd.DataFrame(columns=['TVH', 'duration-period-value-lot'])
#     with pytest.raises(ValueError):
#         detect_outliers(df, ['TVH', 'duration-period-value-lot'])


# # Test invalid name/call raise error
# def test_invalid_feature_column_raises_key_error():
#     df = pd.DataFrame({
#         'TVH': [100000, 200000, 300000],
#         'duration-period-value-lot': [6, 6, 6]
#     })
#     with pytest.raises(KeyError):
#         detect_outliers(df, ['nonexistent_feature'])


# # Test no clear outlier
# def test_all_inliers_with_uniform_data():
#     df = pd.DataFrame({
#         'TVH': [100000, 100001, 99999, 100002, 99998],
#         'duration-period-value-lot': [12, 12, 12, 12, 12]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])
#     outliers = result['outlier'].value_counts().to_dict()

#     # false positive avoidance (Chatty told me to use it)
#     assert outliers.get(-1, 0) <= 1


# # test shape & % (chatty made me do it )
# def test_output_dataframe_structure():
#     df = pd.DataFrame({
#         'TVH': [100000, 200000, 500000, 900000],
#         'duration-period-value-lot': [6, 12, 24, 48]
#     })
#     result = detect_outliers(df.copy(), ['TVH', 'duration-period-value-lot'])
#     assert result.shape == (4, 3)  # original 2 cols + 1 'outlier'
#     assert list(result.columns) == [
#         'TVH', 'duration-period-value-lot', 'outlier']
