"""
This module provides functionalities for reading, sorting, and verifying
Flinkster carsharing data in CSV format.

The MIT License (MIT)

Copyright (c) 2023 Marvin Teichmann
Email: marvin.teichmann@googlemail.com

The above author notice shall be included in all copies or
substantial portions of the Software.

It is written in Python 3.8 and tested under Linux.
"""

import os
import sys
import logging
import pandas as pd
from datetime import timedelta

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)


def combine_and_verify_booking_data(
    csv_files, encoding="ISO-8859-1", delimiter=";"
):
    """
    This function combines booking data from a list of CSV files, sorts the
    data, and then performs checks on overlapping bookings and time
    differences between bookings.

    The function assumes that the CSV file names are in chronological order
    of the bookings, i.e., when sorted lexicographically, the files represent
    the chronological order of the bookings.

    Ideally, the function assumes minor overlap between consecutive booking
    data or that the bookings in the CSV files are back-to-back, i.e., the
    last booking in one CSV file is directly followed by the first booking in
    the next CSV file without any gap in between.

    Parameters:
    csv_files (list): A list of CSV files containing booking data.
    encoding (str): Encoding type of the CSV files. Defaults to "ISO-8859-1".
    delimiter (str): Delimiter used in the CSV files. Defaults to ";".

    Returns:
    df_list_sorted (list): A list of sorted dataframes containing booking data.
    csv_files_sorted (list): A list of sorted CSV files.
    """
    # Load and sort Fahrten data
    df_list_sorted, csv_files_sorted = load_and_sort_fahrten_data(
        csv_files, encoding, delimiter
    )

    # Verify overlapping bookings
    verify_overlapping_bookings(df_list_sorted)

    # Assert time difference in bookings
    assert_time_difference_in_bookings(df_list_sorted, csv_files_sorted)

    return merge_nonoverlapping_dfs(df_list_sorted)


def merge_nonoverlapping_dfs(df_list_sorted):
    """
    This function combines a list of sorted pandas DataFrame objects, ensuring
    that there are no duplicated records due to overlaps.

    The function assumes that the DataFrames in 'df_list_sorted' are already
    sorted by the column "anfang" (beginning) in ascending order and that they
    may contain overlaps. It removes the overlaps by only including records in
    each DataFrame that have a timestamp greater than the maximum timestamp in
    the previous DataFrame.

    Parameters
    ----------
    df_list_sorted : list of pandas.DataFrame
        The sorted list of pandas DataFrames to be combined. Each DataFrame
        should have a column named "anfang" representing timestamps.

    Returns
    -------
    pandas.DataFrame
        The merged DataFrame without overlaps.

    Raises
    ------
    ValueError
        If 'df_list_sorted' is not a list or if any element in 'df_list_sorted'
        is not a pandas DataFrame.
    """

    # Ensure 'df_list_sorted' is a list and all its elements are pandas DataFrame objects
    if not isinstance(df_list_sorted, list) or not all(
        isinstance(df, pd.DataFrame) for df in df_list_sorted
    ):
        raise ValueError(
            "'df_list_sorted' should be a list of pandas DataFrame objects"
        )

    master_df = df_list_sorted[0]

    for i in range(1, len(df_list_sorted)):
        last_date_master_df = master_df["anfang"].max()

        # Include only those records in the current DataFrame that have a timestamp greater
        # than the maximum timestamp in the previous (master) DataFrame
        new_data = df_list_sorted[i][
            df_list_sorted[i]["anfang"] > last_date_master_df
        ]

        master_df = pd.concat([master_df, new_data])

    return master_df


def verify_overlapping_bookings(df_list_sorted):
    """
    This function verifies whether there are any overlapping bookings
    within the list of sorted dataframes.

    Parameters:
    df_list_sorted (list): A list of sorted dataframes.

    Returns:
    None
    """
    for i in range(len(df_list_sorted) - 1):
        last_date_current_df = df_list_sorted[i]["anfang"].max()
        first_date_next_df = df_list_sorted[i + 1]["anfang"].min()

        if last_date_current_df >= first_date_next_df:
            overlap_current_df = df_list_sorted[i][
                df_list_sorted[i]["anfang"] >= first_date_next_df
            ]
            overlap_next_df = df_list_sorted[i + 1][
                df_list_sorted[i + 1]["anfang"] <= last_date_current_df
            ]

            # Check if overlapping dataframes have the same length
            assert_msg_len = f"The overlapping bookings have different lengths between files {i} and {i+1}"
            assert len(overlap_current_df) == len(
                overlap_next_df
            ), assert_msg_len

            # Reset index for comparison
            overlap_current_id = overlap_current_df.reset_index()[
                "fahrtneu_id"
            ]
            overlap_next_id = overlap_next_df.reset_index()["fahrtneu_id"]

            # Check if 'fahrtneu_id' values are the same in both dataframes
            assert_msg_id = f"The overlapping bookings do not align between files {i} and {i+1}"
            assert set(overlap_current_id) == set(
                overlap_next_id
            ), assert_msg_id


def assert_time_difference_in_bookings(
    df_list_sorted, csv_files_sorted, threshold_hours=6
):
    """
    This function asserts that the time difference between bookings in
    the list of sorted dataframes does not exceed a certain threshold.

    Parameters:
    df_list_sorted (list): A list of sorted dataframes.
    csv_files_sorted (list): A list of sorted CSV files.
    threshold_hours (int): Maximum allowed time difference in hours.

    Returns:
    None
    """
    for i in range(len(df_list_sorted) - 1):
        last_date_current_df = pd.to_datetime(
            df_list_sorted[i]["anfang"].max()
        )
        first_date_next_df = pd.to_datetime(
            df_list_sorted[i + 1]["anfang"].min()
        )

        if not last_date_current_df >= first_date_next_df:
            time_diff = first_date_next_df - last_date_current_df
            assert_msg = (
                f"No overlap and time difference greater than {threshold_hours} "
                f"hours between {csv_files_sorted[i]} and {csv_files_sorted[i+1]}"
            )
            assert time_diff <= timedelta(hours=threshold_hours), assert_msg
            logging.warning(
                f"No overlap between {csv_files_sorted[i]} and {csv_files_sorted[i+1]}.\n"
                f"Last date: {last_date_current_df}\n"
                f"First date: {first_date_next_df}"
            )


def load_and_sort_fahrten_data(
    csv_files, encoding="ISO-8859-1", delimiter=";"
):
    """
    This function reads the Fahrten data from a list of CSV files and sorts
    the dataframes by 'fahrt_anfang'.

    Parameters:
    csv_files (list): A list of CSV files.
    encoding (str): Encoding type of the CSV files.
    delimiter (str): Delimiter used in the CSV files.

    Returns:
    df_list_sorted (list): A list of sorted dataframes.
    csv_files_sorted (list): A list of sorted CSV files.
    """
    csv_files_sorted = sorted(csv_files)
    df_list_sorted = [
        pd.read_csv(f, encoding=encoding, delimiter=delimiter)
        for f in csv_files_sorted
    ]
    for df in df_list_sorted:
        df["anfang"] = pd.to_datetime(df["anfang"])
        df["ende"] = pd.to_datetime(df["ende"])
        df["fahrt_anfang"] = pd.to_datetime(df["fahrt_anfang"])
        df["fahrt_ende"] = pd.to_datetime(df["fahrt_ende"])

    return df_list_sorted, csv_files
