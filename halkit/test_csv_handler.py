"""
The MIT License (MIT)

Copyright (c) 2023 Marvin Teichmann
Email: marvin.teichmann@googlemail.com

The above author notice shall be included in all copies or
substantial portions of the Software.

It is written in Python 3.8 and tested under Linux.
"""


import os
import glob
import pandas as pd
import pytest
from halkit import csv_handler


def test_load_and_sort_fahrten_data():
    root_dir = "../data/"
    csv_files = glob.glob(root_dir + "*Fahrten_CarSharing_Erlangen*.csv")

    df_list_sorted, csv_files_sorted = csv_handler.load_and_sort_fahrten_data(
        csv_files
    )

    import ipdb  # NOQA

    ipdb.set_trace()
    pass

    # Check if dataframes are sorted correctly
    for i in range(len(df_list_sorted) - 1):
        assert (
            df_list_sorted[i]["anfang"].max()
            <= df_list_sorted[i + 1]["anfang"].min()
        )

    # Check if csv_files_sorted is correctly sorted
    assert csv_files_sorted == sorted(csv_files)


def test_check_fahrten_overlaps():
    root_dir = "../data/"
    csv_files = glob.glob(root_dir + "*Fahrten_CarSharing_Erlangen*.csv")

    df_list_sorted, csv_files_sorted = csv_handler.load_and_sort_fahrten_data(
        csv_files
    )

    # Ensure no AssertionError is raised
    csv_handler.check_fahrten_overlaps(df_list_sorted)


def test_assert_time_difference_in_bookings():
    root_dir = "../data/"
    csv_files = glob.glob(root_dir + "*Fahrten_CarSharing_Erlangen*.csv")

    df_list_sorted, csv_files_sorted = csv_handler.load_and_sort_fahrten_data(
        csv_files
    )

    # Ensure no AssertionError is raised
    csv_handler.assert_time_difference_in_bookings(
        df_list_sorted, csv_files_sorted
    )


def test_process_past_bookings():
    root_dir = "../data/"
    csv_files = glob.glob(root_dir + "*Fahrten_CarSharing_Erlangen*.csv")

    # Ensure no AssertionError is raised
    csv_handler.process_past_bookings(csv_files)


if __name__ == "__main__":
    test_load_and_sort_fahrten_data()
