"""
The MIT License (MIT)

Copyright (c) 2023 Marvin Teichmann
Email: marvin.teichmann@googlemail.com

The above author notice shall be included in all copies or
substantial portions of the Software.

This file is written in Python 3.8 and tested under Linux.
"""

import os
import sys

import numpy as np
import scipy as scp

import logging

import pandas as pd
import glob

from datetime import timedelta

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)


def check_overlaps(df_list_sorted):
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
            assert len(overlap_current_df) == len(
                overlap_next_df
            ), f"The overlapping bookings have different lengths between files {i} and {i+1}"

            # Reset index for comparison
            overlap_current_id = overlap_current_df.reset_index()[
                "fahrtneu_id"
            ]
            overlap_next_id = overlap_next_df.reset_index()["fahrtneu_id"]

            # Check if 'fahrtneu_id' values are the same in both dataframes
            assert set(overlap_current_id) == set(
                overlap_next_id
            ), f"The overlapping bookings do not align between files {i} and {i+1}"


def check_overlaps_with_time(
    df_list_sorted, csv_files_sorted, threshold_hours=6
):
    for i in range(len(df_list_sorted) - 1):
        last_date_current_df = pd.to_datetime(
            df_list_sorted[i]["anfang"].max()
        )
        first_date_next_df = pd.to_datetime(
            df_list_sorted[i + 1]["anfang"].min()
        )

        if not last_date_current_df >= first_date_next_df:
            time_diff = first_date_next_df - last_date_current_df
            assert time_diff <= timedelta(
                hours=threshold_hours
            ), f"No overlap and time difference greater than {threshold_hours} hours between {csv_files_sorted[i]} and {csv_files_sorted[i+1]}"
            logging.warning(
                f"No overlap between {csv_files_sorted[i]} and {csv_files_sorted[i+1]}.\n"
                f"Last date: {last_date_current_df}\n"
                f"First date: {first_date_next_df}"
            )


def read_csvs_sorted(csv_files):
    csv_files_sorted = sorted(csv_files)
    # Read and sort dataframes by 'fahrt_anfang'
    df_list_sorted = [
        pd.read_csv(f, encoding="ISO-8859-1", delimiter=";")
        for f in csv_files_sorted
    ]
    for df in df_list_sorted:
        df["anfang"] = pd.to_datetime(df["anfang"])
        df["ende"] = pd.to_datetime(df["ende"])
        df["fahrt_anfang"] = pd.to_datetime(df["fahrt_anfang"])
        df["fahrt_ende"] = pd.to_datetime(df["fahrt_ende"])

    return df_list_sorted, csv_files_sorted


if __name__ == "__main__":
    logging.info("Hello World.")
