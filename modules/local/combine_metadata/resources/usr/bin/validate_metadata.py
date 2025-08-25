#!/usr/bin/env python3

import os
import sys
import json
import glob
import logging
import argparse
from colored_logger import setup_logging
import pandas as pd


def init_parser() -> argparse.ArgumentParser:
    """
    Initialise argument parser for the script
    """

    parser = argparse.ArgumentParser(
        description="Reads metadata from a set of .json files and combines everything to a .tsv file"
    )
    parser.add_argument(
        "input",
        metavar="file",
        type=str,
        help="specify a path to json file with metadata you want to validate",
    )
    parser.add_argument(
        "-s",
        "--sample_column",
        help="specify the name of the sample column",
        type=str,
        default="sample",
    )
    parser.add_argument(
        "-c",
        "--cram_column",
        help="specify the name of the cram path column",
        type=str,
        default="cram_path",
    )
    parser.add_argument(
        "-p",
        "--prefix_column",
        help="specify the name of the file prefix column",
        type=str,
        default="fastq_prefix",
    )
    parser.add_argument(
        "--check_duplicated_prefix",
        help="checks that there are no duplicated file prefixes in the metadata",
        action="store_true",
    )
    parser.add_argument(
        "--check_readcounts",
        help="checks that read counts mentioned in metadata match actual read counts",
        action="store_true",
    )
    parser.add_argument(
        "--check_library_types",
        help="checks that library types are the same for each sample",
        action="store_true",
    )
    parser.add_argument(
        "--check_readlengths",
        help="checks that read lengths are the same for each sample",
        action="store_true",
    )
    parser.add_argument(
        "--logfile",
        metavar="<file>",
        type=str,
        help="Specify a log file name",
        default="combine_metadata.log",
    )
    parser.add_argument(
        "--output",
        metavar="<file>",
        type=str,
        help="Specify a name for output file",
        default="metadata.tsv",
    )
    parser.add_argument(
        "--sep",
        metavar="<char>",
        type=str,
        help="Specify a separator for the output file",
        default=",",
    )
    return parser


def validate_columns_match(
    metadata: pd.DataFrame,
    col1: str,
    col2: str,
    sample_column: str = "sample",
    cram_column: str = "cram_path",
):
    """
    Validate that values in two columns match for entry
    Args:
        metadata (pd.DataFrame): The metadata DataFrame
        col1 (str): The name of the first column to compare
        col2 (str): The name of the second column to compare
    """
    mismatches = metadata[metadata[col1] != metadata[col2]].copy()
    if not mismatches.empty:
        mismatched_files = "\n".join(
            f"{row[sample_column]}: {row[cram_column]}"
            for row in mismatches.to_dict("records")
        )
        logging.warning(
            "Mismatches found between columns '%s' and '%s' for cram files: %s",
            col1,
            col2,
            mismatched_files,
        )


def validate_multiple_values_column(
    metadata: pd.DataFrame,
    column: str,
    sample_column: str = "sample",
):
    """
    Validate that values in a specified column are consistent for each sample
    Args:
        metadata (pd.DataFrame): The metadata DataFrame
        column (str): The name of the column to check for consistency
    """
    unique_counts = (
        metadata.groupby(sample_column)
        .agg(
            n=pd.NamedAgg(column=column, aggfunc="nunique"),
            values=pd.NamedAgg(column=column, aggfunc=lambda x: ",".join(x)),
        )
        .reset_index()
    )
    inconsistent_samples = unique_counts[unique_counts["n"] > 1]
    if not inconsistent_samples.empty:
        inconsistent_values = "\n".join(
            f"{row[sample_column]}: {row['values']}"
            for row in inconsistent_samples.to_dict("records")
        )
        logging.warning(
            "There are multiple values found in column '%s' for samples: %s",
            column,
            inconsistent_values,
        )


def validate_duplicated_column(
    metadata: pd.DataFrame,
    column: str,
):
    """
    Validate that a specified column does not contain duplicated values.
    Args:
        metadata (pd.DataFrame): The metadata DataFrame
        column (str): The name of the column to check for duplicates
    """
    duplicated = metadata[column].duplicated()
    if duplicated.any():
        duplicated_values = metadata.loc[duplicated, column].unique()
        logging.warning(
            "There are duplicated values found in column '%s': %s",
            column,
            ", ".join(duplicated_values),
        )


def main() -> None:
    # parse arguments
    parser = init_parser()
    args = parser.parse_args()

    # set up logging
    setup_logging(args.logfile)
    logging.info("validate_metadata.py called with arguments: %s", args)
    logging.info("Starting metadata validation")

    # check if metadata file exists
    if not os.path.exists(args.input):
        logging.error("File %s not found", args.input)
        raise FileNotFoundError(f"File {args.input} not found")

    # read metadata
    metadata = pd.read_json(args.input, orient="records", lines=False)

    # Validate metadata
    if args.check_readcounts:
        validate_columns_match(
            metadata,
            "total_reads",
            "num_reads_processed",
            args.sample_column,
            args.cram_column,
        )
    if args.check_library_types:
        validate_multiple_values_column(
            metadata,
            "library_type",
            args.sample_column,
        )
    if args.check_readlengths:
        validate_multiple_values_column(
            metadata,
            "r1len",
            args.sample_column,
        )
        validate_multiple_values_column(
            metadata,
            "r2len",
            args.sample_column,
        )
    if args.check_duplicated_prefix:
        validate_duplicated_column(
            metadata,
            args.prefix_column,
        )

    # Save validation results
    metadata.to_csv(args.output, sep=args.sep, index=False)


if __name__ == "__main__":
    main()
