"""Compare sensor data outputs.

"""

__authors__ = "D. Knowles"
__date__ = "03 Feb 2024"

import os
import argparse

import numpy as np
import pandas as pd

def main(data_dir_1, data_dir_2):
    """Main run function.

    Parameters
    ----------
    data_dir_1 : string
        First directory of CSV files.
    data_dir_2 : string
        Second directory of CSV files.

    """

    itow_ms_tables = ["nav_pvt","nav_cov", "nav_posecef", "nav_status", "nav_timegps", "nav_velecef"]

    for file1 in os.listdir(data_dir_1):
        if not os.path.exists(os.path.join(data_dir_2,file1)):
            print(file1,"not in",data_dir_2)
            continue
        
        csv1_filepath = os.path.join(data_dir_1,file1)
        csv2_filepath = os.path.join(data_dir_2,file1)

        df1 = prep_df(csv1_filepath)
        df2 = prep_df(csv2_filepath)

        if file1[:-4] in itow_ms_tables:
            df1 = df1[df1["itow_ms"].isin(df2["itow_ms"])]
            df1.reset_index(drop=True, inplace=True)

        if df1.shape == df2.shape:
            compared = df1.compare(df2)
            if compared.empty:
                print(file1, "files are identical.")
            else:
                print("[WARNING]",file1, "files different.")
                print(compared)
        else:
            print("[WARNING]",df1.shape,df2.shape)
            print(len(np.unique(df1["itow_ms"].to_numpy())),
                  len(np.unique(df2["itow_ms"].to_numpy())))

def prep_df(csv_filepath):
    """Prepare a dataframe from a CSV file.

    Parameters
    ----------
    csv_filepath : string
        Filepath to the CSV file.

    Returns
    -------
    df : pandas dataframe
        Dataframe of the CSV file.

    """
    df = pd.read_csv(csv_filepath)
    if "session" in df.columns:
        df.drop("session", axis=1, inplace=True)
    if "session_id" in df.columns:
        df.drop("session_id", axis=1, inplace=True)
    if "id" in df.columns:
        df.drop("id", axis=1, inplace=True)
    if "name" in df.columns:
        df.sort_values(by="name", inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df

def setup_parser():
    """Extract command line arguments.

    Returns
    -------
    cmd_args : list
        List of all command line arguments.

    """
    parser = argparse.ArgumentParser(description='Compare two directories of csv files.')
    parser.add_argument("--data_dir_1", type=str, default="", help="First data directory")
    parser.add_argument("--data_dir_2", type=str, default="", help="Second data directory")
    cmd_args = parser.parse_args()

    return cmd_args

if __name__ == "__main__":
    parser = setup_parser()

    if parser.data_dir_1 == "" or parser.data_dir_2 == "":
        print("Examlpe use: python3 compare_replayed_data.py --data_dir_1 ~/original_data/ --data_dir_2 ~/replayed_data/")
    else:
        main(parser.data_dir_1, parser.data_dir_2)
