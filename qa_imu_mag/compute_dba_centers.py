"""Computes Dynamic Time Warping (DTW) barycenter averaging centers.

Assumes data was created using the preprocess_stat_data_for_averaging.py script.

"""

__author__ = "D. Knowles"
__date__ = "20 Mar 2025"

import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dtaidistance import dtw_barycenter

class AverageTest():

    def __init__(self, dataset_dir):
        self.dataset_dir = dataset_dir
        self.data = {
                     "Station1" : {},
                     "Station2" : {},
                     "Station3" : {},
                    }
        self.centers = {
                        "Station1" : {},
                        "Station2" : {},
                        "Station3" : {},
                       }
        self.data_cols = ["acc_x", "acc_y", "acc_z", "gyro_x", "gyro_y", "gyro_z", "mag_x", "mag_y", "mag_z"]
        self.load_dataset()
        self.compute_centers()
        self.save_centers()
        self.plot_dataset(plot_centers=True)

        plt.show()

    def compute_centers(self):
         for station in self.data.keys():
            for col_idx, col in enumerate(self.data_cols):
                print(f"Computing center for {station} {col}")
                center = self.compute_dtw_barycenter(station, col)
                self.centers[station][col] = center

    def compute_dtw_barycenter(self, station, col):

        series = self.data[station][col].values.T
        new_center = dtw_barycenter.dba_loop(series, max_it=20, thr=0.0001, use_c=True)

        return new_center

    def save_centers(self):
        for station in self.centers.keys():
            center_filename = os.path.join(self.dataset_dir, station, f"dba_centers_{station.lower()}.csv")
            df = pd.DataFrame(dict([(key, pd.Series(value)) for key, value in self.centers[station].items()]) )
            df.to_csv(center_filename, index=False)
    
    def load_dataset(self):
        for station in sorted(os.listdir(self.dataset_dir)):
            print(station)
            for col in self.data_cols:
                self.data[station][col] = pd.read_csv(os.path.join(self.dataset_dir, station, col + ".csv"))

    def plot_dataset(self, stations=None, cols=None, plot_centers=False):
        
        if stations is None:
            stations = self.data.keys()
        if cols is None:
            cols = self.data_cols

        fig_centers, axes_centers = plt.subplots(len(cols), 1, figsize=(18, 10))
        for station in stations:
            fig_dataset, axes_dataset = plt.subplots(len(cols), 1, figsize=(18, 10))
            if len(cols) == 1:
                axes_dataset = [axes_dataset]
            else:
                axes_dataset = axes_dataset.flatten()
            plt.suptitle(f"Station {station}")

            for col_idx, col in enumerate(cols):
                # plot all columns in one figure
                self.data[station][col].plot(y=self.data[station][col].columns, 
                                             ax=axes_dataset[col_idx], 
                                             alpha=0.3,
                                             legend=False)
                axes_dataset[col_idx].set_title(col)
                axes_dataset[col_idx].set_ylabel("Value")
                if plot_centers:
                    center = self.centers[station][col]
                    axes_dataset[col_idx].plot(np.arange(len(center)), center, color="red")
                    axes_centers[col_idx].plot(np.arange(len(center)), center, label=station)
                    axes_centers[col_idx].set_title(col)
                    axes_centers[col_idx].set_ylabel("Value")
        
            fig_dataset.tight_layout()
        fig_centers.tight_layout()
            

        axes_centers[0].legend()
        

if __name__ == "__main__":
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    avg = AverageTest(dataset_path)