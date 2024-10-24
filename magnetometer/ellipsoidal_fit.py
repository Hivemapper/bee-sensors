"""Plot mag data

"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def main():
    # Load data
    # TEST_TYPE = "patio"
    # TEST_TYPE = "inside"

    for TEST_TYPE in ["patio", "inside"]:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        if TEST_TYPE == "patio":
            mag_data = pd.read_csv("/data/redis_handler/20241017_2_mag_flips/magnetometer.csv")
        elif TEST_TYPE == "inside":
            mag_data = pd.read_csv("/data/redis_handler/20241017_3_mag_flips_inside/magnetometer.csv")
        mag_data = mag_data.dropna()
        mag_data = mag_data.reset_index(drop=True)

        # Plot 3D data
        ax.scatter(mag_data["mag_x"], mag_data["mag_y"], mag_data["mag_z"],label=TEST_TYPE)
        ax.set_xlabel("mag_x")
        ax.set_ylabel("mag_y")
        ax.set_zlabel("mag_z")
        
        if TEST_TYPE == "patio":
            plt.title("On Patio away from Electronics")
        elif TEST_TYPE == "inside":
            plt.title("Inside next to Computers")
    # ax.legend()
    plt.show()


if __name__ == "__main__":
    main()
 
