'''Bandpass filter demo for IMU data.

The following python modules will need to be downloaded:
numpy, scipy, pyqtgraph, PyQt5(Python3)

'''

__author__ = "D. Knowles"
__data__ = "07 Oct 2024"

# import modules
import os
import sys
import csv

import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt
import pyqtgraph as pg
from pyqtgraph.Qt import QtGui, QtCore
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QMainWindow, QLabel, QGridLayout, QWidget, QComboBox
from PyQt5.QtCore import QSize, QRect 

#+++++++++++++++++++ VALUES TO CHANGE +++++++++++++++++++++++++++++
data_dir = "/Users/derekknowles/personal-data/imu_data/"
id = "1727691609083"
session = "5d4b3fc3"     # File to read
# Variables
fs = 1/0.625e-5     # sampling frequency
low_cutoff = 500        # initial low cutoff value
high_cutoff = 3000       # initial cutoff value
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Filtering Functions
def butter_highpass(cutoff, fs, order=5, btype='bandpass'):
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    print("norm:",cutoff)
    b, a = butter(order, Wn=normal_cutoff, btype=btype, analog=False)
    return b, a

def butter_highpass_filter(data, low_cutoff, high_cutoff, fs, order=5, btype='bandpass'):
    if high_cutoff == None:
        b, a = butter_highpass(np.array([low_cutoff]), fs, order=order, btype='lowpass')
    else:
        b, a = butter_highpass(np.array([low_cutoff, high_cutoff]), fs, order=order, btype=btype)
    y = filtfilt(b, a, data)
    return y

# Extract data from variables
time = []           # time data
output1_x = []        # mic pickup

df_imu = pd.read_csv(os.path.join(data_dir, id + "_"+ session , id + "_"+ session + "_imu.csv"))
df_imu['time'] = pd.to_datetime(df_imu['time'], format='mixed')
time_ms = (df_imu['time'].astype(np.int64) / 10**9)
time_ms = (time_ms - time_ms[0]).to_list()
time = time_ms
output1_x = df_imu['acc_x'].to_list()
output1_y = df_imu['acc_y'].to_list()
output1_z = df_imu['acc_z'].to_list()

# Filter output 1 data
filtered_output1_x = butter_highpass_filter(output1_x,low_cutoff,high_cutoff,fs)
low_pass_filtered_x = butter_highpass_filter(output1_x,low_cutoff,None,fs)
filtered_output1_y = butter_highpass_filter(output1_y,low_cutoff,high_cutoff,fs)
low_pass_filtered_y = butter_highpass_filter(output1_y,low_cutoff,None,fs)
filtered_output1_z = butter_highpass_filter(output1_z,low_cutoff,high_cutoff,fs)
low_pass_filtered_z = butter_highpass_filter(output1_z,low_cutoff,None,fs)

# Get frequency data
ps_x = np.abs(np.fft.fft(output1_x))**2
time_step = 0.625e-5
freqs_x = np.fft.fftfreq(len(output1_x), time_step)
idx_x = np.argsort(freqs_x)
ps_y = np.abs(np.fft.fft(output1_y))**2
freqs_y = np.fft.fftfreq(len(output1_y), time_step)
idx_y = np.argsort(freqs_y)
ps_z = np.abs(np.fft.fft(output1_z))**2
freqs_z = np.fft.fftfreq(len(output1_z), time_step)
idx_z = np.argsort(freqs_z)


# Estimation of trip location
left_bound = time[int(0.25*len(time))]  # left bound for zoomed area
right_bound = time[int(0.75*len(time))] # right bound for zoomed area


# Get main screen size
getscreen = QtWidgets.QApplication(sys.argv)
screen = getscreen.primaryScreen()
#print('Screen: %s' % screen.name())
size = screen.size()
#print('Size: %d x %d' % (size.width(), size.height()))
rect = screen.availableGeometry()
#print('Available: %d x %d' % (rect.width(), rect.height()))
top_left_x = rect.width()/10.0
top_left_y = rect.height()/10.0
width = rect.width()*0.8
height = rect.height()*0.8 

# Graphing with PyqtGraph
view = pg.GraphicsView()
app = QtWidgets.QApplication([])
view.setBackground('w')
pg.setConfigOption('foreground', 'k')
win = pg.GraphicsLayout()
view.setGeometry(top_left_x,top_left_y,width,height)
win.setWindowTitle(id + " " + session)
view.setCentralItem(win)
view.show()

# Enable antialiasing for prettier plots
pg.setConfigOptions(antialias=True)

# RAW DATA AND COMBINED DATA
win.nextRow()
p2 = win.addPlot(title="Raw Sensor Output vs. Time",colspan=5)
g2x = p2.plot(x = time,y = output1_x, pen="#0EA486")
g2x_combined = p2.plot(x=time,y=low_pass_filtered_x+filtered_output1_x, pen=pg.mkPen("#EF5D43",width=5))
g2y = p2.plot(x = time,y = output1_y, pen="#EF8843")
g2y_combined = p2.plot(x=time,y=low_pass_filtered_y+filtered_output1_y, pen=pg.mkPen("#3D87F7",width=5))
g2z = p2.plot(x = time,y = output1_z, pen="#8570FA")
g2z_combined = p2.plot(x=time,y=low_pass_filtered_z+filtered_output1_z, pen=pg.mkPen("#FBB53C",width=5))
# g2x_filtered = p2.plot(x = time,y = output1_x, pen=(0,0,255))
# axes labeles
p2.setLabel('left', "Sensor Output")
p2.setLabel('bottom', "Time", units='s')
p2.setYRange(-0.5, 1.2, padding=0)

# Subplot FREQUENCY SPECTRUM
win.nextRow()
p5 = win.addPlot(title="Frequency Spectrum",col=0,colspan=1)
g5x = p5.plot(freqs_x[idx_x], ps_x[idx_x],pen="#0EA486",width=5)
g5y = p5.plot(freqs_y[idx_y], ps_y[idx_y],pen="#EF8843",width=5)
g5z = p5.plot(freqs_z[idx_z], ps_z[idx_z],pen="#8570FA",width=5)

p5.setLabel('bottom', "Frequency", units='Hz')
p5.hideButtons()
p5.setLogMode(x=False, y=True)
p5.setXRange(0, 30000, padding=0)
lr = pg.LinearRegionItem(values=[low_cutoff,high_cutoff], bounds=[0.01,1E30])
lr.setZValue(-100)
p5.addItem(lr)

def update_plots():
    global g4x, g4y, g4z
    global g5x_lowpass, g5y_lowpass, g5z_lowpass
    global g2x_combined, g2y_combined, g2z_combined
    p4.removeItem(g4x)
    p4.removeItem(g4y)
    p4.removeItem(g4z)
    p5.removeItem(g5x_lowpass)
    p5.removeItem(g5y_lowpass)
    p5.removeItem(g5z_lowpass)
    p2.removeItem(g2x_combined)
    p2.removeItem(g2y_combined)
    p2.removeItem(g2z_combined)
    low_cutoff = lr.getRegion()[0]
    high_cutoff = lr.getRegion()[1]
    filtered_output1_x = butter_highpass_filter(output1_x,low_cutoff,high_cutoff,fs)
    low_pass_filtered_x = butter_highpass_filter(output1_x,low_cutoff,None,fs)
    g4x = p4.plot(x=time,y=filtered_output1_x, pen=pg.mkPen("#EF5D43",width=5))    
    g5x_lowpass = p5.plot(x=time,y=low_pass_filtered_x, pen=pg.mkPen("#EF5D43",width=5))
    g2x_combined = p2.plot(x=time,y=low_pass_filtered_x+filtered_output1_x, pen=pg.mkPen("#EF5D43",width=5))
    # y data
    filtered_output1_y = butter_highpass_filter(output1_y,low_cutoff,high_cutoff,fs)
    low_pass_filtered_y = butter_highpass_filter(output1_y,low_cutoff,None,fs)
    g4y = p4.plot(x=time,y=filtered_output1_y, pen=pg.mkPen("#3D87F7",width=5))
    g5y_lowpass = p5.plot(x=time,y=low_pass_filtered_y,
                            pen=pg.mkPen("#3D87F7",width=5))
    g2y_combined = p2.plot(x=time,y=low_pass_filtered_y+
                            filtered_output1_y, pen=pg.mkPen("#3D87F7",width=5))
    # z data
    filtered_output1_z = butter_highpass_filter(output1_z,low_cutoff,high_cutoff,fs)
    low_pass_filtered_z = butter_highpass_filter(output1_z,low_cutoff,None,fs)
    g4z = p4.plot(x=time,y=filtered_output1_z, pen=pg.mkPen("#FBB53C",width=5))
    g5z_lowpass = p5.plot(x=time,y=low_pass_filtered_z,
                            pen=pg.mkPen("#FBB53C",width=5))
    g2z_combined = p2.plot(x=time,y=low_pass_filtered_z+
                            filtered_output1_z, pen=pg.mkPen("#FBB53C",width=5))
lr.sigRegionChanged.connect(update_plots)

# BANDPASS FILTER OUTPUT
win.nextRow()
p4 = win.addPlot(title="BANDPASS FILTER vs. Time",colspan=5)
g4x = p4.plot(x=time,y=filtered_output1_x, pen=pg.mkPen("#EF5D43",width=5))
g4y = p4.plot(x=time,y=filtered_output1_y, pen=pg.mkPen("#3D87F7",width=5))
g4z = p4.plot(x=time,y=filtered_output1_z, pen=pg.mkPen("#FBB53C",width=5))
# axes labeles
p4.setLabel('left', "Sensor Output")
p4.setLabel('bottom', "Time", units='s')
# p4.setYRange(-0.2, 0.2, padding=0)
p4.showGrid(x=None,y=True,alpha=0.2) # add grid lines

# LOWPASS FILTER OUTPUT
win.nextRow()
p5 = win.addPlot(title="LOWPASS FILTER vs. Time",colspan=5)
g5x_lowpass = p5.plot(x=time,y=low_pass_filtered_x, pen=pg.mkPen("#EF5D43",width=5))
g5y_lowpass = p5.plot(x=time,y=low_pass_filtered_y, pen=pg.mkPen("#3D87F7",width=5))
g5z_lowpass = p5.plot(x=time,y=low_pass_filtered_z, pen=pg.mkPen("#FBB53C",width=5))
# axes labeles
p5.setLabel('left', "Sensor Output")
p5.setLabel('bottom', "Time", units='s')
p5.showGrid(x=None,y=True,alpha=0.2) # add grid lines
p5.setYRange(-0.5, 1.2, padding=0)

# Run these intially
update_plots()

# Start Qt event loop unless running in interactive mode or using pyside.
if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtWidgets.QApplication([]).instance().exec_()