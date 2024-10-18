#!/usr/bin/env python3
#
# stereo_depth_testing.py
#
# Script for streaming and analyzing depthmaps
# for calibration purposes
#
# Copyright 2024 Luxonis Inc.
# With contributions from Hellbender Inc. and Hivemapper Inc.
# See LICENSE file for rights reserved and usage.
#
# Changelog:
# Author Email, Date, Comment
# niessl@hellbender.com, 2024-10-03, VC'ed current version
# alexei@hivemapper.com, 2024-10-16, Added in outlier and preferred surface estimation
#


import cv2
import numpy as np
import depthai as dai

import argparse
import threading

class boxManager:

  def __init__(self):
    self.coords = [None, None]
    self.lock   = threading.Lock()
  
  def setCoords(self, index, coords):
    self.lock.acquire()
    self.coords[index] = coords
    self.lock.release()
  
  def getCoords(self):
    retVal = None
    self.lock.acquire()
    retVal = self.coords
    self.lock.release()
    return retVal

  def reorderCoords(self):
    self.lock.acquire()
    oldPoints = [self.coords[0], self.coords[1]]
    self.lock.release()
    
    newPoints = [[0, 0], [0, 0]]
    
    if oldPoints[0][0] <= oldPoints[1][0]:
      newPoints[0][0] = oldPoints[0][0]
      newPoints[1][0] = oldPoints[1][0]
    else:
      newPoints[0][0] = oldPoints[1][0]
      newPoints[1][0] = oldPoints[0][0]
    if oldPoints[0][1] <= oldPoints[1][1]:
      newPoints[0][1] = oldPoints[0][1]
      newPoints[1][1] = oldPoints[1][1]
    else:
      newPoints[0][1] = oldPoints[1][1]
      newPoints[1][1] = oldPoints[0][1]
    
    self.lock.acquire()
    self.coords = newPoints
    self.lock.release()
    
parser = argparse.ArgumentParser()
parser.add_argument(
    "-res",
    "--resolution",
    type=str,
    default="720",
    help="Sets the resolution on mono cameras. Options: 800 | 720 | 400",
)
parser.add_argument(
    "-md",
    "--mesh_dir",
    type=str,
    default=None,
    help="Output directory for mesh files. If not specified mesh files won't be saved",
)
parser.add_argument(
    "-lm",
    "--load_mesh",
    default=False,
    action="store_true",
    help="Read camera intrinsics, generate mesh files and load them into the stereo node.",
)
parser.add_argument(
    "-rect",
    "--out_rectified",
    default=False,
    action="store_true",
    help="Generate and display rectified streams",
)
parser.add_argument(
    "-lr",
    "--lrcheck",
    default=False,
    action="store_true",
    help="Better handling for occlusions",
)
parser.add_argument(
    "-e",
    "--extended",
    default=False,
    action="store_true",
    help="Closer-in minimum depth, disparity range is doubled",
)
parser.add_argument(
    "-s",
    "--subpixel",
    default=False,
    action="store_true",
    help="Better accuracy for longer distance, fractional disparity 32-levels",
)
parser.add_argument(
    "-m",
    "--median",
    type=str,
    default="7x7",
    help="Choose the size of median filtering. Options: OFF | 3x3 | 5x5 | 7x7 (default)",
)
parser.add_argument(
    "-d",
    "--depth",
    default=False,
    action="store_true",
    help="Display depth frames",
)
parser.add_argument(
    "-swlr",
    "--swap_left_right",
    default=False,
    action="store_true",
    help="Swap left right frames",
)
parser.add_argument(
    "-a",
    "--alpha",
    type=float,
    default=None,
    help="Alpha scaling parameter to increase FOV",
)
args = parser.parse_args()

RES_MAP = {
    '800': {'w': 1280, 'h': 800, 'res': dai.MonoCameraProperties.SensorResolution.THE_800_P },
    '720': {'w': 1280, 'h': 720, 'res': dai.MonoCameraProperties.SensorResolution.THE_720_P },
    '400': {'w': 640, 'h': 400, 'res': dai.MonoCameraProperties.SensorResolution.THE_400_P }
}
if args.resolution not in RES_MAP:
    exit("Unsupported resolution!")

resolution = RES_MAP[args.resolution]

meshDirectory = args.mesh_dir  # Output dir for mesh files
generateMesh = args.load_mesh  # Load mesh files

outRectified = args.out_rectified  # Output and display rectified streams
lrcheck = args.lrcheck  # Better handling for occlusions
extended = args.extended  # Closer-in minimum depth, disparity range is doubled
subpixel = args.subpixel  # Better accuracy for longer distance, fractional disparity 32-levels
depth = args.depth  # Display depth frames

medianMap = {
    "OFF": dai.StereoDepthProperties.MedianFilter.MEDIAN_OFF,
    "3x3": dai.StereoDepthProperties.MedianFilter.KERNEL_3x3,
    "5x5": dai.StereoDepthProperties.MedianFilter.KERNEL_5x5,
    "7x7": dai.StereoDepthProperties.MedianFilter.KERNEL_7x7,
}
if args.median not in medianMap:
    exit("Unsupported median size!")

median = medianMap[args.median]

print("StereoDepth config options:")
print(f"    Resolution:  {resolution['w']}x{resolution['h']}")
print("    Left-Right check:  ", lrcheck)
print("    Extended disparity:", extended)
print("    Subpixel:          ", subpixel)
print("    Median filtering:  ", median)
print("    Generating mesh files:  ", generateMesh)
print("    Outputting mesh files to:  ", meshDirectory)


def getMesh(calibData):
    M1 = np.array(calibData.getCameraIntrinsics(dai.CameraBoardSocket.CAM_B, resolution[0], resolution[1]))
    d1 = np.array(calibData.getDistortionCoefficients(dai.CameraBoardSocket.CAM_B))
    R1 = np.array(calibData.getStereoLeftRectificationRotation())
    M2 = np.array(calibData.getCameraIntrinsics(dai.CameraBoardSocket.CAM_C, resolution[0], resolution[1]))
    d2 = np.array(calibData.getDistortionCoefficients(dai.CameraBoardSocket.CAM_C))
    R2 = np.array(calibData.getStereoRightRectificationRotation())
    mapXL, mapYL = cv2.initUndistortRectifyMap(M1, d1, R1, M2, resolution, cv2.CV_32FC1)
    mapXR, mapYR = cv2.initUndistortRectifyMap(M2, d2, R2, M2, resolution, cv2.CV_32FC1)

    meshCellSize = 16
    meshLeft = []
    meshRight = []

    for y in range(mapXL.shape[0] + 1):
        if y % meshCellSize == 0:
            rowLeft = []
            rowRight = []
            for x in range(mapXL.shape[1] + 1):
                if x % meshCellSize == 0:
                    if y == mapXL.shape[0] and x == mapXL.shape[1]:
                        rowLeft.append(mapYL[y - 1, x - 1])
                        rowLeft.append(mapXL[y - 1, x - 1])
                        rowRight.append(mapYR[y - 1, x - 1])
                        rowRight.append(mapXR[y - 1, x - 1])
                    elif y == mapXL.shape[0]:
                        rowLeft.append(mapYL[y - 1, x])
                        rowLeft.append(mapXL[y - 1, x])
                        rowRight.append(mapYR[y - 1, x])
                        rowRight.append(mapXR[y - 1, x])
                    elif x == mapXL.shape[1]:
                        rowLeft.append(mapYL[y, x - 1])
                        rowLeft.append(mapXL[y, x - 1])
                        rowRight.append(mapYR[y, x - 1])
                        rowRight.append(mapXR[y, x - 1])
                    else:
                        rowLeft.append(mapYL[y, x])
                        rowLeft.append(mapXL[y, x])
                        rowRight.append(mapYR[y, x])
                        rowRight.append(mapXR[y, x])
            if (mapXL.shape[1] % meshCellSize) % 2 != 0:
                rowLeft.append(0)
                rowLeft.append(0)
                rowRight.append(0)
                rowRight.append(0)

            meshLeft.append(rowLeft)
            meshRight.append(rowRight)

    meshLeft = np.array(meshLeft)
    meshRight = np.array(meshRight)

    return meshLeft, meshRight


def saveMeshFiles(meshLeft, meshRight, outputPath):
    print("Saving mesh to:", outputPath)
    meshLeft.tofile(outputPath + "/left_mesh.calib")
    meshRight.tofile(outputPath + "/right_mesh.calib")


def getDisparityFrame(frame, cvColorMap):
    maxDisp = stereo.initialConfig.getMaxDisparity()
    disp = (frame * (255.0 / maxDisp)).astype(np.uint8)
    disp = cv2.applyColorMap(disp, cvColorMap)

    return disp

def colorDistanceFrame(frame, cvColorMap):
    dist = (frame / 256).astype(np.uint8)
    dist = cv2.applyColorMap(dist, cvColorMap)
    return dist

baseframe = None
clickCoords = boxManager()

def filter_outliers(data, threshold=2):
    data = data.astype(float)
    median = np.nanmedian(data)
    deviation = np.abs(data - median)
    mad = np.nanmedian(deviation)
    mask = np.logical_and(np.isfinite(deviation), deviation < (threshold * mad))
    filtered_data = np.copy(data)
    filtered_data[~mask] = np.nan
    return filtered_data

def fill_missing_values(data):
    nans, x = np.isnan(data), lambda z: z.nonzero()[0]
    if nans.all():
        return data
    data[nans] = np.interp(x(nans), x(~nans), data[~nans])
    return data

def fit_plane(X, Y, Z):
    mask = ~np.isnan(Z)
    A = np.c_[X[mask], Y[mask], np.ones(X[mask].size)]
    C, _, _, _ = np.linalg.lstsq(A, Z[mask], rcond=None)
    return C

def calculate_depth_stats(frame, points):
    print(f"Bounds: {points[0]} to {points[1]}")
    x1, y1 = points[0]
    x2, y2 = points[1]
    
    # Extract the depth box using slicing
    depth_box = frame[y1:y2, x1:x2]
    
    # Create a mask for valid depth values
    valid_mask = (depth_box > 0) & (depth_box < 65535)
    pointsArray = depth_box[valid_mask]
    totalPoints = len(pointsArray)
    
    if totalPoints > 0:
        # Calculate percentiles using numpy
        percentiles = [10, 25, 50, 75, 90]
        values = np.percentile(pointsArray, percentiles) / 1000.0  # Convert to meters
        
        # Calculate average distance
        totalDistance = pointsArray.sum()
        reportDist = totalDistance / (1000.0 * totalPoints)
        
        print(f"Average Distance: {reportDist}m, over {totalPoints} points")
        print("Distance in m @ 10th%, 25th, 50th, 75th, 90th:")
        print(", ".join([f"{v:.3f}" for v in values]))
        
        # Apply outlier removal and fill missing values
        filtered_depth_data_2d = filter_outliers(depth_box, threshold=3)
        filled_filtered_data_2d = fill_missing_values(filtered_depth_data_2d)
        
        # Check if there are valid points after filtering
        if np.isnan(filled_filtered_data_2d).all():
            print("All data is NaN after filtering and filling.")
            return
        
        # Fit plane
        X, Y = np.meshgrid(np.arange(filled_filtered_data_2d.shape[1]), np.arange(filled_filtered_data_2d.shape[0]))
        try:
            C = fit_plane(X, Y, filled_filtered_data_2d)
        except ValueError as e:
            print(e)
            return
        
        # Calculate distance to plane at the center
        center_x = (x2 - x1) / 2
        center_y = (y2 - y1) / 2
        distance = (C[0] * center_x + C[1] * center_y + C[2]) / 1000.0  # Convert to meters
        print(f"Distance to plane: {distance:.3f}m")
    else:
        print("Not enough good data to assess distance")


def click_inspect_depth(event, x, y, flags, param):
    # grab references to the global variables
    global baseframe
    global clickCoords
    
    if event == cv2.EVENT_LBUTTONDOWN:
      clickCoords.setCoords(0, [x, y])
      clickCoords.setCoords(1, None)
    elif event == cv2.EVENT_LBUTTONUP:
      clickCoords.setCoords(1, [x, y])
      clickCoords.reorderCoords()
      coords = clickCoords.getCoords()
      calculate_depth_stats(baseframe, coords)
    
device = dai.Device()
calibData = device.readCalibration()
print("Creating Stereo Depth pipeline")
pipeline = dai.Pipeline()

camLeft = pipeline.create(dai.node.MonoCamera)
camLeft.setBoardSocket(dai.CameraBoardSocket.CAM_B)
camRight = pipeline.create(dai.node.MonoCamera)
camRight.setBoardSocket(dai.CameraBoardSocket.CAM_C)
stereo = pipeline.create(dai.node.StereoDepth)
xoutLeft = pipeline.create(dai.node.XLinkOut)
xoutRight = pipeline.create(dai.node.XLinkOut)
xoutDisparity = pipeline.create(dai.node.XLinkOut)
xoutDepth = pipeline.create(dai.node.XLinkOut)
xoutRectifLeft = pipeline.create(dai.node.XLinkOut)
xoutRectifRight = pipeline.create(dai.node.XLinkOut)

if args.swap_left_right:
    camLeft.setBoardSocket(dai.CameraBoardSocket.RIGHT)
    camRight.setBoardSocket(dai.CameraBoardSocket.LEFT)
else:
    camLeft.setBoardSocket(dai.CameraBoardSocket.LEFT)
    camRight.setBoardSocket(dai.CameraBoardSocket.RIGHT)

for monoCam in (camLeft, camRight):  # Common config
    monoCam.setResolution(resolution['res'])
    monoCam.setFps(10.0)

stereo.setDefaultProfilePreset(dai.node.StereoDepth.PresetMode.HIGH_DENSITY)
stereo.initialConfig.setMedianFilter(median)  # KERNEL_7x7 default
stereo.setRectifyEdgeFillColor(0)  # Black, to better see the cutout
stereo.setLeftRightCheck(lrcheck)
stereo.setExtendedDisparity(extended)
stereo.setSubpixel(subpixel)
stereo.setFrameSync(False)

#Matic's recommended updates
stereo.enableDistortionCorrection(True)
stereo.setRectificationUseSpecTranslation(False)
stereo.setDepthAlignmentUseSpecTranslation(False)
stereo.setDisparityToDepthUseSpecTranslation(False)

if args.alpha is not None:
    stereo.setAlphaScaling(args.alpha)
    config = stereo.initialConfig.get()
    config.postProcessing.brightnessFilter.minBrightness = 0
    stereo.initialConfig.set(config)

xoutLeft.setStreamName("left")
xoutRight.setStreamName("right")
xoutDisparity.setStreamName("disparity")
xoutDepth.setStreamName("depth")
xoutRectifLeft.setStreamName("rectifiedLeft")
xoutRectifRight.setStreamName("rectifiedRight")

camLeft.out.link(stereo.left)
camRight.out.link(stereo.right)
stereo.syncedLeft.link(xoutLeft.input)
stereo.syncedRight.link(xoutRight.input)
#stereo.disparity.link(xoutDisparity.input)
if depth:
    stereo.depth.link(xoutDepth.input)
if outRectified:
    stereo.rectifiedLeft.link(xoutRectifLeft.input)
    stereo.rectifiedRight.link(xoutRectifRight.input)

streams = ["left", "right"]
if outRectified:
    streams.extend(["rectifiedLeft", "rectifiedRight"])
#streams.append("disparity")
if depth:
    streams.append("depth")

cvColorMap = cv2.applyColorMap(np.arange(256, dtype=np.uint8), cv2.COLORMAP_INFERNO)
cvColorMap[0] = [0, 0, 0]
print("Creating DepthAI device")
cv2.namedWindow("depth")
cv2.setMouseCallback("depth", click_inspect_depth)
with device:
    device.startPipeline(pipeline)

    # Create a receive queue for each stream
    qList = [device.getOutputQueue(stream, 8, blocking=False) for stream in streams]

    while True:
        for q in qList:
            name = q.getName()
            frame = q.get().getCvFrame()
            if name == "depth":
                baseframe = frame.astype(np.uint16)
                frame = colorDistanceFrame(frame, cvColorMap)
                #frame = (baseframe // 100).clip(0, 255).astype(np.uint8)     #### add this last line
                rectCoords = clickCoords.getCoords()
                if rectCoords[1] is not None:
                  cv2.rectangle(frame, rectCoords[0], rectCoords[1], (255, 0, 0), 2)
            elif name == "disparity":
                frame = getDisparityFrame(frame, cvColorMap)

            cv2.imshow(name, frame)
        if cv2.waitKey(1) == ord("q"):
            break
