#!/bin/bash
# Description: This script converts an ONNX model into IR, transfers the files to a remote server and compiles it using OpenVINO for RVC3.
# Requirements: The script expects `openvino-dev==2022.3.0` to be installed in the environment. To install it, use this command: `pip install openvino-dev==2022.3.0`.
# Author: Luxonis
# Date: 2024-10-02

# === Usage Function ===
usage() {
    echo "Usage: $0 <remote_user> <remote_host> <model_path>"
    echo ""
    echo "Arguments:"
    echo "  remote_user   - Username for the remote server"
    echo "  remote_host   - Hostname or IP address of the remote server"
    echo "  model_path    - Local path to the ONNX model file to be exported"
    echo ""
    echo "Example:"
    echo "  $0 your_username remote.server.com /path/to/model.xml"
    exit 1
}

# === Check for Required Arguments ===
if [ "$#" -ne 3 ]; then
    echo "Error: Invalid number of arguments."
    usage
fi

# === Assign Arguments to Variables ===
REMOTE_USER="$1"
REMOTE_HOST="$2"
MODEL_PATH="$3"

# === Validate Model Path ===
if [ ! -f "$MODEL_PATH" ]; then
    echo "Error: Model file '$MODEL_PATH' does not exist."
    exit 1
fi

# === IR Conversion ===
echo "Converting ONNX model to IR..."
mo --input_model "$MODEL_PATH" --mean_values "[0,0,0]" --scale_values "[255,255,255]" --reverse_input_channels --compress_to_fp16=False --output_dir ./optimized_model/

if [ $? -ne 0 ]; then
    echo "Error: Failed to convert ONNX model to IR."
    exit 1
fi
MODEL_BASENAME=$(basename "$MODEL_PATH" .onnx)

# === Optional: Define Remote Directory ===
REMOTE_DIR="models"

# === Check if Remote Directory Exists ===
echo "==========================================================================="
echo "Checking if remote directory '${REMOTE_DIR}' exists on ${REMOTE_HOST}..."
ssh -o StrictHostKeyChecking=no "${REMOTE_USER}@${REMOTE_HOST}" "if [ ! -d ${REMOTE_DIR} ]; then mkdir -p ${REMOTE_DIR}; echo 'Directory created.'; else echo 'Directory already exists.'; fi"
if [ $? -ne 0 ]; then
    echo "Error: Failed to check/create remote directory '${REMOTE_DIR}'."
    exit 1
fi

# === Transfer the Model File to Remote Server ===
echo "==========================================================================="
echo "Transferring model file to remote server..."
scp -o StrictHostKeyChecking=no "./optimized_model/$MODEL_BASENAME.xml" "${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}/"
scp -o StrictHostKeyChecking=no "./optimized_model/$MODEL_BASENAME.bin" "${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}/"
if [ $? -ne 0 ]; then
    echo "Error: Failed to transfer model file to remote server."
    exit 1
fi
echo "Model file transferred successfully."

# === Commands to Execute on Remote Server ===
REMOTE_COMMANDS="source /opt/openvino/setupvars.sh && cd ~/${REMOTE_DIR}/ && /opt/openvino/tools/compile_tool/compile_tool -d VPUX.3400 -m ${MODEL_BASENAME}.xml -ip U8"

# === Execute Commands via SSH ===
echo "==========================================================================="
echo "Executing commands on remote server..."
ssh -o StrictHostKeyChecking=no "${REMOTE_USER}@${REMOTE_HOST}" "$REMOTE_COMMANDS"
EXIT_STATUS=$?

if [ $EXIT_STATUS -eq 0 ]; then
    echo "Commands executed successfully on remote server."
else
    echo "Commands failed with exit status ${EXIT_STATUS}."
    exit $EXIT_STATUS
fi

# === Download the Compiled Model File ===
echo "==========================================================================="
echo "Downloading compiled model file from remote server..."
scp -o StrictHostKeyChecking=no "${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}/${MODEL_BASENAME}.blob" .

if [ $? -ne 0 ]; then
    echo "Error: Failed to download compiled model file from remote server."
    exit 1
fi

echo "==========================================================================="
echo "Compiled model file downloaded successfully to './${MODEL_BASENAME}.blob'."

# === Exit Script ===
exit 0
