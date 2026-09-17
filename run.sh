#!/bin/bash

# ===========================
# CONFIGURATION
# ===========================

N_WORKERS=${1} # change this to any number you want

# ===========================
# BUILD EXECUTABLES
# ===========================

echo "Compiling .cpp files..."

g++ Src_Prjt-CO23BTECH11003-worker.cpp -lzmq -o worker || {
  echo "worker build failed"
  exit 1
}
g++ Src_Prjt-CO23BTECH11003-dispatch.cpp -lzmq -o dispatch || {
  echo "dispatch build failed"
  exit 1
}

echo "Build complete."

# ===========================
# START DISPATCHER
# ===========================

echo "Starting dispatcher..."
./dispatch -n $N_WORKERS -port 5000 -base=6000 -data=dataset.csv --lr=0.0001 &
DISPATCH_PID=$!
echo "Dispatcher PID: $DISPATCH_PID"

# ===========================
# START WORKERS
# ===========================

echo "Starting $N_WORKERS workers..."

WORKER_PIDS=()

for ((i = 1; i <= N_WORKERS; i++)); do
  echo "Starting worker $i..."
  ./worker -id=w$i -port=$((6000 + i)) -disp-port=5000 -data=dataset.csv & # pass unique worker ID
  WORKER_PIDS+=($!)                                                        # store PID
done

echo "Workers started:"
printf '%s\n' "${WORKER_PIDS[@]}"

# ===========================
# WAIT (optional)
# ===========================

wait # keeps script alive until all processes exit
