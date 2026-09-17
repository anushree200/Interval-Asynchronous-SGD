INTERVAL ASYNCHRONOUS PARALLEL SGD

CONTENTS
Assignment Description
Technologies Used
Installation of the C++ Compiler
Contents of the folder
Executing the program



ASSIGNMENT DESCRIPTION

This assignment focuses on implementing Interval Asynchronous Parallel SGD, a semi-asynchronous optimization strategy designed to balance scalability and accuracy during distributed training. Traditional synchronous SGD suffers from waiting overhead, while fully asynchronous methods introduce excessive gradient staleness that harms convergence. Interval-Asynchrony solves this by dividing execution into fixed-size asynchronous intervals, allowing workers to operate independently within each interval while enforcing a strict staleness bound between intervals.

In this project, we extend the Interval-Asynchrony technique—originally proposed for multithreaded shared-memory systems—to a distributed environment using ZeroMQ sockets. Multiple worker nodes compute gradients in parallel and send them to a central dispatcher (parameter server), which decides whether each gradient should be accepted or discarded based on interval boundaries. Only gradients fully contained within the active interval are applied to the global model, ensuring bounded staleness and improved statistical efficiency.

This implementation demonstrates how Interval-Asynchronous SGD can maintain high throughput, reduce staleness, and achieve stable convergence even at higher levels of parallelism. The result is a distributed SGD system that is significantly faster than synchronous execution and more accurate than naive asynchronous approaches.

TECHNOLOGIES USED:
C++17 – Core programming language used to implement the distributed Interval-Asynchronous SGD logic.
ZeroMQ (ZMQ) – Messaging library used for reliable, high-performance communication between distributed worker nodes and the central dispatcher.
Linux Environment – Recommended platform for compiling and running the distributed system.

INSTALLATION OF C++ COMPILER AND ZMQ
On Ubuntu / Debian

sudo apt update
sudo apt install build-essential

This installs:
g++ (C++ compiler)
standard libraries

Install ZeroMQ Library
On Ubuntu / Debian:

sudo apt install libzmq3-dev

CONTENTS OF THE FOLDER
dataset.csv - this is the input dataset file for the SGD
Readme.txt - Current file which contains the execution instructions
Rpt_Prjt-CO23BTECH11003.pdf - Contains the detailed explanation and analysis of approach
Src_Prjt-CO23BTECH11003-worker.cpp - contains the worker thread code
Src_Prjt-CO23BTECH11003-dispatch.cpp - contains the dispatcher thread code
ZmqNode.hpp - helper function file for the Zmq sockets
helper.hpp - functions for serialising and deserialising arrays
Turnitin_Prjt-CO23BTECH11003.pdf - Score report using Turnitin
FastParallelSGD.pdf - Paper we referred to for the project
run.sh - This simulates multiple independent worker processes in a distributed setup.

EXECUTION OF THE CODE
To run the entire distributed setup, simply execute the provided run.bash script.
This script automatically starts the dispatcher and launches the required number of worker processes.

./run.sh <number_of_workers>

Example: ./run.sh 5

This creates a log file for each of the worker threads and a overall log file created by the dispatcher.

Make sure the following file is present in the same directory: dataset.csv

