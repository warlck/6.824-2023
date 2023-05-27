#!/usr/bin/env bash

# Runs the test provided by test name in loop until failure using Verbose mode. Records the verbose output of the failed test in 
# output file. Need to be in the folder with the tests to run. 
for i in {1..100}; do        
    echo "Running iteration $i"
    VERBOSE=$1 go test -run $2 1> out.log 2>&1 || { echo "Test failed on attempt $i"; break; }
done