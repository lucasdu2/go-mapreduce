#!/bin/bash

print_error () {
  echo -e "\e[31mError:\e[0m $@"
  exit 1
}

print_success () {
  echo -e "\e[32mCheck passed:\e[0m $@"
}

compare_output () {
  if [[ ! -f mr-out-sequential ]]; then
    print_error "Cannot find sequential reference output"
  fi
  if [[ ! -d outputs ]]; then
    print_error "Cannot find directory containing MapReduce outputs"
  fi
  SEQUENTIAL_OUT=$(cat mr-out-sequential)
  OUT=$(cat outputs/mr-out-* | sort)
  if [[ SEQUENTIAL_OUT != OUT ]]; then
    diff <(echo "$SEQUENTIAL_OUT") <(echo "$OUT")
    print_error "Output does not match sequential reference output"
  else
    print_success "Output is correct"
  fi
}

INPUT=
MRAPP=
# TODO: Figure out 1) how to take long options, 2) how to silence errors with 
# leadign colon and then handle ? and : flags later on
# Take command-line arguments: 
while getopts -l "f:a:" flag; do 
  case $flag in
  f) INPUT="$OPTARG";;
  a) MRAPP="$OPTARG";;
  esac
done

# Check that all arguments were provided
if [[ -z "$INPUT" ]]; then 
  print_error "No input file provided"
fi
if [[ -z "$MRAPP" ]]; then
  print_error "No MapReduce application plugin provided"
fi

# Build MapReduce run command (main func in run-mr.go)
go version
if ! go build; then
  print_error "go build failed"
fi

# Run sequential MapReduce to produce reference output
if ! ./go-mapreduce --input="$INPUT" --functions="$MRAPP" --sequential; then
  print_error "Sequential MapReduce failed to run"
fi

# Start running MapReduce tests
# ------------------------------------------------------------------------------
echo "Test #1: parallel MapReduce, no crashes"
echo "---------------------------------------"
if ! ./go-mapreduce --input="$INPUT" --functions="$MRAPP"; then
  print_error "MapReduce failed to run"
fi
check_output
rm -rf outputs
