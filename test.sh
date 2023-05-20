#!/bin/bash

print_error () {
  echo -e "\033[0;31mError:\033[0m $@"
  exit 1
}

print_success () {
  echo -e "\033[0;32mCheck passed:\033[0m $@"
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
  if [[ "$SEQUENTIAL_OUT" != "$OUT" ]]; then
    diff <(echo "$SEQUENTIAL_OUT") <(echo "$OUT") --side-by-side --color
    print_error "Output does not match sequential reference output"
  else
    print_success "Output is correct"
  fi
}

clean_up() {
  rm -rf outputs workbench
  rm -f pg-*
  rm -f mrlog
}

INPUT=
MRAPP=

# Take command-line arguments: 
while getopts "f:a:" flag; do 
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
clean_up
echo "Test #1: parallel MapReduce, no crashes"
echo "==="
if ! ./go-mapreduce --input="$INPUT" --functions="$MRAPP" \
--clean=false 2>&1 | tee mrlog; then
  print_error "MapReduce failed to run"
fi
echo "Check A: correct output"
compare_output
echo "Check B: no extraneous tasks scheduled"
for i in {0..31}; do
  if [[ $(grep -c "Assigned Task $i from Stage map" mrlog) != 1 ]]; then
    print_error "Task $i assigned more than once during Stage map"
  fi
done
for i in {0..7}; do
  if [[ $(grep -c "Assigned Task $i from Stage reduce" mrlog) != 1 ]]; then
    print_error "Task $i assigned more than once during Stage reduce"
  fi
done
print_success "No extraneous tasks scheduled"
clean_up
echo "==="

echo "Test #2: parallel MapReduce, simulate crashes"
echo "==="
if ! ./go-mapreduce --input="$INPUT" --functions="$MRAPP" \
--clean=false --crash-count=2 2>&1 | tee mrlog; then
  print_error "MapReduce failed to run"
fi
echo "Check A: correct output"
compare_output
clean_up
echo "==="

# At end of all tests, remove sequential reference output
rm mr-out-sequential