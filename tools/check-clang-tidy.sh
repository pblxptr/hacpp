#!/bin/bash

set -e

if [ ! -f "compile_commands.json" ]; then
    echo "No 'compile_commands.json' file found. Aborting."
    exit 1
fi

python3 ../tools/run-clang-tidy.py \
    -quiet \
    -header-filter='.*(lib|app|test)/.*' \
    -export-fixes=errors.yml
