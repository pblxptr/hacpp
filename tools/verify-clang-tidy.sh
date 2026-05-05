#!/bin/bash

set -e

if [[ $(wc -l < errors.yml) != "0" ]]; then
    cat errors.yml
    exit 1
fi
