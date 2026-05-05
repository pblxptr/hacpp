#!/bin/bash

set -e

git diff --color > clang-format.patch
if [[ $(wc -l < clang-format.patch) != "0" ]]; then
  cat clang-format.patch
  git checkout -- .
  exit 1
fi
