#!/bin/bash

for f in $(find "$1" -type f); do
  echo -n "$f "
  echo -n `stat -c "%s" "$f"` " "
  echo `sha1sum "$f" | cut -d ' ' -f 1`
done
