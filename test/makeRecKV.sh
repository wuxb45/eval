#!/bin/bash
find $1 -type f -printf "%p\1%s\1" -exec sha1sum {} \;
