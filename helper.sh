#!/bin/bash

if [[ -n $2 ]]; then
  xx=$2
else
  echo "usage: > ./helper b <target>"
  echo "       > ./helper d <target>"
  exit 0
fi
hostlist=
ghcopts='--make -Wall -threaded -fforce-recomp'

if [[ -e test/config ]]; then
  . 'test/config'
fi

syncbin ()
{
  bin=$1
  for server in ${hostlist}; do
    ssh ${server} killall -q "${bin}"
    echo "============sync to ${server}============="
    rsync "${bin}" "${server}:~/program/usr/bin/${bin}"
  done
}

case "$1" in
  'd') # distribute
    syncbin "$2"
    ;;
  'b') # build
    echo "with opts: ${ghcopts}"
    ghc ${ghcopts} "$2"
    ;;
esac
    
