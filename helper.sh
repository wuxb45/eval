#!/bin/bash

if [[ -n $2 ]]; then
  xx=$2
else
  xx=TestStorage
fi

syncbin ()
{
  server=$1
  ssh ${server} killall -q "${xx}"
  echo "sync to ${server}"
  rsync -z --progress "${xx}" "${server}:~/program/usr/bin/${xx}"
}

case "$1" in
    d) # distribute
      syncbin think
      syncbin server
      syncbin dell
      #syncbin dualcore
      ;;
    b) # build
      ghc --make -Wall -threaded -fforce-recomp "$xx"
      ;;
esac
    
