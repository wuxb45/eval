#!/bin/bash

xx=TestStorage
syncbin ()
{
  server=$1
  ssh ${server} killall "${xx}"
  rsync -vz "${xx}" "${server}:~/program/usr/bin/${xx}"
}

case "$1" in
    d) # distribute
      syncbin think
      syncbin server
      syncbin dell
      #syncbin dualcore
      ;;
    b) # build
      ghc --make -Wall -threaded -fllvm -fforce-recomp "$xx"
      ;;
esac
    
