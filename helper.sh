#!/bin/bash

xx=TestStorage

case "$1" in
    d) # distribute
      rsync -v "$xx" think:~/program/usr/bin/"$xx"
      rsync -v "$xx" server:~/program/usr/bin/"$xx"
      rsync -v "$xx" dell:~/program/usr/bin/"$xx"
      rsync -v "$xx" dualcore:~/program/usr/bin/"$xx"
      ;;
    b) # build
      ghc --make -threaded -fllvm -fforce-recomp "$xx"
      ;;
esac
    
