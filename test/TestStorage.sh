#!/bin/bash
if [[ ! -e a ]]; then
  echo "file \"a\" do not exists"
  exit 0
fi

n=$(lsd | wc -l)
if [[ $n -lt 2 ]]; then
  echo "no enough Storage servers online"
  exit 0
fi

TestStorage client <<EOF
* del t1
* del t2
0 cachesize
0 put t1 a
0 ls
0 lsc
0 putc t2 a
0 cachesize
0 ls
0 lsc
0 get t1 get1
0 getc t2 get2
0 cachesize
0 ls
0 lsc
0 getc t1 get3
0 ls
0 lsc
0 verify t1
0 verify t2
0 cachesize
0 ls
0 lsc
0 freeze t1
0 cachesize
0 ls
0 lsc
0 freeze
0 cachesize
0 ls
0 lsc
0 getsum t1
0 getsum t2
idup t1 0 1
idupc t2 0 1
1 ls
1 lsc
1 getsum t1
1 getsum t2
1 get t1 get4
1 getc t2 get5
* del t1
* del t2
quit
EOF

sha1sum a get1 get2 get3 get4 get5

