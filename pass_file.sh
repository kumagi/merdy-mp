#!/bin/bash
for i in `seq -w 1 18`;
  do
  #echo "akiyama${i}"
  #ssh -l kumazaki akiyama${i} "rm merdy-mp" &> /dev/null
  #ssh -l kumazaki akiyama${i} "mkdir merdy-mp -p"
  scp * kumazaki@akiyama${i}:~/merdy-mp/ &> /dev/null &
  done

echo "done."