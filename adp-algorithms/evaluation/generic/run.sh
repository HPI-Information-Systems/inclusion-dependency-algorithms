#!/usr/bin/env bash
ttl=2h
signal=9

for algorithm in spider.sh sindd.sh binder.sh #bb.sh demarchi.sh
do
  # on OS X, use 'gtimeout'
  timeout -s $signal $ttl generic/$algorithm $@
  echo "Exit code (killed=137?): "$?
done
