#!/usr/bin/env bash
ttl=2h
signal=9

for algorithm in demarchi.sh sql.sh # bb.sh
do
  # on OS X, use 'gtimeout'
  timeout -s $signal $ttl generic/$algorithm $@
  echo "Exit code (killed=137?): "$?
done
