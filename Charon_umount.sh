#!/bin/sh

sudo umount $1
for i in `ps aux | grep charon | head -n -1 | awk '{print $2}'`; do kill -9 $i; done

