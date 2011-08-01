#!/bin/sh

nc localhost 4000 >/tmp/cmd &
echo "GET http://ya.ru HTTP/1.0

" | nc localhost 4001 &
sleep 1
port=`cat /tmp/cmd | cut -d" " -f 2 | cut -d":" -f 2`
nc localhost $port
