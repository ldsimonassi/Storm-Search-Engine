#!/bin/bash
echo Starting...
nohup node ./node-drpc-server.js 1> server.log 2>server.err &
echo DONE!
