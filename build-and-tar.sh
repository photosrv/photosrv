#!/bin/bash
rm photosrv
rm main

cd cmd/shardsprites
go build
cd ../logcrunch-s
go build
cd ../logcrunch
go build
cd ..
cd ..
bash build.sh
bash mk-build.sh
cp cmd/shardsprites/shardsprites build
cp cmd/logcrunch-s/logcrunch-s build
cp cmd/logcrunch/logcrunch build
rm photosrv.tgz
tar -czvf photosrv-build.tgz build/
mv build/photosrv ../bin
