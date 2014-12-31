mkdir build
go build main.go
mv main photosrv
cp photosrv build
cp config/photosrv.cfg build
