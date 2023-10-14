#! /usr/bin/bash

mkdir test_temp
cp -r ./src/* ./test_temp
cp -r ./tests/* ./test_temp

cd ./test_temp
rm -f Main.go
go test -coverprofile ../cov.txt


cd ..
rm -rf ./test_temp