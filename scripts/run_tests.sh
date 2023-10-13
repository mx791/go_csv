#! /usr/bin/bash

mkdir test_temp
cp ./src/* ./test_temp
cp ./tests/* ./test_temp

cd ./test_temp
go test


cd ..
rm -rf ./test_temp