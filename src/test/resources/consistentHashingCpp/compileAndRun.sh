#!/bin/bash

#clang++ -std=c++20 -std=gnu++11  TableHash.cc
clang++ -std=c++2a -stdlib=libc++ TableHash.cc -o toBedeleted

./toBedeleted

rm ./toBedeleted
