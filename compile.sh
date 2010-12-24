#!/bin/sh

c++ -o queue_tagged -O3 -pthread -march=native queue_tagged_main.cc

c++46 -o queue_hazard -O3 -pthread -march=native -std=c++0x -Wl,-rpath,/usr/local/lib/gcc46 -DNDEBUG queue_hazard_main.cc hazard_ptr.cc

c++46 -o sortedlistmap -O3 -pthread -march=native -std=c++0x -Wl,-rpath,/usr/local/lib/gcc46 -DNDEBUG sortedlistmap_main.cc hazard_ptr.cc
