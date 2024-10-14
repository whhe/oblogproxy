#!/usr/bin/env

mkdir -p packenv && cd packenv
cmake -DWITH_TEST=OFF -DWITH_ASAN=OFF -DWITH_DEMO=OFF -DCOMMUNITY_BUILD=OFF -DCMAKE_VERBOSE_MAKEFILE=ON -DWITH_DEBUG=OFF -DWITH_US_TIMESTAMP=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cd ..

#      sh package.sh -e business -d OFF ; echo ""

set -x
sed -i "s/-znoexecstack//g" packenv/compile_commands.json
sed -i "s/-znow//g" packenv/compile_commands.json
sed -i "s/-Wno-class-memaccess//g" packenv/compile_commands.json
sed -i "s/-pie//g" packenv/compile_commands.json

run-clang-tidy -p ./packenv > ./clang-tidy-output.txt ; python3 ./clang-tidy-to-junit.py ./clang-tidy-output.txt > ./clang-tidy-output-junit.xml

python3 ./clang-tidy-to-junit.py ./clang-tidy-output.txt > ./clang-tidy-output-junit.xml
