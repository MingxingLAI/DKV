# DKV

[![Java CI with Maven](https://github.com/MingxingLAI/DKV/actions/workflows/maven.yml/badge.svg)](https://github.com/MingxingLAI/DKV/actions/workflows/maven.yml)

Distributed key-value database

# TODO List

* Using restart point to save disk space
* Support Snappy comression
* Support more compression options: zlib
* Implement [SkipList algorithm][https://www.cnblogs.com/xuqiang/archive/2011/05/22/2053516.html]
* Using Block Cache to improve read operation
* Use yaml file to store config
* Use varint to save disk space
* Use chunk and chunk pool to reduce gc
