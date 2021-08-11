#!/bin/sh
export PYTHONIOENCODING=utf8
#cd $HOME
/usr/local/spark/bin/spark-submit wordcount.py $1
cd -
