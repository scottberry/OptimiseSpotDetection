#!/bin/bash
head -1 $1
tail -q -n +2 $*
