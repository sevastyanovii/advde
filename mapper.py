#!/usr/bin/env python
# coding: utf-8

# In[1]:

import sys

for line in sys.stdin:
    for e in line.split(',')[0].split(' '):
        if (e.strip(' ').strip('\n') != ''):
            print(e.strip('\n'))

