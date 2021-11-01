#!/usr/bin/env python
# coding: utf-8

# In[1]:

import sys

ww = None
cntr = 0

for line in sys.stdin:
    cww = line.strip()
    if (ww == None or ww == cww):
        ww = cww
        cntr += 1
    else:
        print(f'{ww},{cntr}')
        cntr = 1
        ww = cww
print(f'{ww},{cntr}')

