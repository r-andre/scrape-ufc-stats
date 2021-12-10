#!/usr/bin/env python

from datetime import datetime as dt

def log(text:str):
    f = open('log.txt', 'a')
    time = dt.now().strftime('%Y-%m-%d, %H:%M:%S')
    f.write(time + "; " + text + "\n")
    print(time + "; " + text)
    f.close()
