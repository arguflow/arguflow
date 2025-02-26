#!/usr/bin/python3
import sys
import requests
import os

request_timeout = 3


def embed(text):
    if text == "":
        return "NULL"
    try:
        return []
    except:
        return "ERROR"


for size in sys.stdin:
    try:
        # collect a batch for performance
        for row in range(0, int(size)):
            print(embed(sys.stdin.readline().strip()))
        sys.stdout.flush()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.stdout.flush()
        continue
