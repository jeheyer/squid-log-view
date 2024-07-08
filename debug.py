#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import time
from random import sample
from math import floor
from guppy import hpy
from main import get_locations, get_data
from asyncio import run


async def main():

    start_time: time = time()
    options = {'location': list(get_locations().keys())[0]}
    data = await get_data(options)
    print("seconds_to_execute:", round((time() - start_time), 3))
    if len(data['entries']) > 0:
        print(f"now: {floor(start_time)}\n first = {data['entries'][0]}\n last = {data['entries'][-1]}")
        random_samples = sample(data['entries'], 5)
        print("random_samples:")
        for _ in range(len(random_samples)):
            print(f"{_}: {random_samples[_]}")
    h = hpy()
    print(h.heap())
    print(data['durations'])


if __name__ == '__main__':

    run(main())



