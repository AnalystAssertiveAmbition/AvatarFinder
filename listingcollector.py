#!/usr/bin/env python3
import json
import os
import threading
import time
from queue import Queue

import argparse
import requests
from urllib.parse import unquote
from helpers import parse_price
import traceback
from helpers import Database, Listing


ACTIVITY_URL = "https://steamcommunity.com/market/itemordersactivity"\
               "?item_nameid={item_id}&country=RU&language=english&currency=1&&two_factor=0&norender=1"


def get_activities(item_id):
    return requests.get(ACTIVITY_URL.format(item_id=item_id)).json()["activity"]


def worker(queue):
    global request_count, keyerror_count, unexpected_error_count
    db = Database()
    while True:
        listing_id, listing_link = queue.get()
        try:
            for activity in get_activities(listing_id):
                if activity["type"] == "BuyOrderCancel" or activity["type"] == "BuyOrderMulti":
                    continue
                listing = Listing(
                    game=int(listing_link.split('/')[5]),
                    item_name=unquote(listing_link.split('/')[6]),
                    price=parse_price(activity["price"]),
                    owner_name=activity["persona_seller"] or activity["persona_buyer"],
                    owner_avatar=activity["avatar_seller"] or activity["avatar_buyer"]
                )
                db.insert_listing(listing)
            request_count += 1
        except KeyError:
            keyerror_count += 1
        except:
            unexpected_error_count += 1
            traceback.print_exc()
        queue.put((listing_id, listing_link))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threadCount", type=int, default=50)

    args = parser.parse_args()

    with open('listings.json', 'r') as f:
        listings = json.load(f)

    queue = Queue()

    request_count = 0
    keyerror_count = 0
    unexpected_error_count = 0

    for listing_id, listing_link in listings.items():
        queue.put((listing_id, listing_link))

    for _ in range(args.threadCount):
        threading.Thread(target=worker, args=(queue,)).start()

    while True:
        print(f"{request_count} request. {keyerror_count} keyerror. {unexpected_error_count} unexpected error")
        request_count = 0
        keyerror_count = 0
        unexpected_error_count = 0
        time.sleep(60)
