#!/usr/bin/env python3
import os
import re

from flask import Flask, request, render_template, redirect

from helpers import Database, Listing, get_player_summaries

app = Flask(__name__)
db = Database()


@app.route("/")
def index():
    minprice = request.args.get("minprice", 30)
    maxprice = request.args.get("maxprice", 2000)
    csgo = request.args.get("disable_csgo", "off") != "on"
    dota = request.args.get("disable_dota2", "off") != "on"
    tf = request.args.get("disable_tf2", "off") != "on"
    minprice = int(minprice)
    maxprice = int(maxprice)

    games = [
        730 if csgo else None,
        550 if dota else None,
        440 if tf else None,
    ]

    return render_template(
        "index.html",
        listings=db.get_listings(games, minprice, maxprice),
        minprice=minprice,
        maxprice=maxprice,
        csgo=csgo,
        dota=dota,
        tf=tf
    )


@app.route("/avatar-finder")
def avatar_finder():
    avatar = re.sub(r"[^a-z0-9/]", "", request.args["avatar"])
    name = request.args.get("name", "")

    if avatar == "fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb"\
            or avatar == "":
        return "too many results"

    profiles = db.get_profiles(avatar)

    if len(profiles) > 5000:
        return "too many results"

    count = 0
    while True:
        try:
            summaries = get_player_summaries(profiles)
            for profile in summaries:
                if profile["personaname"] == name:
                    return redirect("https://steamcommunity.com/profiles/" + profile["steamid"])
        except Exception as e:
            print(e)
        count += 1
        if count > 3:
            return "too many results"

    return render_template("avatar_finder.html", profiles=summaries)


if __name__ == "__main__":
    app.run()
