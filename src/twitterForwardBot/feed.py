import asyncio
import itertools
import logging
from typing import Union, List

import feedparser
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from feedparser import FeedParserDict
from telegram.ext import ContextTypes


class Tweet:
    def __init__(self, text: str, link: str, author: str, published: Union[pd.Timestamp, str]):
        self.text = text
        self.link = link
        self.author = author
        self.published_time = pd.Timestamp(published)

    def to_html(self):
        return (
            f'<b>{self.author} @ {self.published_time.strftime("%Y-%m-%d %H:%M")}</b> \n'
            + rf'{self.text} <a href="{self.link}"> original_post</a>'
        )


async def grab_feed(twitter_id: str, cutoff_time_utc: pd.Timestamp) -> List[Tweet]:
    url = f"https://rss.qiaomu.pro/twitter/user/{twitter_id}"
    response = await httpx.AsyncClient().get(url, follow_redirects=True)
    feed: FeedParserDict = feedparser.parse(response.content)
    tweets = []
    entries = feed["entries"]
    for entry in entries:
        soup = BeautifulSoup(entry["description"], "html.parser")
        for rsshub_quote in soup.find_all("div", {"class": "rsshub-quote"}):
            rsshub_quote.string = f"\n&gt; {rsshub_quote.get_text(separator=' ', strip=True)}\n\n"
        for br in soup.find_all("br"):
            br.replace_with("\n")
        tm = pd.Timestamp(entry["published"])
        if tm >= cutoff_time_utc:
            tweet = Tweet(soup.text, entry["link"], entry["author"], entry["published"])
            tweets.append(tweet)
    logging.info(f"grabbed {len(tweets)} tweets from {twitter_id} by {tm}")
    return tweets


async def grab_and_publish(context: ContextTypes.DEFAULT_TYPE):
    class context:
        bot_data = {"time_back": pd.Timedelta("1 hour"), "ids":["starzqeth"]}

    tasks = [
        grab_feed(twitter_id, pd.Timestamp.utcnow() - context.bot_data["time_back"]) for twitter_id in context.bot_data["ids"]
    ]
    tweets_collection = await asyncio.gather(*tasks)
    tweets = sorted(list(itertools.chain(*tweets_collection)), key=lambda x: x.published_time)

    for tweet in tweets:
        await context.bot.send_message(chat_id=context.bot_data["publish_chat"], text=tweet.to_html(), parse_mode="HTML")
        await asyncio.sleep(context.bot_data["publish_interval"])

async def send_heart_beat(context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=context.bot_data["heart_beat_chat"],
        text=f"heart beat from twitterBot at {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
    )
