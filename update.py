import asyncio
import string
import sys
from random import random

import tqdm
from bilibili_api import search, video, sync
from typing import List

class Update:

    def __init__(self, topic):
        self.topic = topic

    async def getVideos(self) -> List[tuple[str, float]]:
        numPages = await search.search_by_type(
                keyword=self.topic,
                search_type = search.SearchObjectType.VIDEO,
                time_range=5
            )
        numPages = int(numPages['numPages'])

        videos = []
        try:
            for videoPages in range(1, numPages + 1):
                res = await search.search_by_type(
                    keyword=self.topic,
                    search_type = search.SearchObjectType.VIDEO,
                    time_range=5,
                    page=videoPages
                )
                for video in res['result']:
                    videos.append( ( video['bvid'], video['like'] ) )
        except Exception as e:
            print(f"2{e}")

        return videos


