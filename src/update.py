from dataclasses import dataclass

import sqlite3
from bs4 import BeautifulSoup
import asyncio
import httpx


@dataclass
class ParsedAudioBook:
    name: str
    author: str
    links: str = ""


class Crawler:
    URLS_PENDING = [
        "http://mds-club.ru/cgi-bin/index.cgi?r=84&lang=rus",
    ]
    URLS_PROCESSED = []
    MAX_CONCURRENT_TASKS = 5

    def __init__(self):
        self.semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_TASKS)
        self.client = httpx.AsyncClient()

        self.known_book_names = self.get_known()
        self.scrapped = []

    @staticmethod
    def get_known() -> list:
        with sqlite3.connect("mds.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM books")
            names = [row[0] for row in cursor.fetchall()]
        return names

    def write_scrapped_info(self):
        print("Writing scrapped info...")

        with sqlite3.connect("mds.db") as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                    INSERT INTO books (name, author, links, status)
                    VALUES (?, ?, ?, ?)
                    """,
                [(x.name, x.author, x.links, "pending") for x in self.scrapped],
            )
            conn.commit()
        print(f"Wrote {len(self.scrapped)} new items to database")

    async def download_page(self, url: str) -> str:
        async with self.semaphore:
            try:
                response = await self.client.get(url)
            finally:
                pass#await self.client.aclose()
        return response.text

    async def scrap_book(self, name: str, author: str, url: str) -> None:
        response = await self.download_page(url)
        soup = BeautifulSoup(response, "html.parser")
        links = "|".join([x["href"] for x in soup.find_all("a") if x["href"].endswith(".mp3")])
        self.scrapped.append(ParsedAudioBook(name=name, author=author, links=links))

    async def crawl(self) -> None:
        if not self.URLS_PENDING:
            return

        url = self.URLS_PENDING.pop()
        print(f"Crawling {url}")
        self.URLS_PROCESSED.append(url)

        html = await self.download_page(url)
        soup = BeautifulSoup(html, "html.parser")
        links = [x["href"] for x in soup.find(id="roller").find_all("a") if "href" in x.attrs]
        self.URLS_PENDING += [x for x in links if x not in self.URLS_PROCESSED + self.URLS_PENDING]
        self.URLS_PENDING = list(set(self.URLS_PENDING))

        tasks = [self.crawl() for _ in range(len(self.URLS_PENDING))]

        table = soup.find(id="catalogtable")
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if not cells[2].find("a"):
                continue
            name = cells[2].find("a").text
            author = cells[1].find("a").text.replace("\xa0", " ")
            if not name in self.known_book_names:
                tasks.append(self.scrap_book(name=name, author=author, url=cells[2].find("a")["href"]))

        await asyncio.gather(*tasks)


async def main():
    obj = Crawler()
    await obj.crawl()
    obj.write_scrapped_info()


if __name__ == "__main__":
    asyncio.run(main())
