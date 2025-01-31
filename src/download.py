import argparse
import sqlite3
from enum import Enum
from ftplib import FTP
from urllib.parse import urlparse

import requests
from tqdm import tqdm


class Status(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class Downloader:
    def __init__(self):
        parser = argparse.ArgumentParser(description="Downloads MDS files.")
        parser.add_argument(
            "-n",
            metavar="Number",
            default=10,
            type=int,
            required=False,
            help="Number of files to download (10 by default)",
        )
        self.args = parser.parse_args()
        self.batch_size = self.args.n

        self.input_db = "./mds.db"
        self.output_folder = "output"

    def download_http(self, url, filename, ui_filename):
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get("content-length", 0))

        with open(f"{self.output_folder}/{filename}", "wb") as file, tqdm(
            desc=ui_filename,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)
                bar.update(len(data))

    def download_ftp(self, url, filename, ui_filename):
        parsed_url = urlparse(url)

        with FTP(parsed_url.hostname) as ftp:
            ftp.login(parsed_url.username, parsed_url.password)

            try:
                total_size = ftp.size(parsed_url.path)
            except Exception:
                total_size = None

            with open(f"{self.output_folder}/{filename}", "wb") as file, tqdm(
                desc=ui_filename,
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                dynamic_ncols=True,
            ) as bar:

                def callback(data):
                    file.write(data)
                    bar.update(len(data))

                ftp.retrbinary(f"RETR {parsed_url.path[1:]}", callback, blocksize=1024)

    def download_batch(self) -> None:
        with sqlite3.connect(self.input_db) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM books
                WHERE status = ?
                ORDER BY name ASC
                LIMIT ?
            """,
                ("pending", self.batch_size),
            )

            for item in cursor.fetchall():
                filename = f"{item[1]} ({item[3]}).mp3".replace("/", "_")
                ui_filename = f"{item[3]:<20} {item[1]:<40}"
                links = sorted(item[2].split(" || "), key=lambda s: not s.startswith("http"))
                for link in links:
                    try:
                        if link.startswith("http"):
                            self.download_http(link, filename, ui_filename)
                        elif link.startswith("ftp://"):
                            self.download_ftp(link, filename, ui_filename)
                    except Exception as e:
                        status = Status.FAILED
                        print(f"Error downloading {link}")
                        print(e)
                        continue
                    else:
                        status = Status.COMPLETED
                        break
                    finally:
                        cursor.execute(
                            """
                            UPDATE books
                            SET status = ?
                            WHERE id = ?
                            """,
                            (status, item[0]),
                        )
                        conn.commit()


if __name__ == "__main__":
    Downloader().download_batch()
