import csv
import os

from datetime import datetime, timezone


class TrendsCsvPipeline(object):
    """
    Archive trends data to CSV files
    Classified by date and language
    """

    def __init__(self):
        self.trends = {
            'daily': {},
            'weekly': {},
            'monthly': {}
        }

    def process_item(self, item, spider):
        # Add to buffer
        since = item['since']
        lang = item['lang']
        if lang not in self.trends[since]:
            self.trends[since][lang] = []
        self.trends[since][lang].append(item)
        return item

    def close_spider(self, spider):
        """
        Before spider closed, we write data to CSV files
        """
        utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        for date_range in self.trends:
            for lang in self.trends[date_range]:
                filename = "{}.csv".format(lang)
                # Make dirs
                if date_range == "weekly":
                    path = utc_now.strftime("%Y/%W")
                elif date_range == "monthly":
                    path = utc_now.strftime("%Y/%m")
                else:
                    path = utc_now.strftime("%Y/%m/%d")
                path = os.path.join("archive", date_range, path)
                os.makedirs(path, exist_ok=True)
                self.write_csv(os.path.join(path, filename), self.trends[date_range][lang])

    def write_csv(self, path, items):
        with open(path, "w", newline='') as fp:
            writer = csv.writer(fp, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(["name", "lang", "new stars"])
            for item in items:
                primary_lang = item['primaryLanguage']['name'] if item['primaryLanguage'] else ""
                writer.writerow([item['nameWithOwner'], primary_lang, item['stars_inc']])
