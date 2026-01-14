#!/usr/bin/env python3
# Copyright 2025 Google Inc.
from datetime import date,datetime,timezone
from google.cloud import bigquery
from urllib.parse import urlparse
import logging
import os
import pandas as pd
try:
    import ujson as json
except BaseException:
    import json

class Collect(object):
    def __init__(self):
        self.pervasive_count = 100000
        self.query = """
                        #standardSQL
                        SELECT
                            url,
                            ANY_VALUE(dest) as dest,
                            ANY_VALUE(size) as size,
                            ANY_VALUE(request_headers) as request_headers,
                            ANY_VALUE(response_headers) as response_headers,
                            body_hash,
                            COUNT(*) as num
                        FROM (
                            SELECT
                                url,
                                PARSE_NUMERIC(JSON_VALUE(payload, "$._objectSize")) as size,
                                JSON_VALUE(payload, "$._body_hash") as body_hash,
                                req_h.value as dest,
                                request_headers,
                                response_headers
                            FROM
                                `httparchive.crawl.requests`,
                                UNNEST (request_headers) as req_h,
                                UNNEST (response_headers) as resp_h
                            WHERE
                                date = "{}-01" AND
                                JSON_VALUE(payload, "$._body_hash") IS NOT NULL AND
                                lower(resp_h.name) = "cache-control" AND
                                lower(resp_h.value) LIKE "%public%" AND
                                lower(req_h.name) = "sec-fetch-dest" AND
                                (lower(req_h.value) = "script" OR lower(req_h.value) = "style" OR lower(req_h.value) = "empty") AND
                                PARSE_NUMERIC(JSON_VALUE(payload, "$._responseCode")) = 200 AND
                                PARSE_NUMERIC(JSON_VALUE(payload, "$._objectSize")) > 1000
                        ) Hashes
                        GROUP BY url, body_hash
                        HAVING COUNT(*) > 20000
                        ORDER BY num DESC
                    """
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.dates = []
        today = date.today()
        year = today.year
        month = today.month
        for _ in range(6):
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            self.dates.append('{}-{:02d}'.format(year, month))
        self.current_date = self.dates[0]
        self.bq_client = None
        self.origins = {}
        self.urls = []
        self.patterns = []

    def process_headers(self, headers):
        result = {}
        for header in headers:
            name = header['name'].lower()
            value = header['value']
            val = '{}, {}'.format(result[name], value) if name in result else value
            result[name] = val
        return result

    def query_date(self, date):
        results_file = os.path.join(self.data_dir, '{}.json'.format(date))
        if not os.path.exists(results_file):
            logging.info("Collecting results for %s...", date)
            query = self.query.format(date)
            if self.bq_client is None:
                self.bq_client = bigquery.Client()
            job = self.bq_client.query(query)
            # Convert query results to a Pandas DataFrame
            df = job.to_dataframe()
            results = json.loads(df.to_json(orient="records", date_format='iso'))
            with open(results_file, 'w', encoding='utf-8') as f:
                f.write('[')
                is_first = True
                for result in results:
                    out = dict(result)
                    out['request_headers'] = self.process_headers(result['request_headers'])
                    out['response_headers'] = self.process_headers(result['response_headers'])
                    # only allow "empty" dest if it is a compression dictionary
                    if out['dest'] == 'empty' and 'use-as-dictionary' not in out['response_headers']:
                        continue
                    # Exclude requests with query parameters
                    if '?' in out['url']:
                        continue
                    # Exclude any responses with a set-cookie response header
                    if 'set-cookie' in out['response_headers']:
                        continue
                    # Exclude any responses that are not "cache-control: public"
                    if 'cache-control' not in out['response_headers'] or 'public' not in out['response_headers']['cache-control']:
                        continue
                    # write the candidate
                    if is_first:
                        is_first = False
                        f.write('\n')
                    else:
                        f.write(',\n')
                    json.dump(out, f)
                f.write('\n]\n')

    def collect_raw_data(self):
        """ Run the raw bigquery queries and store the results locally """
        for date in self.dates:
            self.query_date(date)

    def load_date(self, date):
        results_file = os.path.join(self.data_dir, '{}.json'.format(date))
        raw = []
        with open(results_file, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        for entry in raw:
            url = entry['url']
            hash = entry['body_hash']
            num = entry['num']
            parsed_uri = urlparse(url)
            origin_str = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
            path = parsed_uri.path
            # Only consider new origins if they are from the latest crawl
            if origin_str not in self.origins and date == self.current_date:
                self.origins[origin_str] = {}
            if origin_str not in self.origins:
                continue
            origin = self.origins[origin_str]
            if path not in origin:
                origin[path] = {}
            if date not in origin[path]:
                origin[path][date] = {}
            if hash not in origin[path][date]:
                origin[path][date][hash] = {'count': 0, 'size': entry['size']}
            origin[path][date][hash]['count'] += num

    def find_pervasive_urls(self):
        """ Find URLs that were the same and pervasive for all months """
        for origin in self.origins:
            for path in list(self.origins[origin].keys()):
                if len(self.origins[origin][path]) == len(self.dates):
                    is_pervasive = True
                    counts = []
                    for date in self.dates:
                        total_count = 0
                        if date in self.origins[origin][path]:
                            for hash in self.origins[origin][path][date]:
                                total_count += self.origins[origin][path][date][hash]['count']
                        counts.append(total_count)
                        if total_count < self.pervasive_count:
                            is_pervasive = False
                            break
                    if is_pervasive:
                        logging.info(f"Pervasive static URL {counts}: {origin}{path}")
                        url = f"{origin}{path}"
                        if url not in self.urls:
                            self.urls.append(url)
                        del self.origins[origin][path]

    def remove_static_urls(self):
        """
        Find URLs that were present in all months and did not change.
        We do this after extracting the pervasive ones to make sure we don't use
        these URLs when generating patterns for the remaining resources.
        """
        for origin in self.origins:
            for path in list(self.origins[origin].keys()):
                if len(self.origins[origin][path]) == len(self.dates):
                    hashes = []
                    for date in self.origins[origin][path]:
                        for hash in self.origins[origin][path][date]:
                            if hash not in hashes:
                                hashes.append(hash)
                    if len(hashes) == 1:
                        logging.info(f"Removing non-pervasive static URL: {origin}{path}")
                        del self.origins[origin][path]

    def find_patterns(self):
        """
        Take the remaining requests and see if there are similar urls that
        are pervasive as a set and automate generating a pattern for them.
        """
        pass

    def aggregate_urls(self):
        """ Load the raw results and group them by origin """
        for date in self.dates:
            self.load_date(date)
        self.find_pervasive_urls()
        self.remove_static_urls()
        self.find_patterns()

    def run(self):
        self.collect_raw_data()
        self.aggregate_urls()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    collect = Collect()
    collect.run()
