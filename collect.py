#!/usr/bin/env python3
# Copyright 2025 Google Inc.
from datetime import date,datetime,timezone
from google.cloud import bigquery
from urllib.parse import urlparse
import logging
import os
import string
try:
    import ujson as json
except BaseException:
    import json

class Collect(object):
    def __init__(self):
        self.query = """
                        #standardSQL
                        SELECT
                            url,
                            dest,
                            PARSE_NUMERIC(JSON_VALUE(payload, "$._objectSize")) as size,
                            request_headers,
                            response_headers,
                            JSON_VALUE(payload, "$._body_hash") as body_hash,
                        FROM
                            `httparchive.crawl.requests`,
                        WHERE
                            date = "{}-01" AND
                            JSON_VALUE(payload, "$._body_hash") IS NOT NULL AND
                            lower(resp_h.name) = "cache-control" AND
                            lower(resp_h.value) LIKE "%public%" AND
                            lower(req_h.name) = "sec-fetch-dest" AND
                            (lower(req_h.value) = "script" OR lower(req_h.value) = "style" OR lower(req_h.value) = "empty") AND
                            PARSE_NUMERIC(JSON_VALUE(payload, "$._responseCode")) = 200 AND
                            PARSE_NUMERIC(JSON_VALUE(payload, "$._objectSize")) > 1000
                        LIMIT 10
                    """
        self.query2 = """
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
        for _ in range(1):
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            self.dates.append('{}-{}'.format(year, month))
        self.bq_client = None

    def query_date(self, date):
        results_file = os.path.join(self.data_dir, '{}.json'.format(date))
        if not os.path.exists(results_file):
            query = self.query.format(date)
            if self.bq_client is None:
                self.bq_client = bigquery.Client()
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            job_config.allow_large_results = True
            job = self.bq_client.query(query, job_config=job_config)
            result = job.result()
            results = []
            for row in result:
                results.append(row)
            with open(results_file, 'w') as f:
                json.dump(results, f)

    def collect_raw_data(self):
        """ Run the raw bigquery queries and store the results locally """
        for date in self.dates():
            self.query_date(date)

    def run(self):
        self.collect_raw_data()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    collect = Collect()
    collect.run()
