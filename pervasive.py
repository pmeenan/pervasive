#!/usr/bin/env python3
# Copyright 2025 Google Inc.
from datetime import date,datetime,timezone
from google.cloud import bigquery
from urllib.parse import urlparse
import difflib
import fnmatch
import logging
import os
import pandas as pd
try:
    import ujson as json
except BaseException:
    import json

MAX_URL_LENGTH = 200
BLOCKLIST = ['chunk']
SIZE_MATCH_PERCENT = 5
MONTHS = 6
MIN_STABLE_PATH = 2

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
        for _ in range(MONTHS):
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
        self.destinations = {}

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
                self.destinations[url] = entry['dest']
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
                        logging.debug(f"Removed non-pervasive static URL: {origin}{path}")
                        del self.origins[origin][path]

    def remove_unversioned_urls(self):
        """ Find URLs that have multiple hashes since they are updated in-place """
        for origin in self.origins:
            for path in list(self.origins[origin].keys()):
                hashes = []
                for date in self.origins[origin][path]:
                    for hash in self.origins[origin][path][date]:
                        if hash not in hashes:
                            hashes.append(hash)
                if len(hashes) > 1:
                    logging.debug(f"Removed unversioned URL: {origin}{path}")
                    del self.origins[origin][path]

    def remove_blocked_urls(self):
        """ Remove any URLs that have a blocked string in their filename component """
        for origin in self.origins:
            for path in list(self.origins[origin].keys()):
                filepart = path.split("/")[-1]
                if self.is_blocked(filepart):
                    logging.debug(f"Removed blocked URL: {origin}{path}")
                    del self.origins[origin][path]

    def remove_long_urls(self):
        """ Remove any URLs longer than 200 characters """
        for origin in self.origins:
            for path in list(self.origins[origin].keys()):
                if len(f"{origin}{path}") > MAX_URL_LENGTH:
                    logging.debug(f"Removed long URL: {origin}{path}")
                    del self.origins[origin][path]


    def find_first_difference(self, str1, str2):
        for index, (char1, char2) in enumerate(zip(str1, str2)):
            if char1 != char2:
                return index
        return None

    def create_filename_pattern(self, path, candidates):
        """ Create a wildcard that will match the filenames given as selectively as possible """
        common = None
        last = None
        first = None
        file = path.split('/')[-1]
        for p in candidates:
            f = p.split('/')[-1]
            if f != file:
                s = difflib.SequenceMatcher(None, file, f)
                matches = s.get_matching_blocks()
                if common == None:
                    common = []
                    for match in matches:
                        start, _, size = match
                        if size > 0:
                            end = start + size
                            if first == None or start < first:
                                first = start
                            if last == None or end > last:
                                last = end
                            common.append((start, end))
                else:
                    # Only include the intersection of both match sets
                    intersect = []
                    last = None
                    first = None
                    for match in matches:
                        start, _, size = match
                        if size > 0:
                            end = start + size
                            for cmatch in common:
                                cstart, cend = cmatch
                                s = max(start, cstart)
                                e = min(end, cend)
                                if s < e:
                                    if first == None or s < first:
                                        first = s
                                    if last == None or e > last:
                                        last = e
                                    intersect.append((s, e))
                    common = intersect
        if common is None:
            return None
        if not common:
            return "*"
        pattern = ""
        if first != 0:
            pattern += "*"
        is_first = True
        for chunk in common:
            start, end = chunk
            if not is_first and (not pattern or pattern[-1] != "*"):
                pattern += "*"
            is_first = False
            part = file[start:end].strip("1234567890.-")
            if len(part) > 1:
                pattern += part
        if last != len(file):
            pattern += "*"
        return pattern

    def find_path_pattern(self, origin, path, candidates):
        """
        Find the cases where one path segment has the version or hash (simple wildcard)
        i.e. /maps/1.2.3/common.js
        """
        differences = []
        path_parts = path.split('/')
        for candidate in candidates:
            candidate_parts = candidate.split('/')
            if len(candidate_parts) != len(path_parts):
                return None
            for index in range(len(path_parts)):
                if path_parts[index] != candidate_parts[index] and index not in differences:
                    differences.append(index)
        differences.sort()

        if not differences:
            return None
        
        # The case where a small number of path segments differ and the filename is the same
        filename_matches = differences[-1] != len(path_parts) - 1
        if len(path_parts) - len(differences) > MIN_STABLE_PATH and filename_matches:
            pattern = path
            for diff in differences:
                pattern = pattern.replace(f"/{path_parts[diff]}/", "/*/")
            return pattern
        # Special-case a single difference
        if len(differences) == 1 and differences[0] != len(path_parts) - 1:
            pattern = path.replace(f"/{path_parts[differences[0]]}/", "/*/")
            return pattern
        # The case where the file name differs and, optionally, one path segment
        if differences and len(differences) <= 2 and differences[-1] == len(path_parts) - 1:
            filename_pattern = self.create_filename_pattern(path, candidates)
            if filename_pattern is not None:
                pattern = path.replace(f"/{path_parts[differences[0]]}/", "/*/")
                pattern = pattern.replace(f"/{path_parts[differences[-1]]}", f"/{filename_pattern}")
                return pattern
        return None

    def matches_existing_pattern(self, url):
        """ See if the given URL matches a pattern we already have """
        if url in self.urls:
            return True
        for pattern in self.patterns:
            if fnmatch.fnmatch(url, pattern):
                return True
            
        return False

    def is_blocked(self, path):
        """ Check to see if the file component of the path has any of the blocked strings in it. """
        file = path.split('/')[-1]
        for block in BLOCKLIST:
            if block in file:
                return True
        return False

    def find_patterns(self):
        """
        Take the remaining requests and see if there are similar urls that
        are pervasive as a set and automate generating a pattern for them.
        """
        for origin in self.origins:
            o = self.origins[origin]
            for path in list(o.keys()):
                url = f"{origin}{path}"
                if url in self.destinations and path in o and self.current_date in o[path] and not self.matches_existing_pattern(url):
                    dest = self.destinations[url]
                    hash = list(o[path][self.current_date].keys())[0]
                    target_size = o[path][self.current_date][hash]['size']
                    path_segments = len(path.split('/'))
                    # Find candidate paths that are within 5% of the target size (assume minor changes from version to version)
                    # with "similar" urls
                    candidates = []
                    for p in list(o.keys()):
                        curl = f"{origin}{p}"
                        cdest = self.destinations[curl] if curl in self.destinations else None
                        if p != path and p not in candidates and cdest == dest and len(p.split('/')) == path_segments:
                            date = list(o[p].keys())[0]
                            hash = list(o[p][date].keys())[0]
                            size = o[p][date][hash]['size']
                            if (abs(size - target_size) * 100) / target_size <= SIZE_MATCH_PERCENT:
                                candidates.append(p)
                    if candidates:
                        pattern = self.find_path_pattern(origin, path, candidates)
                        if pattern:
                            # Make sure the aggregate of all of the candidates meet the pervasive threshold
                            matched_urls = []
                            counts = []
                            is_pervasive = True
                            for date in self.dates:
                                total_count = 0
                                for p in o:
                                    if fnmatch.fnmatch(p, pattern):
                                        matched_urls.append(p)
                                        if date in o[p]:
                                            hash = list(o[p][date].keys())[0]
                                            total_count += o[p][date][hash]['count']
                                counts.append(total_count)
                                if total_count < self.pervasive_count:
                                    is_pervasive = False
                            if is_pervasive:
                                logging.info(f"Pattern {counts}: {origin}{pattern}")
                                self.patterns.append(f"{origin}{pattern}")
                            # clean up all of the paths that were used with the pattern
                            for path in matched_urls:
                                if path in o:
                                    del o[path]
                            continue

    def show_unmatched(self):
        """ Display the remaining unmatched URLs """
        for origin in self.origins:
            for path in sorted(list(self.origins[origin].keys())):
                if self.current_date in self.origins[origin][path]:
                    counts = []
                    for date in self.dates:
                        total_count = 0
                        if date in self.origins[origin][path]:
                            for hash in self.origins[origin][path][date]:
                                total_count += self.origins[origin][path][date][hash]['count']
                        counts.append(total_count)
                    logging.info(f"Unmatched URL {counts}: {origin}{path}")

    def aggregate_urls(self):
        """ Load the raw results and group them by origin """
        for date in self.dates:
            self.load_date(date)
        self.find_pervasive_urls()
        self.remove_long_urls()
        self.remove_static_urls()
        self.remove_unversioned_urls()
        self.remove_blocked_urls()
        self.find_patterns()
        self.show_unmatched()

    def run(self):
        self.collect_raw_data()
        self.aggregate_urls()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    collect = Collect()
    collect.run()
