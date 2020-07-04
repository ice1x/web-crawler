# -*- coding: utf-8 -*-

"""
2016 (c) Iakhin Ilia
Web Crawler - Website parser and URL collector
"""

import itertools
import logging
import sys
import time
import unittest
import urllib
import zlib
import ssl
import urllib.request

from abc import ABC
from logging import info, error
from multiprocessing import Pool as ThreadPool

from urllib3 import PoolManager

from html.parser import HTMLParser
from urllib.error import URLError

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

THREADS = 1
MANAGER = PoolManager(10)
if len(sys.argv) > 1:
    URI = str(sys.argv[1])
else:
    URI = 'http://quotes.toscrape.com'
logging.basicConfig(
    filename='web-crawler.log',
    format='%(levelname)s:%(message)s',
    level=logging.INFO)
BLACKLIST = ['#', "@"]
LINE = '__________________'
TAG = 'a'


def get_normalized_uri(old_uri):
    """
    Prepare URL's

    Args:
        old_uri(str): raw URL
    Returns:
        (str): unified URL
    """
    for word in BLACKLIST:
        if word in old_uri:
            return 'blank'

    info(f'URI before unifying: {old_uri}')

    if old_uri == '/':
        new_uri = URI
    elif old_uri[0] == '/':
        new_uri = URI + old_uri

    if 'http' not in old_uri and 'www' in old_uri:
        new_uri = 'http://' + old_uri
    elif 'http' not in old_uri:
        new_uri = URI + '/' + old_uri

    if old_uri[-1] != '/':
        new_uri += '/'
        # Add slash to the end to prevent any downloading

    info(f'URI updated: {old_uri} >>> {new_uri}')
    return new_uri


def exec_multi(threads, function, multi_args):
    """
    Run function on thread_count threads

    Args:
        threads(int):
        function(function):
        multi_args(list):

    Returns:
        (list):
    """
    pool = ThreadPool(threads)
    result = pool.map(function, multi_args)
    pool.close()
    pool.join()
    return result


def drop_duplicates(a):
    """
    Remove matches from list

    Args:
        a(list):

    Returns:
        (list):
    """
    a.sort()
    return list(a for a, _ in itertools.groupby(a))


def get_url_code(url, message_='OK'):
    """
    Try to get url and return HTTP status code

    Args:
        url(str):
        message_(str):

    Returns:
        (str)
    """

    try:
        MANAGER.request('GET', url, timeout=30)
    except URLError as e:
        if hasattr(e, 'code'):
            message_ = str(e.code)
        elif hasattr(e, 'reason'):
            message_ = e.reason
        # TODO: this log message should be 'debug'
        info(f'{message_} - {str(url)}')
        return message_
    except Exception as e:
        error(f'Exception: {str(url)} - {repr(e)}')
        time.sleep(1)
        info(f'{LINE}One more attempt')
        return get_url_code()
    return message_


def get_urls_by_redirects(redirects):
    """
    Get URL's by redirects without a prefix
    Args:
        redirects(list): list of list's where: 0 - child, 1 - parent

    Returns:
        (list): parental URL's
    """
    uris = []
    for cell in redirects:
        info(f'Checked: {cell[1]} >>> {cell[0]}')
        if cell[1].find('https') == -1:
            uris.append(cell[1])
        else:
            uris.append(str(cell[1]).replace('https', 'http'))
    return uris


class UrlFinder(HTMLParser, ABC):
    """
    Tag parser base class
    """

    def __init__(self, my_tag):
        HTMLParser.__init__(self)
        self.links = []
        self.my_tag = my_tag

    def handle_starttag(self, tag, attrs):
        """

        Args:
            tag(str): HTML tag
            attrs:

        Returns:
            (None): increment self.links

        """
        attrs = dict(attrs)
        if self.my_tag == tag:
            try:
                self.links.append(attrs['href'])
            except:
                pass


def html_tag_parser(node, tag):
    """
    Parse sitemap "where - node" / "search inside this tag"

    Args:
        node(str): URL
        tag(str): HTML tag

    Returns:
        (list): list of list(child URL, parent URL)
    """
    result = []
    _parser = UrlFinder(tag)
    try:
        response = urllib.request.urlopen(node).read()
    except Exception as e:
        info(f'urllib failed: {e.__repr__()}')
        response = []

    if response and len(response) > 0:
        if urllib.request.urlopen(node).headers.get('Content-Encoding') == 'gzip':
            content = zlib.decompress(response, zlib.MAX_WBITS | 32)
        else:
            content = response
        _parser.feed(content.decode())
        for link in _parser.links:
            if link and (URI in link or 'http' not in link):
                processed_link = get_normalized_uri(link)
                result.append([processed_link, node])
    else:
        info(f'Content length: {str(len(response))} on {str(node)} by tag: {str(tag)}')
    return result


def nodelist_checker(node_, nodelist):
    """
    Find 'node' inside the parentals from 'nodelist',
    if found: return 1
    if not: return 0

    nodelist cell format:
    [0] - children node
    [1] - parental node
    Args:
        node_(str): URL
        nodelist(list): child URL's

    Returns:
        (int):
    """
    for transition in nodelist:
        if transition[0] == node_:
            return True


class WebCrawler(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.redirects = []
        self.message = []

    def _add_redirects(self, redirects):
        """
        Args:
            redirects(list):
        Returns:
            (None)
        """
        for redirect in redirects:
            info(f'Trying to compare child node: {redirect[0]} from\nparental node: {redirect[1]}')
            if not nodelist_checker(redirect[0], self.redirects):
                info(f'Child {redirect[0]} was not visited, start iterator!')
                self.redirects.append(redirect)
                self._iterator(redirect[0])
            else:
                info(f'Child {redirect[0]} was visited, skip!')

            info('Iterator completed, exit loop')

    def _iterator(self, node):
        """
        Iterator walks through the website and launch itself if child-link found

        Args:
            node(str): URL

        Returns:
            (None): increment self.output
        """
        info(LINE)
        info(f'Iterator started on node: {node}')

        """
        Get all href's from node via 'parser' function
        """
        redirects = html_tag_parser(node, TAG)
        info(f'Output during child processing contain {len(redirects)} lines')
        self._add_redirects(redirects)
        self.redirects = drop_duplicates(self.redirects)
        info(LINE)
        info(f'Iterator out from node: {node}')

    def check_urls(self):
        """
        For each broken URI generate string like:
        HTTP status code :: broken URI :: <<< parental URI and append to message

        Returns:
            broken_urls(list):
        """
        info(f'Call initial iterator on node: {self.base_url}')
        self._iterator(self.base_url)
        info(f'Crawling completed! Processed: {len(self.redirects)} URLs')

        uris = get_urls_by_redirects(self.redirects)
        uris = drop_duplicates(uris)

        info(f'Links: {str(len(uris))}')
        code = exec_multi(THREADS, get_url_code, uris)
        broken_urls = []
        for i in range(0, len(uris)):
            if code[i] == 'OK':
                continue
            for cell in self.redirects:
                if cell[1] == uris[i]:
                    broken_urls.append([code[i], cell[1], f' <<< {cell[0]}'])
        return broken_urls


class CheckUrls(unittest.TestCase):
    """
    Unittest class as launcher with embedded logging and asserts
    """

    def test_spider(self):
        """
        Show broken URL's if exists
        """
        web_crawler = WebCrawler(URI)
        broken_urls = web_crawler.check_urls()
        self.assertTrue(len(broken_urls) == 0, '\n' + '\n'.join(' :: '.join(x) for x in broken_urls))


if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]], verbosity=2)
