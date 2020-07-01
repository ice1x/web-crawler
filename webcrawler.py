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


def unify_uri(old_uri):
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


def exec_multi(thread_count, function, multi_args):
    """

    Args:
        thread_count(int):
        function(function):
        multi_args(list):

    Returns:
        (list):
    """
    pool = ThreadPool(thread_count)
    responses = pool.map(function, multi_args)
    pool.close()
    pool.join()
    return responses


def filt(a):
    """
    Remove matches from list

    Args:
        a(list):

    Returns:
        (list):
    """
    a.sort()
    return list(a for a, _ in itertools.groupby(a))


def urlopen(url):
    """
    Get HTTP code
    func. from sitemap
    Args:
        url(str):

    Returns:

    """

    def try_urlopen(message_='OK'):
        """

        Args:
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
            try_urlopen()
        return message_

    message = try_urlopen()
    return message


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


def parser(node, tag):
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
                processed_link = unify_uri(link)
                result.append([processed_link, node])
    else:
        info(f'Content length: {str(len(response))} on {str(node)} by tag: {str(tag)}')
    return result


class WebCrawler(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.output = []
        self.message = []

    def iterator(self, node):
        """
        Iterator walks through the website and launch itself if child-link found

        Args:
            node(str): URL

        Returns:
            (None): increment self.output
        """
        info('__________________')
        info(f'Iterator started on node: {node}')

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
                    return 1
            return 0

        """
        Get all href's from node via 'parser' function
        """
        childs = parser(node, 'a')
        info(f'Output during child processing contain {len(childs)} lines')
        for j in childs:
            info(
                f'Trying to compare child node: {j[0]} from\n'
                f'parental node: {j[1]}'
            )
            if not nodelist_checker(j[0], self.output):
                info(f'Child {j[0]} was not visited, start iterator!')
                self.output.append(j)
                self.iterator(j[0])
            else:
                info(f'Child {j[0]} was visited, skip!')

            info('Iterator completed, exit loop')

        # self.output = self.output + childs
        self.output = filt(self.output)
        info('__________________')
        info(f'Iterator out from node: {node}')

    def check_urls(self):
        """
        For each broken URI generate string like:
        HTTP status code :: broken URI :: <<< parental URI
        and append to message

        Returns:
            broken_urls(list):
        """
        info(f'Call zero iterator on node: {self.base_url}')
        self.iterator(self.base_url)
        info('Crawling completed!')
        uris = []
        for cell in self.output:
            info(f'Checked: {cell[1]} >>> {cell[0]}')
            if cell[1].find('https') == -1:
                uris.append(cell[1])
            else:
                uris.append(str(cell[1]).replace('https', 'http'))
        uris = filt(uris)
        info(f'Links: {str(len(uris))}')
        code = exec_multi(1, urlopen, uris)
        broken_urls = []
        for i in range(0, len(uris)):
            if code[i] != 'OK':
                for cell in self.output:
                    if cell[1] == uris[i]:
                        broken_urls.append([code[i], cell[1], f' <<< {cell[0]}'])
        return broken_urls


class CheckUrls(unittest.TestCase):
    """
    Unittest class,pylint make me a capitan
    """

    def test_spider(self):
        """
        Show message if it exist
        """
        spider_unit = WebCrawler(URI)
        message = spider_unit.check_urls()
        self.assertTrue(len(message) == 0, '\n' + '\n'.join(' :: '.join(x) for x in message))


if __name__ == '__main__':
    unittest.main(argv=[sys.argv[0]], verbosity=2)
