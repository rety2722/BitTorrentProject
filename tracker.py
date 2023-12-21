import aiohttp
import random
import logging
import socket
from struct import unpack
from urllib.parse import urlencode

import bencoding


def _decode_port(port):
    """
    Converts a 32-bit packed binary port number to int
    """
    # Convert from C style big-endian encoded as unsigned short
    return unpack(">H", port)[0]


class TrackerResponse:

    def __init__(self, response: dict):
        self.response = response

    @property
    def failure(self):
        """
        :return: Reason of tracker request failure if it
        occurs

        If not tracker request failure occurs returns None
        """
        if b'failure reason' in self.response:
            return self.response[b'failure reason'].decode('utf-8')
        return None

    @property
    def interval(self) -> int:
        """
        :return: int
            Interval in seconds that the client should wait between sending
            periodic requests to the tracker. Default value is 0
        """
        return self.response.get(b'interval', 0)

    @property
    def complete(self) -> int:
        """
        :return: int
            Number of peers, obtaining entire file, or seeders
        """
        return self.response.get(b'complete', 0)

    @property
    def incomplete(self) -> int:
        """
        :return: int
            Number of peers, that are non-seeders, or 'leechers'
        """
        return self.response.get(b'incomplete', 0)

    @property
    def peers(self) -> list:
        """
        :return: list
            of tuples of peers in format (ip, port)
        """
        peers = self.response[b'peers']
        if isinstance(peers, list):
            logging.debug('Dictionary model peers are returned by tracker')
            return self.decode_list_peers(peers)
        else:
            logging.debug('Binary model peers are returned by tracker')
            return self.decode_string_peers(peers)

    def decode_list_peers(self, peers):
        """
        :param peers: expanded list of peers from tracker response
        :return: list of tuples (ip, port)
        """
        return [(p[b'ip'], p[b'port']) for p in peers]

    def decode_string_peers(self, peers):
        """
        :param peers: binary list of peers from tracker response
        :return: list of tuples (ip, port)
        """
        peers = [peers[i:i + 6] for i in range(0, len(peers), 6)]
        return [(socket.inet_ntop(socket.AF_INET, p[:4]), _decode_port(p[4:])) for p in peers]

    def __str__(self):
        return (
            f"""
            incomplete: {self.incomplete}
            complete: {self.complete}
            interval: {self.interval}
            peers: {', '.join([x for (x, _) in self.peers])}
            """
        )
