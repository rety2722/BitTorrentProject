import aiohttp
import random
import logging
import socket
from struct import unpack
from urllib.parse import urlencode

from bencoding import Decoder


def _decode_port(port):
    """
    Converts a 32-bit packed binary port number to int
    """
    # Convert from C style big-endian encoded as unsigned short
    return unpack(">H", port)[0]


def _calculate_peer_id():
    """
    Calculate and return unique peer id

    The `peer id` is a 20 byte long identifier. This implementation use the
    Azureus style `-PC1000-<random-characters>`.
    :return: unique peer id
    """
    return '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)])


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
            interval in seconds that the client should wait between sending
            periodic requests to the tracker. Default value is 0
        """
        return self.response.get(b'interval', 0)

    @property
    def complete(self) -> int:
        """
        :return: int
            number of peers, obtaining entire file, or 'seeders'
        """
        return self.response.get(b'complete', 0)

    @property
    def incomplete(self) -> int:
        """
        :return: int
            number of peers, that are non-seeders, or 'leechers'
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


class Tracker:
    """
    Represents the connection to a tracker for a given Torrent that is either
    under download or seeding state.
    """

    def __init__(self, torrent):
        """
        :param torrent: torrent.Torrent instance
            representing a torrent file
        """
        self.torrent = torrent
        self.peer_id = _calculate_peer_id()
        self.http_client = aiohttp.ClientSession()

    async def connect(self,
                      first: bool = None,
                      uploaded: int = 0,
                      downloaded: int = 0):
        """
        Makes announce call to the tracker to update with our statistics
        as well as get a list of peers to connect to

        If the call was successful, the list of peers will be updated as a result
        of calling this function

        :param first: Whether this is first announce call or not
        :param uploaded: The total number of bytes uploaded
        :param downloaded: The total number of bytes downloaded
        """
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': self.torrent.total_size - downloaded,
            'compact': 1
        }

        if first:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        logging.info('Connecting to tracker at: ' + url)

        async with self.http_client.get(url) as response:
            # unsuccessful connection
            if not response.status == 200:
                raise ConnectionError(f'Unable to connect to tracker, status code {response.status}')
            data = await response.read()
            self.raise_for_error(data)
            return TrackerResponse(Decoder(data).decode())

    def close(self):
        self.http_client.close()

    def raise_for_error(self, tracker_response):
        """
        A hacky fix to detect errors by tracker even when the response status
        code is 200
        :param tracker_response: response data
        :raises: ConnectionError if 'failure' parameter is in response
        """
        try:
            # error message will be utf-8 only
            message = tracker_response.decode('utf-8')
            if 'failure' in message:
                raise ConnectionError(f'Unable to connect to tracker: {message}')
        # successful response will contain non-unicode data
        except UnicodeDecodeError:
            pass
