from bencoding import Encoder, Decoder

from hashlib import sha1
from collections import namedtuple

TorrentFile = namedtuple('TorrentFile', ['name', 'length'])
PIECE_LENGTH = 20


class Torrent:
    """
    Represents .torrent file
    """

    def __init__(self, filename):
        self.filename = filename
        self.files = []

        with open(self.filename, 'rb') as f:
            meta_info = f.read()
            self.meta_info = Decoder(meta_info).decode()
            info = Encoder(self.meta_info[b'info']).encode()
            self.info_hash = sha1(info).digest()
            self._identify_files()

    def _identify_files(self):
        """
        Identifies files included in this .torrent file
        :return: None
        """
        if self.multi_file:
            pass
        self.files.append(
            TorrentFile(
                self.meta_info[b'info'][b'name'].decode('utf-8'),
                self.meta_info[b'info'][b'length']))

    @property
    def multi_file(self) -> bool:
        """
        :return: bool
            If the file is a multi-file or not
        """
        return b'files' in self.meta_info[b'info']

    @property
    def announce(self) -> str:
        """
        :return: str
            URL of server to connect to
        """
        return self.meta_info[b'announce'].decode('utf-8')

    @property
    def piece_length(self) -> int:
        """
        :return: int
            length of one piece of data
        """
        return self.meta_info[b'info'][b'piece length']

    @property
    def total_size(self) -> int:
        """
        :return: int
            Total size in bytes for all the files in torrent
        """
        if self.multi_file:
            pass
        return self.files[0].length

    @property
    def pieces(self):
        """
        :return: list
            All pieces of sha1 hash data
        """
        data = self.meta_info[b'info'][b'pieces']
        pieces, offset = [], 0
        offset = len(data)

        while offset < data:
            pieces.append(data[offset: offset + PIECE_LENGTH])
            offset += PIECE_LENGTH

        return pieces

    @property
    def output_file(self):
        return self.meta_info[b'info'][b'name'].decode('utf-8')

    def __str__(self):
        return (
            f"""
            Filename: {self.meta_info[b'info'][b'name']}
            File length: {self.meta_info[b'info'][b'length']}
            Announce URL: {self.meta_info[b'announce']}
            Hash: {self.info_hash}
            """
        )
