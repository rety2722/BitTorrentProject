from collections import OrderedDict


class Decoder:
    """
    Decodes bencoded data

    ...

    Attributes
    ----------
    _data : bytes
        bencoded data, that is to be decoded
    _index : int
        current decoding index

    Methods
    -------
    decode()
        Decodes bencoded data, is a main decode function
    read_next()
        Reads next token
    read_by_length(length)
        Reads 'length' next tokens
    read_until(token)
        Reads data until the 'token' is met
    """

    def __init__(self, data: bytes):
        """
        :param data: bytes
            Bencoded data to be decoded
        """
        if not isinstance(data, bytes):
            raise TypeError('Argument \'data\' must be of type bytes')
        self._data = data
        self._index = 0

    def decode(self):
        """
        Decodes bencoded data
        :return: Any
            matching python object (int, str, dict or list)
        :raises
            EOFError if data ends unexpectedly (not with e)
            RuntimeError if data is invalid
        """
        c = self.read_next()
        if c is None:
            raise EOFError('Unexpected end-of-file')
        elif c == b'i':
            self._index += 1
            return self._decode_int()
        elif c == b'l':
            self._index += 1
            return self._decode_list()
        elif c == b'd':
            self._index += 1
            return self._decode_dict()
        elif c == b'e':
            return None
        elif c in b'0123456789':
            return self._decode_str()
        else:
            raise RuntimeError(f'Invalid token read at {str(self._index)}')

    def read_next(self):
        """
        Reads next character of _data
        :return: Any
            Next character if possible, else None
        """
        if self._index + 1 > len(self._data):
            return None
        return self._data[self._index: self._index + 1]

    def read_by_length(self, length: int) -> bytes:
        """
        Reads next 'length' number of bytes from data
        :param length: int
            Number of bytes to read
        :return: bytes
            Byte string of length 'length'
        :raises
            IndexError if given length exceeds left data size
        """
        if self._index + length > len(self._data):
            raise IndexError(f'Cannot read {length} bytes form current position {self._index}')
        result = self._data[self._index: self._index + length]  # next characters
        self._index += length
        return result

    def read_until(self, token: bytes) -> bytes:
        """
        Reads data until finds 'token'
        :param token: bytes
            Symbol, that should be found
        :return: bytes
            Substring of data starting from current position to 'token', excluding 'token'
        """
        try:
            occurrence = self._data.index(token, self._index)
            result = self._data[self._index: occurrence]
            self._index = occurrence + 1
            return result
        except ValueError:
            raise RuntimeError(f'Unable to find token {str(token)}')

    def _decode_int(self):
        return int(self.read_until(b'e'))

    def _decode_str(self):
        length = int(self.read_until(b':'))
        return self.read_by_length(length)

    def _decode_list(self):
        result = []
        # recursively decode until corresponding end is found
        while self.read_next() != b'e':
            result.append(self.decode())
        self._index += 1  # read ending
        return result

    def _decode_dict(self):
        result = OrderedDict()
        while self.read_next() != b'e':
            key = self.decode()
            value = self.decode()
            result[key] = value
        self._index += 1
        return result


class Encoder:
    """
    Encodes python objects to byte string

    Supported python types are:
    int,
    str,
    list,
    dict

    Any other types will be ignored
    """

    def __init__(self, data):
        """
        :param data: Any
            Data to be bencoded
        """
        self._data = data

    def encode(self):
        """
        Encodes provided data to byte string
        :return: bytes
            Bencoded data
        """
        return self.encode_next(self._data)

    def encode_next(self, data):
        if isinstance(data, int):
            return self._encode_int(data)
        elif isinstance(data, str):
            return self._encode_str(data)
        elif isinstance(data, bytes):
            return self._encode_bytes(data)
        elif isinstance(data, list):
            return self._encode_list(data)
        elif isinstance(data, dict):
            return self._encode_dict(data)
        else:
            return None

    def _encode_int(self, data: int) -> bytes:
        return str.encode('i' + str(data) + 'e')

    def _encode_str(self, data: str) -> bytes:
        return str.encode(str(len(data)) + ':' + data)

    def _encode_list(self, data: list) -> bytes:
        return bytearray('l', 'utf-8') + b''.join([self.encode_next(i) for i in data]) + b'e'

    def _encode_dict(self, data: dict) -> bytes:
        result = bytearray('d', 'utf-8')
        for k, v in data.items():
            key = self.encode_next(k)
            value = self.encode_next(v)
            if key and value:
                result += key + value
            else:
                raise RuntimeError('invalid dict')
        return result + b'e'

    def _encode_bytes(self, data: bytes) -> bytes:
        return bytearray() + str.encode(str(len(data))) + b':' + data
