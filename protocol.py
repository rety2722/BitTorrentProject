import asyncio
import logging
import struct
from asyncio import Queue
from concurrent.futures import CancelledError

import bitstring

REQUEST_SIZE = 2 ** 14


class ProtocolError(BaseException):
    pass


class PeerConnection:
    """
    PeerConnection is used to download and upload pieces (blocks)

    It will take (consume) one peer from the given queue.
    Based on the peer details the PeerConnection will try to
    make a connection and perform a peer handshake (first message sent).

    After a successful handshake, the PeerConnection will be in
    a 'choked' state, not allowed to request any data from the
    remote peer. After sending an interested message the PeerConnection
    will be waiting to get 'unchoked'.

    When the remote peer 'unchokes' us, we start requesting pieces (blocks of data).
    The PeerConnection will continue to request pieces for as long as
    there are pieces left to request, or until remote peer disconnects.

    If the connection with a remote peer drops, the PeerConnection will consume
    the next available peer from off the queue and try to connect to that one
    instead.
    """

    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, on_block_cb=None):
        """
        Construct a PeerConnection and add it to the asyncio event loop.

        Use 'stop' to abort the connection and any subsequent connection
        attempts.

        :param queue: asyncio.Queue
            containing available peers to connect to
        :param info_hash: Torrent.info_hash
            SHA1 hash for the meta-data info
        :param peer_id: int
            our peer id used to identify ourselves
        :param piece_manager:
            The manager responsible to determine which pieces to request
        :param on_block_cb:
            The callback function. Called when a block is received from a remote peer
        """
        self.my_state = []
        self.peer_state = []
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_id = None
        self.writer = None
        self.reader = None
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.future = asyncio.ensure_future(self._start())

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = self.queue.get()
            logging.info(f'Got assigned peer with {ip}')

            try:
                # TODO there is a problem occurring
                self.reader, self.writer = await asyncio.open_connection(ip, port)
                logging.info(f'Connection open to peer {ip}')

                # initiate a handshake
                buffer = await self._handshake()

                # TODO add support for sending data

                # Default state for connection is: not interested and choked
                self.my_state.append('choked')

                # State that we are interested in downloading pieces
                await self._send_interested()
                self.my_state.append('interested')

                # read responses as a stream of messages
                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id,
                                                       message.index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove('pending_request')
                        self.on_block_cb(
                            peer_id=self.remote_id,
                            piece_index=message.index,
                            block_offset=message.begin,
                            data=message.block)
                    elif type(message) is Request:
                        # TODO Add support for sending data
                        logging.info('Ignoring the received Request message.')
                    elif type(message) is Cancel:
                        # TODO Add support for sending data
                        logging.info('Ignoring the received Cancel message.')

                    # Send block request to remote peer if interested
                    if 'choked' not in self.my_state:
                        if 'interested' in self.my_state:
                            if 'pending_request' not in self.my_state:
                                self.my_state.append('pending_request')
                                await self._request_piece()
            except ProtocolError as e:
                logging.exception('Protocol error')
            except (ConnectionRefusedError, TimeoutError):
                logging.warning('Unable to connect to peer')
            except (ConnectionResetError, CancelledError):
                logging.warning('Connection closed')
            except Exception as e:
                logging.exception('An error occurred')
                self.cancel()
                raise e
            self.cancel()

    def cancel(self):
        """
        Sends the cancel message to the remote peer to close the connection

        :return: None
        """
        logging.info(f'Closing peer {self.remote_id}')
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()

        self.queue.task_done()

    def stop(self):
        """
        Stop this connection from the current peer (if a connection exist) and
        from connecting to any new peer.
        """
        # Set state to stopped and cancel our future to break out of the loop.
        # The rest of the cleanup will eventually be managed by loop calling
        # `cancel`.
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()

    async def _handshake(self):
        """
        Send the initial handshake to remote peer and wait for response
        """
        self.writer.write(Handshake(self.info_hash, self.peer_id)).encode()
        self.writer.drain()

        buffer = b''
        tries = 1
        while len(buffer) < Handshake.length and tries < 10:
            tries += 1
            await self.reader.read(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buffer[:Handshake.length])

        if not response:
            raise ProtocolError('Unable receive and parse a handshake')
        if not response.info_hash == self.info_hash:
            raise ProtocolError('Handshake with invalid info_hash')

        # TODO: According to spec we should validate that the peer_id received
        # from the peer match the peer_id received from the tracker.
        self.remote_id = response.peer_id
        logging.info('Handshake with peer was successful')

        return buffer[Handshake.length:]

    async def _request_piece(self):
        block = self.piece_manager.next_request(self.remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()

            logging.debug(f'Requesting block {block.offset} for piece {block.piece} of length {block.length} bytes '
                          f'from peer {self.remote_id}')

            self.writer.write(message)
            await self.writer.drain()

    async def _send_interested(self):
        message = Interested()
        logging.debug(f'Sending message {message}')
        self.writer.write(message.encode())
        self.writer.drain()


class PeerStreamIterator:
    """
    Async iterator. Continuously reads from the given
    stream reader and tries to parse valid BitTorrent messages
    from those stream bytes

    If the connection drops or something fails, it will abort iterator
    by  raising StopAsyncIterator error ending the  calling function
    """
    CHUNK_SIZE = 10 * 1024

    def __init__(self, reader, initial: bytes = None):
        """
        :param reader:
            reader of data
        :param initial:
            bytes string to be read
        """
        self.reader = reader
        self.buffer = initial if initial else b''

    async def __aiter__(self):
        return self

    async def __anext__(self):
        """
        Read data from the socket. Parse when there is enough data

        :return:
            parsed message
        :raises
            StopAsyncIteration in case something goes wrong
        """
        while True:
            try:
                data = self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    logging.debug('No data read from stream')
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()
            except ConnectionResetError:
                logging.debug('Connection closed by peer')
                raise StopAsyncIteration()
            except CancelledError:
                raise StopAsyncIteration()
            except StopAsyncIteration as e:
                # catch to stop logging
                raise e
            except Exception:
                logging.exception('Error when iterating stream!')
                raise StopAsyncIteration()

    def parse(self):
        """
        Tries to parse protocol message if there is enough bytes to read

        :return: bytes
            the parsed message or None if no message could be parsed
        """
        # Each message is structured as:
        #     <length prefix><message ID><payload>
        #
        # The `length prefix` is a four byte big-endian value
        # The `message ID` is a decimal byte
        # The `payload` is the value of `length prefix`
        #
        # The message length is not part of the actual length. So another
        # 4 bytes needs to be included when slicing the buffer.
        header_length = 4

        if len(self.buffer) > 4:
            message_length = struct.unpack('>I', self.buffer[0:4])[0]

            if message_length == 0:
                return KeepAlive()

            if len(self.buffer) > message_length:
                message_id = struct.unpack('>b', self.buffer[4:5])[0]

                def _consume():
                    """Consume current message from the read buffer"""
                    self.buffer = self.buffer[header_length + message_length:]

                def _data():
                    """Extract current message from buffer"""
                    return self.buffer[:header_length + message_length]

                if message_id is PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                elif message_id is PeerMessage.Interested:
                    _consume()
                    return Interested()
                elif message_id is PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()
                elif message_id is PeerMessage.Choke:
                    _consume()
                    return Choke()
                elif message_id is PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()
                elif message_id is PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)
                elif message_id is PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)
                elif message_id is PeerMessage.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)
                elif message_id is PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                else:
                    logging.info('Unsupported message')
            else:
                logging.debug('Not enough data to parse in buffer')
        return None


class PeerMessage:
    """
    A message between two peers.

    All the remaining messages in the protocol take the form of:
        <length prefix><message ID><payload>

    - The length prefix is a four byte big-endian value.
    - The message ID is a single decimal byte.
    - The payload is message dependent.
    """
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9
    Handshake = None
    KeepAlive = None

    def encode(self) -> bytes:
        """Encodes this object instance into bytes"""
        pass

    @classmethod
    def decode(cls, data: bytes):
        """Decodes the given BitTorrent message into implemented type instance"""
        pass


class Handshake(PeerMessage):
    """
    The handshake message is the first message sent and then received from a
    remote peer.

    The messages are always 68 bytes long for this version of BitTorrent
    protocol.

    Message format:
        <pstrlen><pstr><reserved><info_hash><peer_id>

    In version 1.0 of the BitTorrent protocol:
        pstrlen = 19
        pstr = "BitTorrent protocol".

    Thus, length is:
        49 + len(pstr) = 68 bytes long.
    """
    length = 49 + 19

    def __init__(self, info_hash: bytes, peer_id: bytes):
        """

        :param info_hash: SHA1 hash for info dict
        :param peer_id: Unique peer id
        """
        if isinstance(info_hash, str):
            info_hash = info_hash.encode('utf-8')
        if isinstance(peer_id, str):
            peer_id = peer_id.encode('utf-8')
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack(
            '>B19s8x20s20s',
            19,  # Single byte (B)
            b'BitTorrent protocol',  # String 19s
            # Reserved 8x (pad byte, no value)
            self.info_hash,  # String 20s
            self.peer_id)  # String 20s

    @classmethod
    def decode(cls, data: bytes):
        """
        Decodes the given BitTorrent message into a handshake message, if not
        a valid message, None is returned.
        :param data: bytes
            data to decode
        :return: bytes
            decoded data or None
        """
        logging.debug(f'Decoding Handshake of length {len(data)}')
        if len(data) < (49 + 19):
            return None
        parts = struct.unpack('>B19s8x20s20s', data)
        return cls(info_hash=parts[2], peer_id=parts[3])

    def __str__(self):
        return 'Handshake'


class KeepAlive(PeerMessage):
    """
    The Keep-Alive message has no payload and length is set to zero.

    Message format:
        <len=0000>
    """

    def __str__(self):
        return 'KeepAlive'


class BitField(PeerMessage):
    """
    Message with variable length where payload is a bit array
    representing all the bits the peer has as 1 and does not as 0

    Message format:
    <len=0001+X><id=5><bifField>
    """

    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        """
        Encodes this object instance to raw bytes representing the
        entire message (ready to be transmitted)
        :return: bytes
            encoded data
        """
        bits_length = len(self.bitfield)
        return struct.pack('>Ib' + str(bits_length) + 's',
                           1 + bits_length,
                           PeerMessage.BitField,
                           self.bitfield)

    @classmethod
    def decode(cls, data: bytes):
        message_length = struct.unpack('>I', data[:4])[0]
        logging.debug(f'Decoding BitField of length {message_length}')

        parts = struct.unpack('Ib' + str(message_length - 1) + 's', data)
        return cls(parts[2])

    def __str__(self):
        return "BitField"


class Interested(PeerMessage):
    """
    The interested message is fix length and has no payload other than the
    message identifiers. It is used to notify each other about interest in
    downloading pieces.

    Message format:
        <len=0001><id=2>
    """

    def encode(self) -> bytes:
        """
        Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack('>Ib',
                           1,  # Message length
                           PeerMessage.Interested)

    def __str__(self):
        return 'Interested'


class NotInterested(PeerMessage):
    """
   The not interested message is fix length and has no payload other than the
   message identifier. It is used to notify each other that there is no
   interest to download pieces.

   Message format:
       <len=0001><id=3>
   """

    def __str__(self):
        return 'NotInterested'


class Choke(PeerMessage):
    """
    The choke message is used to tell the other peer to stop send request
    messages until unchoked.

    Message format:
        <len=0001><id=0>
    """

    def __str__(self):
        return 'Choke'


class Unchoke(PeerMessage):
    """
    Unchoking a peer enables that peer to start requesting pieces from the
    remote peer.

    Message format:
        <len=0001><id=1>
    """

    def __str__(self):
        return 'Unchoke'


class Have(PeerMessage):
    """
    Represent a piece successfully downloaded by remote peer.
    The piece is a zero based index of the torrent pieces
    """
    def __init__(self, index: int):
        self.index = index

    def encode(self) -> bytes:
        return struct.pack('>IbI', 5, PeerMessage.Have, self.index)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f'Decoding Have of length: {len(data)}')
        index = struct.unpack('>IbI', data)[2]
        return cls(index)

    def __str__(self):
        return 'Have'


class Request(PeerMessage):
    """
    The message to request a piece from peer
    """

    # Message format <len=0013><id=4><index><begin><length>
    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE):
        """

        :param index: int
            zero based piece index
        :param begin: int
            zero based offset within a piece
        :param length: int
            requested length of data
        """
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self) -> bytes:
        return struct.pack('>IbIII', 13, PeerMessage.Request,
                           self.index, self.begin, self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f'Decoding Request data of length {len(data)}')
        # tuple (message length, id, index, begin, length)
        parts = struct.unpack('IbIII', data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Request'


class Piece(PeerMessage):
    """
    'Block' of information
    format:
    <length of prefix><message ID><index><begin><block>
    """
    length = 9

    def __init__(self, index: int, begin: int, block: bytes):
        """
        :param index: int
            the zero based piece index
        :param begin: int
            the zero based offset withing a piece
        :param block: bytes
            the block of data
        """
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self) -> bytes:
        message_length = Piece.length + len(self.block)
        return struct.pack('>IbII' + str(len(self.block)) + 's',
                           message_length,
                           PeerMessage.Piece,
                           self.index,
                           self.begin,
                           self.block)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f'Decoding Piece of length {len(data)}')
        length = struct.unpack('>I', data[:4])[0]
        parts = struct.unpack('>IbII' + str(length - Piece.length) + 's',
                              data[:length + 4])
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Piece'


class Cancel(PeerMessage):
    """
    Cancels a previously requested block (in fact
    the message is identical (besides from the id) to the Request message).

    Message format:
         <len=0013><id=8><index><begin><length>
    """

    def __init__(self, index, begin, length: int = REQUEST_SIZE):
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack('>IbIII',
                           13,
                           PeerMessage.Cancel,
                           self.index,
                           self.begin,
                           self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug('Decoding Cancel of length: {length}'.format(
            length=len(data)))
        # Tuple with (message length, id, index, begin, length)
        parts = struct.unpack('>IbIII', data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Cancel'
