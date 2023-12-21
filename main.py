from bencoding import Decoder
from bencoding import Encoder
from collections import OrderedDict

with open("C:\\Users\\User\\Downloads\\ubuntu-16.04-desktop-amd64.iso.torrent", 'rb') as f:
    file = f.read()
    data = Decoder(file).decode()

print(data)
