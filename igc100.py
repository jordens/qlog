import docopt
import time
from telnetlib import Telnet

import requests

__doc__ = """
IGC100 Qlog Feeder

Usage:
    igc100 <host> <port> <interval> <url>
"""

args = docopt.docopt(__doc__)
tel = Telnet(args["<host>"], int(args["<port>"]))

while True:
    try:
        tel.write(b"GDAT? 1\r\n")
        v = tel.read_until(b"\r", 10).strip()
        requests.post(args["<url>"], data={"value": float(v)})
    except Exception as e:
        print(e)
    time.sleep(float(args["<interval>"]))
