#!/usr/bin/env python3

import logging
import sys


def init_logging(level=logging.DEBUG):
    logging.basicConfig(level=level,
                        stream=sys.stderr,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
