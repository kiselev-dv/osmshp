# -*- coding: utf-8 -*-
import sys
import readline
import code
from datetime import datetime
from argparse import ArgumentParser

from . import Env, DBSession, RegionGroup, Region


def main(argv=sys.argv):

    argparser = ArgumentParser()

    argparser.add_argument('--config', type=str)

    args = argparser.parse_args(argv[1:])

    env = Env(args.config)

    shell = code.InteractiveConsole(dict(
        env=env,
        datetime=datetime,
        DBSession=DBSession,
        RegionGroup=RegionGroup,
        Region=Region,
    ))
    shell.interact("env")
