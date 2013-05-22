from collections import namedtuple
import textwrap


SqlStatement = namedtuple('SqlStatement', ('sql', 'log'))


def dedent(t):
    return textwrap.dedent(t)[1:]


def indent(t, spaces):
    return ('\n' + '  ' * spaces).join(t.split('\n'))
