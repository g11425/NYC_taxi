import sys
import os


def abc():
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    print(ROOT)
    SRC = ROOT
    sys.path.insert(0, SRC)
    print(sys.path)
    assert 1+1 == 2
