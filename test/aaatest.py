import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(ROOT)
SRC = ROOT
sys.path.insert(0, SRC)