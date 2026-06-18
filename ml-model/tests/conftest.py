import os
import sys

# Make `config` (project root) and `ml_model` (src/) importable in tests.
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))
