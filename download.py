"""
Convenience entrypoint.

Run `python download.py` to execute the download->normalize->upload pipeline.
"""

import runpy
from pathlib import Path


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    runpy.run_path(str(here / "scripts" / "download.py"), run_name="__main__")

