"""pytest setup: generate the gRPC stubs once and put services/common on the path."""
import subprocess
import sys
import tempfile
from pathlib import Path

SERVICES = Path(__file__).resolve().parents[1] / "services"
_gen = Path(tempfile.mkdtemp(prefix="sc-gen-"))
subprocess.run([sys.executable, "-m", "grpc_tools.protoc", f"-I{SERVICES / 'proto'}", f"--python_out={_gen}",
                f"--grpc_python_out={_gen}", *map(str, (SERVICES / "proto").glob("*.proto"))], check=True)
sys.path[:0] = [str(_gen), str(SERVICES / "common")]
