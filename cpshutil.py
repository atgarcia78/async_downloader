from concurrent.futures import ProcessPoolExecutor as Executor
import shutil
from pathlib import Path
from codetiming import Timer
import sys

def copysinglefile(_src, _dest):
    print(f"Init: {_src}")
    with open(_src, "rb") as fsrc:
        with open(_dest, "wb") as fdest:
            shutil.copyfileobj(fsrc, fdest, length=10485760)
    print(f"End: {_src}")

if __name__ == '__main__':
    args = sys.argv
    print(args)
    src = args[1]
    dest = args[2]
    nworkers = int(args[3])
    with Executor(nworkers) as ex:
        fut = [ex.submit(copysinglefile, str(file), str(Path(dest, file.name))) for file in Path(src).iterdir() if file.is_file()]
