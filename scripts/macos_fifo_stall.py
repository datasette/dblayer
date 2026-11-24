"""macOS: a blocking writer stalls on a FIFO read through asyncio's add_reader.

The reader opens the FIFO non-blocking and reads it with loop.add_reader
(kqueue). The writer writes 39 bytes, then blocks of BLOCK bytes. With
blocks of 16 KB or more, after the reader has read the first 39 bytes,
neither side moves again: the writer is blocked in write() and the
reader's kqueue never fires. Blocks of 8 KB or less stream fine.
Seen on Darwin 27.2; not checked on Linux. Prints block size and bytes
received (all 20 blocks plus 39 when it works). Run:
    uv run python scripts/macos_fifo_stall.py
"""
import asyncio, os, tempfile, threading, sys
async def main(block):
    p = os.path.join(tempfile.mkdtemp(), "f"); os.mkfifo(p)
    r = os.open(p, os.O_RDONLY | os.O_NONBLOCK)
    def writer():
        w = os.open(p, os.O_WRONLY | os.O_NONBLOCK)
        os.set_blocking(w, True)
        os.write(w, b"x" * 39)
        import time; time.sleep(0.05)
        for _ in range(20):
            os.write(w, b"y" * block)
        os.close(w)
    t = threading.Thread(target=writer, daemon=True); t.start()
    got = 0; loop = asyncio.get_running_loop(); ev = asyncio.Event()
    def cb():
        nonlocal got
        try: d = os.read(r, 1 << 18)
        except BlockingIOError: return
        if not d: loop.remove_reader(r); ev.set(); return
        got += len(d)
    loop.add_reader(r, cb)
    try:
        await asyncio.wait_for(ev.wait(), 3)
    except TimeoutError: pass
    print(block, got)
for b in [4096, 8192, 16384]:
    asyncio.run(main(b))
