#!/usr/bin/env python3
"""Finite multi-server output replay using only the Python standard library.

Run against portable release binaries, alternating baseline/candidate samples.
Includes socket ingestion, filtering, formatting, and draining stdout. The sink
counts records and bytes; --slow-ms delays reads to exercise backpressure.
"""
import argparse
import asyncio
import contextlib
import json
import resource
import signal
import time


async def run(args):
    ready = asyncio.Event()
    connected = 0
    started = None
    tasks = set()
    failures = []
    value = b"x" * args.payload + b'\\"quoted\\"'
    block = b"".join(
        b'+1.5 [0 127.0.0.1:49152] "%s" "key" "%s"\r\n'
        % (b"GET" if i % 16 == 0 else b"SET", value)
        for i in range(1024)
    )
    marker = b'+1.5 [0 lua] "PING"\r\n'
    input_records = args.sources * (args.blocks * 1024 + 1)
    expected = args.sources * (args.blocks * (64 if args.reject else 1024) + 1)

    async def handle(reader, writer):
        nonlocal connected, started
        tasks.add(asyncio.current_task())
        try:
            while header := await reader.readline():
                command = []
                for _ in range(int(header[1:])):
                    length = int((await reader.readline())[1:])
                    command.append((await reader.readexactly(length + 2))[:-2])
                writer.write(b"+OK\r\n")
                await writer.drain()
                if command[0] == b"MONITOR":
                    connected += 1
                    if connected == args.sources:
                        started = time.perf_counter()
                        ready.set()
                    await ready.wait()
                    for _ in range(args.blocks):
                        writer.write(block)
                        await writer.drain()
                    writer.write(marker)
                    await writer.drain()
                    await reader.read()
                    return
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            failures.append(repr(error))
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            tasks.discard(asyncio.current_task())

    servers = [await asyncio.start_server(handle, "127.0.0.1", 0)
               for _ in range(args.sources)]
    addresses = [str(server.sockets[0].getsockname()[1]) for server in servers]
    command = [args.binary, "--threads", "4", "--output", args.output, *addresses]
    if args.source:
        command += ["--source"]
    if args.format is not None:
        command += ["--format", args.format]
    if args.reject:
        command += ["--filter", "!SET"]
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stderr_task = asyncio.create_task(process.stderr.read())
    records = output_bytes = 0
    try:
        async with asyncio.timeout(120):
            while records < expected:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    raise AssertionError("output ended early")
                output_bytes += len(chunk)
                records += chunk.count(b"\n")
                if args.slow_ms:
                    await asyncio.sleep(args.slow_ms / 1000)
            elapsed = time.perf_counter() - started
            process.send_signal(signal.SIGINT)
            trailing = await process.stdout.read()
            status = await process.wait()
            stderr = (await stderr_task).decode()
        assert status == 0, stderr
        assert records == expected and not trailing
        assert not failures, failures
        assert f"Processed {input_records} lines (filtered: {input_records - expected})" in stderr, stderr
        assert "invalid records: 0" in stderr, stderr
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        input_bytes = args.sources * (len(block) * args.blocks + len(marker))
        print(json.dumps(dict(
            binary=args.binary, output=args.output, source=args.source, format=args.format, payload=args.payload,
            sources=args.sources, blocks=args.blocks, reject=args.reject,
            slow_ms=args.slow_ms, seconds=elapsed,
            input_records_per_second=input_records / elapsed,
            input_bytes_per_second=input_bytes / elapsed,
            output_bytes=output_bytes, output_records=records,
            cpu_seconds=usage.ru_utime + usage.ru_stime,
            peak_rss_kib=usage.ru_maxrss)))
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
        await stderr_task
        for server in servers:
            server.close()
            await server.wait_closed()
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--sources", type=int, default=4)
    parser.add_argument("--blocks", type=int, default=512)
    parser.add_argument("--payload", type=int, default=0)
    parser.add_argument("--output", choices=["plain", "json", "json-source"], default="json")
    parser.add_argument("--source", action="store_true")
    parser.add_argument("--format")
    parser.add_argument("--reject", action="store_true")
    parser.add_argument("--slow-ms", type=float, default=0)
    options = parser.parse_args()
    if options.sources < 1 or options.blocks < 1 or options.payload < 0 or options.slow_ms < 0:
        parser.error("sources/blocks must be positive; payload/slow-ms must be nonnegative")
    asyncio.run(run(options))
