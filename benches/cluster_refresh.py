#!/usr/bin/env python3
"""Finite multi-source replay; compare release binaries with/without refresh.

No Redis server or third-party Python packages required. All records, including
each source's final marker, must reach the sink before shutdown. Use --slow-ms
to delay each stdout read and exercise backpressure. Timing includes streaming
and draining, excludes initial connections, and is sink-inclusive (not a parser
microbenchmark). Run several alternating baseline/candidate samples.
"""
import argparse
import asyncio
import contextlib
import json
import re
import resource
import os
import signal
import time


def bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


async def run(args):
    ready = asyncio.Event()
    ports = []
    connected = 0
    queries = 0
    tasks = set()
    failures = []
    start = None
    # One GET among fifteen SETs, with larger escaped values in payload mode.
    value = b"x" * args.payload + b'\\"quoted\\"'
    records = [b'+1.0 [0 127.0.0.1:1] "%s" "key" "%s"\r\n' %
               (b"GET" if i == 0 else b"SET", value) for i in range(16)]
    block = b"".join(records)
    marker = b'+1.0 [0 127.0.0.1:1] "PING"\r\n'
    expected_input = args.sources * (args.blocks * 16 + 1)
    expected_output = args.sources * (
        args.blocks * (1 if args.reject else 16) + 1)

    async def handle(reader, writer):
        nonlocal connected, queries, start
        tasks.add(asyncio.current_task())
        try:
            while header := await reader.readline():
                command = []
                for _ in range(int(header[1:])):
                    length = int((await reader.readline())[1:])
                    command.append((await reader.readexactly(length + 2))[:-2])
                if command[0] == b"MONITOR":
                    writer.write(b"+OK\r\n")
                    await writer.drain()
                    connected += 1
                    if connected == args.sources:
                        start = time.perf_counter()
                        ready.set()
                    if connected > args.sources:
                        raise AssertionError("unchanged topology reconnected")
                    await ready.wait()
                    for _ in range(args.blocks):
                        writer.write(block)
                        await writer.drain()
                        await asyncio.sleep(0)
                    writer.write(marker)
                    await writer.drain()
                    await reader.read()
                    return
                if command[0] == b"CLUSTER":
                    queries += 1
                    topology = b"*%d\r\n" % len(ports)
                    for i, port in enumerate(ports):
                        topology += b"*3\r\n:%d\r\n:%d\r\n*3\r\n" % (
                            i * 16384 // len(ports),
                            (i + 1) * 16384 // len(ports) - 1)
                        topology += (bulk(b"127.0.0.1") + b":%d\r\n" % port +
                                     bulk(str(i).encode()))
                    writer.write(topology)
                else:
                    writer.write(b"+OK\r\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            failures.append(repr(error))
        finally:
            writer.close()
            # Discovery is deliberately cancellable, including during a reply.
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            tasks.discard(asyncio.current_task())

    servers = [await asyncio.start_server(handle, "127.0.0.1", 0)
               for _ in range(args.sources)]
    ports.extend(server.sockets[0].getsockname()[1] for server in servers)
    command = [args.binary, "--cluster", str(ports[0]), "--threads", "4",
               "--output", args.output]
    if args.refresh is not None:
        command += ["--cluster-refresh", str(args.refresh)]
    if args.reject:
        command += ["--filter", "!SET"]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True)
    stderr_task = asyncio.create_task(process.stderr.read())
    output_records = 0
    output_bytes = 0
    try:
        async with asyncio.timeout(args.timeout):
            while output_records < expected_output:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    raise AssertionError("output ended early")
                output_bytes += len(chunk)
                output_records += chunk.count(b"\n")
                if args.slow_ms:
                    await asyncio.sleep(args.slow_ms / 1000)
            elapsed = time.perf_counter() - start
            os.killpg(process.pid, signal.SIGINT)
            trailing = await process.stdout.read()
            status = await process.wait()
            stderr = (await stderr_task).decode()
        assert status == 0, stderr
        assert not trailing and output_records == expected_output
        assert not failures, failures
        summary = re.search(r"Processed (\d+) lines \(filtered: (\d+)\), "
                            r"backpressure stalls: (\d+), invalid records: (\d+)", stderr)
        assert summary and int(summary[1]) == expected_input, stderr
        assert int(summary[2]) == expected_input - expected_output, stderr
        assert int(summary[4]) == 0, stderr
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        input_bytes = args.sources * (len(block) * args.blocks + len(marker))
        print(json.dumps(dict(
            binary=args.binary, output=args.output, payload=args.payload,
            reject=args.reject, slow_ms=args.slow_ms, refresh=args.refresh,
            seconds=elapsed, input_records=expected_input,
            records_per_second=expected_input / elapsed,
            input_bytes_per_second=input_bytes / elapsed,
            output_bytes=output_bytes, queries=queries,
            stalls=int(summary[3]), cpu_seconds=usage.ru_utime + usage.ru_stime,
            peak_rss_kib=usage.ru_maxrss)))
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
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
    parser.add_argument("--refresh", type=float)
    parser.add_argument("--sources", type=int, default=4)
    parser.add_argument("--blocks", type=int, default=20000)
    parser.add_argument("--payload", type=int, default=0)
    parser.add_argument("--output", choices=["plain", "json"], default="plain")
    parser.add_argument("--reject", action="store_true")
    parser.add_argument("--slow-ms", type=float, default=0)
    parser.add_argument("--timeout", type=float, default=120)
    options = parser.parse_args()
    if options.sources < 1 or options.blocks < 1 or options.payload < 0:
        parser.error("sources/blocks must be positive; payload must be nonnegative")
    asyncio.run(run(options))
