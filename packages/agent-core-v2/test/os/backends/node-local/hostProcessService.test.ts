import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'pathe';
import { PassThrough, Readable } from 'node:stream';

import { DisposableStore } from '#/_base/di/lifecycle';
import { createServices, type TestInstantiationService } from '#/_base/di/test';
import {
  HostProcessError,
  HostProcessErrorCode,
  IHostProcessService,
} from '#/os/interface/hostProcess';
import {
  HostProcessService,
  reapProcessGroup,
} from '#/os/backends/node-local/hostProcessService';

async function collect(stream: Readable): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString('utf8');
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function waitForProcessExit(pid: number, timeoutMs = 5_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && isProcessAlive(pid)) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return !isProcessAlive(pid);
}

async function waitForRecordedPid(path: string, timeoutMs = 5_000): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const text = await readFile(path, 'utf8');
      const pid = Number.parseInt(text.trim(), 10);
      if (Number.isInteger(pid) && pid > 0) return pid;
    } catch {
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`no pid was recorded at ${path}`);
}

function sendGroupSignal(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-pid, signal);
  } catch {
  }
}

describe('HostProcessService', () => {
  let disposables: DisposableStore;
  let ix: TestInstantiationService;

  beforeEach(() => {
    disposables = new DisposableStore();
    ix = createServices(disposables, {
      additionalServices: (reg) => {
        reg.define(IHostProcessService, HostProcessService);
      },
    });
  });

  afterEach(() => {
    disposables.dispose();
  });

  it('spawns a process and captures stdout + exit code', async () => {
    const svc = ix.get(IHostProcessService);
    const proc = await svc.spawn('node', ['-e', 'process.stdout.write("ok")']);
    const out = await collect(proc.stdout);
    expect(out).toBe('ok');
    expect(await proc.wait()).toBe(0);
    expect(proc.exitCode).toBe(0);
  });

  it('passes env overrides to the child', async () => {
    const svc = ix.get(IHostProcessService);
    const proc = await svc.spawn('node', ['-e', 'process.stdout.write(process.env.FOO ?? "")'], {
      env: { FOO: 'bar' },
    });
    const out = await collect(proc.stdout);
    expect(out).toBe('bar');
    expect(await proc.wait()).toBe(0);
  });

  it('throws a coded error when the command does not exist', async () => {
    const svc = ix.get(IHostProcessService);
    await expect(svc.spawn('definitely-not-a-real-command-42')).rejects.toSatisfy((err: unknown) => {
      expect(err).toBeInstanceOf(HostProcessError);
      const error = err as HostProcessError;
      expect(error.code).toBe(HostProcessErrorCode.SpawnFailed);
      expect(error.code).toBe('os.process.spawn_failed');
      expect(error.details).toMatchObject({
        command: 'definitely-not-a-real-command-42',
        errno: 'ENOENT',
      });
      expect(error.cause).toBeInstanceOf(Error);
      return true;
    });
  });

  it('terminates a running process with kill()', async () => {
    const svc = ix.get(IHostProcessService);
    const proc = await svc.spawn('node', ['-e', 'setTimeout(() => {}, 30000)']);
    expect(proc.pid).toBeGreaterThan(0);
    await proc.kill('SIGTERM');
    const code = await proc.wait();
    expect(code).not.toBe(0);
  });
});

describe('reapProcessGroup', () => {
  let dir: string;
  let children: Array<ReturnType<typeof spawn>>;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'kimi-reap-'));
    children = [];
  });

  afterEach(async () => {
    for (const child of children.splice(0)) {
      if (child.pid === undefined) continue;
      sendGroupSignal(child.pid, 'SIGKILL');
      child.kill('SIGKILL');
    }
    await rm(dir, { recursive: true, force: true });
  });

  function track(child: ReturnType<typeof spawn>): number {
    child.on('error', () => {});
    children.push(child);
    if (child.pid === undefined) throw new Error('the process did not start');
    return child.pid;
  }

  function spawnShell(command: string): number {
    return track(
      spawn('/bin/bash', ['-c', `cd /tmp && { ${command}; }`], {
        detached: true,
        stdio: 'ignore',
      }),
    );
  }

  function spawnSharedGroup(command: string, args: readonly string[]): number {
    return track(spawn(command, args as string[], { detached: false, stdio: 'ignore' }));
  }

  it('reaps a verified group leader and the grandchild it left behind', async () => {
    const pidFile = join(dir, 'grandchild.pid');
    const command = `sleep 300 & echo $! > ${pidFile}; sleep 300`;
    const pid = spawnShell(command);

    const grandchild = await waitForRecordedPid(pidFile);
    expect(isProcessAlive(pid)).toBe(true);
    expect(isProcessAlive(grandchild)).toBe(true);

    await expect(reapProcessGroup(pid, command)).resolves.toBe('reaped');

    expect(await waitForProcessExit(grandchild)).toBe(true);
    expect(await waitForProcessExit(pid)).toBe(true);
  });

  it('matches a recorded command whose newlines ps renders as octal escapes', async () => {
    const pidFile = join(dir, 'grandchild.pid');
    const command = `sleep 300 &\necho $! > ${pidFile}\nsleep 300`;
    const pid = spawnShell(command);

    const grandchild = await waitForRecordedPid(pidFile);

    await expect(reapProcessGroup(pid, command)).resolves.toBe('reaped');

    expect(await waitForProcessExit(grandchild)).toBe(true);
    expect(await waitForProcessExit(pid)).toBe(true);
  });

  it('refuses to signal a pid that is not a process group leader', async () => {
    const pid = spawnSharedGroup('sleep', ['300']);

    await expect(reapProcessGroup(pid, 'sleep 300')).resolves.toBe('not-group-leader');

    expect(isProcessAlive(pid)).toBe(true);
  });

  it('refuses to signal a group leader whose command no longer matches', async () => {
    const pid = spawnShell('sleep 300');

    await expect(reapProcessGroup(pid, 'pnpm -r test')).resolves.toBe('command-mismatch');

    expect(isProcessAlive(pid)).toBe(true);
  });

  it('ignores a pid that cannot be a live group leader', async () => {
    await expect(reapProcessGroup(0, 'sleep 300')).resolves.toBe('invalid-pid');
    await expect(reapProcessGroup(-1, 'sleep 300')).resolves.toBe('invalid-pid');
    await expect(reapProcessGroup(999_999, 'sleep 300')).resolves.toBe('not-group-leader');
  });
});

describe('HostProcessService on Windows', () => {
  let savedPlatform: string;
  let spawnedCommands: string[];
  let taskkills: Array<{ kill: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> }>;

  beforeEach(() => {
    savedPlatform = process.platform;
    Object.defineProperty(process, 'platform', { value: 'win32' });
    spawnedCommands = [];
    taskkills = [];
    vi.doMock('node:child_process', async (importOriginal) => ({
      ...(await importOriginal<typeof import('node:child_process')>()),
      spawn: (command: string, args: readonly string[]) => {
        spawnedCommands.push([command, ...args].join(' '));
        const child = Object.assign(new EventEmitter(), {
          pid: 4242,
          stdin: new PassThrough(),
          stdout: new PassThrough(),
          stderr: new PassThrough(),
          kill: vi.fn(() => true),
          unref: vi.fn(),
        });
        if (command === 'taskkill') taskkills.push(child);
        else queueMicrotask(() => child.emit('spawn'));
        return child;
      },
    }));
    vi.resetModules();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.doUnmock('node:child_process');
    vi.resetModules();
    Object.defineProperty(process, 'platform', { value: savedPlatform });
  });

  it('stops waiting for a taskkill that never exits and terminates it', async () => {
    const { HostProcessService: WindowsHostProcessService } = await import(
      '#/os/backends/node-local/hostProcessService'
    );
    const proc = await new WindowsHostProcessService().spawn('node', ['-e', 'setTimeout(() => {}, 30000)']);
    let killed = false;
    const kill = proc.kill('SIGTERM').then(() => {
      killed = true;
    });

    await vi.advanceTimersByTimeAsync(5_000);

    expect(spawnedCommands).toContain('taskkill /T /F /PID 4242');
    expect(killed).toBe(true);
    expect(taskkills).toHaveLength(1);
    expect(taskkills[0]?.kill).toHaveBeenCalled();
    expect(taskkills[0]?.unref).toHaveBeenCalled();
    await kill;
  });

  it('reaps an orphaned process group through taskkill', async () => {
    const { reapProcessGroup: reapOnWindows } = await import(
      '#/os/backends/node-local/hostProcessService'
    );
    const reaping = reapOnWindows(4242, 'node serve.mjs 4400');

    await vi.advanceTimersByTimeAsync(5_000);

    await expect(reaping).resolves.toBe('reaped');
    expect(spawnedCommands).toContain('taskkill /T /F /PID 4242');
  });
});
