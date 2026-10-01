import type { ChildProcess } from 'node:child_process';
import { spawn } from 'node:child_process';
import { connect } from 'node:net';

const fast = process.argv.includes('--fast');
const port = Number(process.env.PORT ?? '3000');
const env = {
    ...process.env,
    PORT: String(port),
    ...(fast ? { DEMO_ACCESS_TOKEN_SECONDS: process.env.DEMO_ACCESS_TOKEN_SECONDS ?? '20' } : {})
};

function run(command: string, args: string[]): ChildProcess {
    return spawn(command, args, { env, stdio: 'inherit' });
}

async function waitForPort(): Promise<void> {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
        const ok = await new Promise<boolean>(resolve => {
            const socket = connect({ port, host: '127.0.0.1' }, () => {
                socket.destroy();
                resolve(true);
            });
            socket.on('error', () => resolve(false));
        });
        if (ok) return;
        await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`server did not bind 127.0.0.1:${port}`);
}

const server = run(process.execPath, ['--import', 'tsx', 'server.ts']);
try {
    await waitForPort();
    const client = run(process.execPath, ['--import', 'tsx', 'client.ts', '--http', `http://127.0.0.1:${port}/mcp`]);
    const code = await new Promise<number>(resolve => {
        client.on('exit', childCode => resolve(childCode ?? 1));
    });
    process.exitCode = code;
} finally {
    server.kill('SIGTERM');
}
