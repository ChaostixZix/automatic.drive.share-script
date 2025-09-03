#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const chalk = require('chalk');
const { spawn } = require('child_process');
const inquirer = require('inquirer');

// Single-entry monitor that wraps the main logic from index.js
// Ensures: polling every 30s by default, single instance via lock file.

if (!process.env.POLL_INTERVAL) process.env.POLL_INTERVAL = '30';
if (!process.env.LOOP) process.env.LOOP = 'true';

const LOCK_PATH = '/tmp/certificate-sharing-monitor.lock';

function acquireLock() {
  try {
    if (fs.existsSync(LOCK_PATH)) {
      const pid = parseInt(fs.readFileSync(LOCK_PATH, 'utf8'));
      if (pid && !Number.isNaN(pid)) {
        try {
          process.kill(pid, 0);
          console.log(chalk.red(`Another monitor is running (pid=${pid}). Exiting.`));
          process.exit(1);
        } catch (_) {
          // stale lock, continue
        }
      }
    }
    fs.writeFileSync(LOCK_PATH, String(process.pid));
    const cleanup = () => { try { fs.unlinkSync(LOCK_PATH); } catch (_) {} };
    process.on('exit', cleanup);
    process.on('SIGINT', () => { cleanup(); process.exit(0); });
    process.on('SIGTERM', () => { cleanup(); process.exit(0); });
  } catch (e) {
    console.log(chalk.yellow(`⚠️  Cannot create lock file: ${e.message}`));
  }
}

function findWorkerBinary() {
  try {
    const execDir = path.dirname(process.execPath);
    const execBase = path.basename(process.execPath);
    const candidates = [];
    // Try replacing 'monitor' with 'sharing' in current binary name
    if (execBase.toLowerCase().includes('monitor')) {
      candidates.push(path.join(execDir, execBase.replace(/monitor/gi, 'sharing')));
    }
    // Common names per platform
    candidates.push(path.join(execDir, 'certificate-sharing-mac'));
    candidates.push(path.join(execDir, 'certificate-sharing-linux'));
    candidates.push(path.join(execDir, 'certificate-sharing-win.exe'));
    for (const p of candidates) {
      if (fs.existsSync(p)) return p;
    }
  } catch (_) {}
  return null;
}

function runWorker(shardIndex, shardTotal) {
  const env = { 
    ...process.env, 
    SHARD_INDEX: String(shardIndex), 
    SHARD_TOTAL: String(shardTotal),
    // Batching 20 per worker as requested
    MAX_PER_RUN: process.env.MAX_PER_RUN || '20',
    // Ambil batch berikutnya dengan jeda kecil
    POLL_INTERVAL: process.env.POLL_INTERVAL || '5',
    LOOP: process.env.LOOP || 'true'
  };
  let child;
  if (process.pkg) {
    const workerBin = findWorkerBinary();
    if (!workerBin) {
      console.log(chalk.red('Worker binary tidak ditemukan di folder yang sama. Pastikan binary certificate-sharing-* ada di sebelah monitor.'));
      return null;
    }
    child = spawn(workerBin, [], { stdio: ['ignore', 'inherit', 'inherit'], env });
  } else {
    child = spawn(process.execPath, ['index.js'], { stdio: ['ignore', 'inherit', 'inherit'], env });
  }
  child.on('exit', (code, signal) => {
    console.log(chalk.yellow(`Worker ${shardIndex}/${shardTotal} exited code=${code} signal=${signal}`));
  });
  return child;
}

(async () => {
  acquireLock();

  // Tanya user jumlah worker jika tidak diset lewat env
  let shards = Number(process.env.WORKER_COUNT || NaN);
  if (!Number.isFinite(shards) || shards <= 0) {
    const answer = await inquirer.prompt([{
      type: 'number',
      name: 'workers',
      message: 'Mau jalan berapa worker paralel?',
      default: 3,
      validate: (v) => {
        const n = Number(v);
        return (Number.isFinite(n) && n > 0 && n <= 16) || 'Masukkan angka 1-16';
      }
    }]);
    shards = Number(answer.workers);
  }

  const children = [];

  console.log(chalk.cyan(`Launching ${shards} workers with sharding...`));
  for (let i = 0; i < shards; i++) {
    children.push(runWorker(i, shards));
  }

  const cleanup = () => {
    console.log(chalk.gray('Shutting down workers...'));
    for (const c of children) {
      try { c.kill('SIGTERM'); } catch (_) {}
    }
  };
  process.on('SIGINT', () => { cleanup(); process.exit(0); });
  process.on('SIGTERM', () => { cleanup(); process.exit(0); });
})().catch(err => {
  console.error(chalk.red(`Fatal: ${err.message}`));
  process.exit(1);
});
