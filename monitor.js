#!/usr/bin/env node

const fs = require('fs');
const chalk = require('chalk');

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

(async () => {
  acquireLock();
  const CertificateSharing = require('./index.js');
  const app = new CertificateSharing();
  await app.run();
})().catch(err => {
  console.error(chalk.red(`Fatal: ${err.message}`));
  process.exit(1);
});

