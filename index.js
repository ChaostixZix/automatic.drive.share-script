#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');
const inquirer = require('inquirer');
const chalk = require('chalk');
// const figlet = require('figlet');
const ora = require('ora');
const cliProgress = require('cli-progress');
const Conf = require('conf');
const os = require('os');

// Configuration storage  
const config = new Conf({
  projectName: 'certificate-sharing',
  defaults: {
    sheetId: '',
    sheetName: 'participants_sample',
    parentFolderId: '',
    role: 'reader',
    dryRun: false,
    throttleMs: 2500,
    maxPerRun: 300
  }
});

class CertificateSharing {
  constructor() {
    this.auth = null;
    this.drive = null;
    this.sheets = null;
    this.progressBar = null;
    this.lastApiCallAt = 0;
    this.debugEnabled = process.env.DEBUG === 'true' || process.env.DEBUG_SHARE === 'true';
    this.logStream = null;
    this.logFilePath = null;
    // Sharding config (untuk multi-worker aman tanpa overlap)
    this.shardTotal = Number(process.env.SHARD_TOTAL || 0) || 0;
    this.shardIndex = Number(process.env.SHARD_INDEX || 0) || 0;
  }

  // Initialize local file logger
  async initLogger() {
    try {
      const logsDir = path.join(process.cwd(), 'logs');
      if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
      const ts = new Date();
      const pad = n => String(n).padStart(2, '0');
      const fname = `share-${ts.getFullYear()}${pad(ts.getMonth()+1)}${pad(ts.getDate())}-${pad(ts.getHours())}${pad(ts.getMinutes())}${pad(ts.getSeconds())}.log`;
      this.logFilePath = path.join(logsDir, fname);
      this.logStream = fs.createWriteStream(this.logFilePath, { flags: 'a' });
      this.writeLog(`Session start: ${ts.toISOString()}`);
    } catch (e) {
      console.log(chalk.yellow(`⚠️  Cannot initialize logger: ${e.message}`));
    }
  }

  // Write a line to log file with timestamp
  writeLog(message, level = 'info') {
    const ts = new Date().toISOString();
    const line = `[${ts}] [${level.toUpperCase()}] ${message}\n`;
    try {
      if (this.logStream) this.logStream.write(line);
    } catch (_) {}
  }

  // Utility: sleep with optional jitter
  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Utility: simple stable hash (djb2)
  hashKey(str) {
    const s = (str || '').toString();
    let hash = 5381;
    for (let i = 0; i < s.length; i++) {
      hash = ((hash << 5) + hash) + s.charCodeAt(i);
      hash = hash | 0; // force 32-bit
    }
    // Convert to unsigned 32-bit
    return hash >>> 0;
  }

  // Sleep with countdown display
  async sleepWithCountdown(totalSeconds) {
    const startTime = Date.now();
    let remaining = totalSeconds;
    
    while (remaining > 0) {
      // Clear current line and show countdown
      process.stdout.write(`\r⏱️  Next check in: ${remaining}s... (Ctrl+C to stop)`);
      
      await this.sleep(1000);
      remaining = totalSeconds - Math.floor((Date.now() - startTime) / 1000);
    }
    
    // Clear countdown line
    process.stdout.write('\r' + ' '.repeat(60) + '\r');
  }

  // Utility: throttle Drive API calls to avoid rate limits
  async throttle() {
    const minDelay = Number(config.get('throttleMs')) || 2500;
    const now = Date.now();
    const elapsed = now - (this.lastApiCallAt || 0);
    if (elapsed < minDelay) {
      const jitter = Math.floor(Math.random() * 400); // 0-400ms
      await this.sleep(minDelay - elapsed + jitter);
    }
    this.lastApiCallAt = Date.now();
  }

  // Detect retryable rate-limit errors
  isRetryableRateLimit(error) {
    const status = error?.response?.status || error?.code;
    const reason = error?.response?.data?.error?.errors?.[0]?.reason || error?.errors?.[0]?.reason || '';
    if (status === 429) return true;
    if (status === 403) {
      const r = String(reason);
      return (
        r.includes('rateLimitExceeded') ||
        r.includes('userRateLimitExceeded') ||
        r.includes('sharingRateLimitExceeded')
      );
    }
    return false;
  }

  // Extract structured error details for logging/debugging
  extractErrorDetails(error) {
    const status = error?.response?.status || error?.code || null;
    const dataErr = error?.response?.data?.error;
    const reasons = Array.isArray(dataErr?.errors) ? dataErr.errors.map(e => e.reason).filter(Boolean) : [];
    const message = dataErr?.message || error?.message || String(error);
    const domain = Array.isArray(dataErr?.errors) ? dataErr.errors.map(e => e.domain).filter(Boolean).join(',') : null;
    return { status, reasons, message, domain };
  }

  // Format concise error summary string
  formatErrorSummary(error) {
    const { status, reasons, message } = this.extractErrorDetails(error);
    const reasonStr = reasons && reasons.length ? reasons.join('|') : 'unknown';
    return `[HTTP ${status ?? 'n/a'}] ${reasonStr} - ${message}`;
  }

  // Optional debug logger
  dlog(...args) {
    if (this.debugEnabled) {
      const msg = args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
      console.log(chalk.gray('[DEBUG]'), msg);
      this.writeLog(msg, 'debug');
    }
  }

  // Wrap error with operation context for richer logs
  wrapError(op, ctx, error) {
    try {
      error.__op = op;
      error.__ctx = ctx;
      return error;
    } catch (_) {
      const e = new Error(`${op} failed: ${error?.message || error}`);
      e.original = error;
      e.__op = op;
      e.__ctx = ctx;
      return e;
    }
  }

  // Print beautiful header
  printHeader() {
    console.clear();
    console.log(chalk.cyan.bold('╔══════════════════════════════════════════════════════════╗'));
    console.log(chalk.cyan.bold('║                 CERTIFICATE SHARING TOOL                ║'));
    console.log(chalk.cyan.bold('║              Script Otomatis Berbagi Sertifikat         ║'));
    console.log(chalk.cyan.bold('╚══════════════════════════════════════════════════════════╝'));
    console.log();
  }

  // Setup Google authentication  
  async setupAuth() {
    const spinner = ora('🔐 Initializing Google services...').start();
    
    try {
      if (!this.logStream) await this.initLogger();
      this.writeLog('Initializing Google services...');
      // Check if service account file exists - look in current directory first
      let serviceAccountPath = path.join(process.cwd(), 'service.json');

      if (!fs.existsSync(serviceAccountPath)) {
        // If running as packaged binary, try alongside executable
        const execDir = path.dirname(process.execPath);
        serviceAccountPath = path.join(execDir, 'service.json');
      }
      
      if (!fs.existsSync(serviceAccountPath)) {
        spinner.fail();
        console.log(chalk.red('❌ File service.json tidak ditemukan!'));
        console.log(chalk.yellow(`   Dicari di: ${process.cwd()} dan ${path.dirname(process.execPath)}`));
        process.exit(1);
      }

      // Load service account
      const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, 'utf8'));
      
      // Create JWT auth
      this.auth = new google.auth.JWT(
        serviceAccount.client_email,
        null,
        serviceAccount.private_key,
        [
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets'
        ]
      );

      // Initialize services
      this.drive = google.drive({ version: 'v3', auth: this.auth });
      this.sheets = google.sheets({ version: 'v4', auth: this.auth });

      spinner.succeed(`🔐 Service Account: ${chalk.green(serviceAccount.client_email)}`);
      this.writeLog(`Service Account: ${serviceAccount.client_email}`);
      return true;
    } catch (error) {
      spinner.fail();
      console.log(chalk.red(`❌ Auth Error: ${error.message}`));
      this.writeLog(`Auth Error: ${error.message}`, 'error');
      return false;
    }
  }

  // Interactive configuration setup
  async setupConfig() {
    console.log(chalk.blue('🔧 KONFIGURASI'));
    console.log(chalk.gray('─'.repeat(40)));

    // ENV shortcut to skip prompts (non-interactive runs)
    const envSheetId = process.env.SHEET_ID;
    const envSheetName = process.env.SHEET_NAME;
    const envParentFolderId = process.env.PARENT_FOLDER_ID;
    const envDryRun = process.env.DRY_RUN;
    if (envSheetId && envSheetName) {
      config.set('sheetId', envSheetId);
      config.set('sheetName', envSheetName);
      config.set('parentFolderId', envParentFolderId || '');
      config.set('role', 'reader');
      if (typeof envDryRun === 'string') {
        config.set('dryRun', envDryRun === 'true');
      }
      console.log(chalk.green('✅ Konfigurasi dari ENV diterapkan.'));
      console.log(`   📊 Sheet ID: ${chalk.cyan(config.get('sheetId'))}`);
      console.log(`   📄 Sheet Name: ${chalk.cyan(config.get('sheetName'))}`);
      console.log(`   📁 Folder ID: ${chalk.cyan(config.get('parentFolderId') || '(semua folder)')}`);
      this.writeLog(`Config: sheetId=${config.get('sheetId')}, sheetName=${config.get('sheetName')}, parentFolderId=${config.get('parentFolderId')}`);
      return true;
    }

    // Check if config exists
    const hasConfig = config.get('sheetId') && config.get('sheetId') !== '';
    
    if (hasConfig) {
      console.log(chalk.green('✅ Konfigurasi ditemukan:'));
      console.log(`   📊 Sheet ID: ${chalk.cyan(config.get('sheetId'))}`);
      console.log(`   📄 Sheet Name: ${chalk.cyan(config.get('sheetName'))}`);
      console.log(`   📁 Folder ID: ${chalk.cyan(config.get('parentFolderId') || '(semua folder)')}`);
      console.log(`   🔗 Role: ${chalk.cyan('reader')}`);
      console.log();

      const nonInteractive = process.env.NON_INTERACTIVE === 'true' || process.env.LOOP === 'true' || !process.stdout.isTTY;
      if (nonInteractive) {
        return true; // gunakan konfigurasi yang ada tanpa prompt
      }

      const { useExisting } = await inquirer.prompt([{
        type: 'confirm',
        name: 'useExisting',
        message: '🔄 Gunakan konfigurasi yang ada?',
        default: true
      }]);

      if (useExisting) return true;
    }

    // New configuration
    console.log(chalk.yellow('🆕 Setup konfigurasi baru...'));
    console.log();

    const answers = await inquirer.prompt([
      {
        type: 'input',
        name: 'sheetId',
        message: '📊 Google Sheets ID:',
        default: config.get('sheetId'),
        validate: input => input.length > 0 || 'Sheet ID wajib diisi!'
      },
      {
        type: 'input',
        name: 'sheetName',
        message: '📄 Nama worksheet:',
        default: config.get('sheetName')
      },
      {
        type: 'input',
        name: 'parentFolderId',
        message: '📁 Parent Folder ID (optional):',
        default: config.get('parentFolderId')
      }
    ]);

    // Save configuration
    config.set('sheetId', answers.sheetId);
    config.set('sheetName', answers.sheetName);
    config.set('parentFolderId', answers.parentFolderId || '');
    config.set('role', 'reader');
    // fixed behaviors: notifications off, throttle & batching from defaults

    console.log();
    console.log(chalk.green('✅ Konfigurasi disimpan!'));
    this.writeLog(`Config saved: sheetId=${answers.sheetId}, sheetName=${answers.sheetName}, parentFolderId=${answers.parentFolderId || ''}`);
    return true;
  }

  // Get spreadsheet data
  async getSpreadsheetData() {
    const spinner = ora('📊 Membaca Google Sheets...').start();
    
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: config.get('sheetId'),
        range: `${config.get('sheetName')}!A:F`
      });

      const values = response.data.values;
      if (!values || values.length === 0) {
        spinner.fail();
        console.log(chalk.red('❌ Sheet kosong atau tidak ditemukan!'));
        return null;
      }

      const headers = values[0];
      const requiredHeaders = ['Nama', 'Email', 'isShared', 'isFolderExists', 'LastLog'];
      
      // Check required headers
      for (const header of requiredHeaders) {
        if (!headers.includes(header)) {
          spinner.fail();
          console.log(chalk.red(`❌ Kolom "${header}" tidak ditemukan!`));
          console.log(chalk.yellow(`   Headers yang ada: ${headers.join(', ')}`));
          return null;
        }
      }

      const participants = values.slice(1).map((row, index) => ({
        rowIndex: index + 2, // +2 because we skip header and 0-based to 1-based
        nama: row[headers.indexOf('Nama')] || '',
        email: row[headers.indexOf('Email')] || '',
        folderId: row[headers.indexOf('FolderId')] || '',
        isShared: row[headers.indexOf('isShared')] || '',
        isFolderExists: row[headers.indexOf('isFolderExists')] || '',
        lastLog: row[headers.indexOf('LastLog')] || ''
      }));

      spinner.succeed(`📊 Found ${chalk.green(participants.length)} participants`);
      return { participants, headers };
    } catch (error) {
      spinner.fail();
      console.log(chalk.red(`❌ Sheets Error: ${error.message}`));
      return null;
    }
  }

  // Get spreadsheet data (flexible mapping + auto-add columns)
  async getSpreadsheetDataFlexible() {
    const spinner = ora('📊 Membaca Google Sheets...').start();
    try {
      const sheetId = config.get('sheetId');
      const sheetName = config.get('sheetName');
      this.writeLog(`Reading sheet: ${sheetId} / ${sheetName}`);

      // Read header row
      const headerRes = await this.sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!1:1`
      });
      const headers = (headerRes.data.values && headerRes.data.values[0]) || [];
      if (headers.length === 0) {
        spinner.fail();
        console.log(chalk.red('❌ Sheet kosong atau header tidak ditemukan!'));
        this.writeLog('Sheet empty or missing headers', 'error');
        return null;
      }

      const lower = headers.map(h => (h || '').toString().trim().toLowerCase());
      const findIndexByNames = (candidates) => {
        for (const name of candidates) {
          const idx = lower.indexOf(name);
          if (idx !== -1) return idx;
        }
        return -1;
      };

      // Detect name/email columns
      const nameCandidates = ['nama peserta','nama','nama lengkap','name','full name','participant name'];
      const emailCandidates = ['email address','email','e-mail','gmail','participant email'];
      const nameCol = findIndexByNames(nameCandidates);
      const emailCol = findIndexByNames(emailCandidates);
      if (nameCol === -1 || emailCol === -1) {
        spinner.fail();
        console.log(chalk.red('❌ Kolom Nama/Email tidak ditemukan!'));
        console.log(chalk.yellow(`   Headers yang ada: ${headers.join(', ')}`));
        this.writeLog(`Missing name/email columns. Headers: ${headers.join(', ')}`, 'error');
        return null;
      }

      // Helper: index -> A1 column letter
      const toCol = (index) => {
        let s = '';
        let n = index + 1;
        while (n > 0) {
          const rem = (n - 1) % 26;
          s = String.fromCharCode(65 + rem) + s;
          n = Math.floor((n - 1) / 26);
        }
        return s;
      };

      // Ensure required columns appended if missing
      const extras = ['FolderId','isShared','isFolderExists','LastLog'];
      const missing = extras.filter(h => !headers.includes(h));
      if (missing.length > 0) {
        const startCol = toCol(headers.length);
        await this.sheets.spreadsheets.values.update({
          spreadsheetId: sheetId,
          range: `${sheetName}!${startCol}1`,
          valueInputOption: 'RAW',
          resource: { values: [missing] }
        });
        this.writeLog(`Added missing columns: ${missing.join(', ')}`);
        headers.push(...missing);
      }

      const folderIdCol = headers.indexOf('FolderId');
      const isSharedCol = headers.indexOf('isShared');
      const isFolderExistsCol = headers.indexOf('isFolderExists');
      const lastLogCol = headers.indexOf('LastLog');

      // Read data rows widely
      const valuesRes = await this.sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${sheetName}!A:ZZ`
      });
      const values = valuesRes.data.values || [];
      if (values.length <= 1) {
        spinner.fail();
        console.log(chalk.red('❌ Tidak ada data peserta (hanya header).'));
        this.writeLog('No participant rows (only header)', 'error');
        return null;
      }

      const participants = values.slice(1).map((row, index) => ({
        rowIndex: index + 2,
        nama: (row[nameCol] || '').toString(),
        email: (row[emailCol] || '').toString(),
        folderId: (row[folderIdCol] || '').toString(),
        isShared: (row[isSharedCol] || '').toString(),
        isFolderExists: (row[isFolderExistsCol] || '').toString(),
        lastLog: (row[lastLogCol] || '').toString()
      }));

      spinner.succeed(`📊 Found ${chalk.green(participants.length)} participants`);
      this.writeLog(`Participants: ${participants.length}`);
      return { participants, headers, columns: { nameCol, emailCol, folderIdCol, isSharedCol, isFolderExistsCol, lastLogCol, toCol } };
    } catch (error) {
      spinner.fail();
      console.log(chalk.red(`❌ Sheets Error: ${error.message}`));
      this.writeLog(`Sheets Error: ${error.message}`, 'error');
      return null;
    }
  }

  // Find folder by name
  async findFolderByName(name, parentFolderId = null) {
    // Fixed recursive search depth = 3
    const targetName = (name || '').toString();
    const targetLower = targetName.toLowerCase();
    if (!targetName) return null;

    // If no parent specified, fall back to global search by name
    if (!parentFolderId) {
      try {
        // Case-insensitive approach: use contains, then filter by exact (lowercase) match
        let query = `mimeType='application/vnd.google-apps.folder' and name contains '${targetName.replace(/'/g, "\\'")}' and trashed=false`;
        let attempt = 0;
        const maxAttempts = 5;
        while (true) {
          try {
            await this.throttle();
            const response = await this.drive.files.list({
              q: query,
              spaces: 'drive',
              fields: 'files(id,name,parents)',
              includeItemsFromAllDrives: true,
              supportsAllDrives: true,
              pageSize: 10
            });
            const files = (response.data.files || []).filter(f => (f.name || '').toLowerCase() === targetLower);
            return files && files.length > 0 ? files[0].id : null;
          } catch (err) {
            this.dlog('files.list(global) error:', this.formatErrorSummary(err));
            attempt++;
            if (this.isRetryableRateLimit(err) && attempt < maxAttempts) {
              const base = Math.min(60000, Math.pow(2, attempt) * 1000);
              const jitter = Math.floor(Math.random() * 500);
              await this.sleep(base + jitter);
              continue;
            }
            throw this.wrapError('drive.files.list', { query }, err);
          }
        }
      } catch {
        return null;
      }
    }

    // BFS up to depth 3 starting from parentFolderId
    const maxDepth = 3;
    const queue = [{ id: parentFolderId, depth: 0 }];

    const listSubfolders = async (parentId) => {
      let all = [];
      let pageToken = undefined;
      do {
        await this.throttle();
        const res = await this.drive.files.list({
          q: `'${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
          spaces: 'drive',
          fields: 'nextPageToken, files(id,name,parents)',
          includeItemsFromAllDrives: true,
          supportsAllDrives: true,
          pageSize: 100,
          pageToken
        });
        all = all.concat(res.data.files || []);
        pageToken = res.data.nextPageToken || undefined;
      } while (pageToken);
      return all;
    };

    while (queue.length) {
      const current = queue.shift();
      if (!current) break;
      if (current.depth > maxDepth) continue;
      try {
        const children = await listSubfolders(current.id);
        // Check match on this level (case-insensitive)
        const hit = children.find(f => (f.name || '').toLowerCase() === targetLower);
        if (hit) return hit.id;
        // Enqueue next level
        if (current.depth < maxDepth) {
          for (const c of children) {
            queue.push({ id: c.id, depth: current.depth + 1 });
          }
        }
      } catch (err) {
        if (this.isRetryableRateLimit(err)) {
          // Soft wait and continue
          await this.sleep(1000);
          queue.push({ id: current.id, depth: current.depth });
          continue;
        }
        this.dlog('files.list(children) error:', this.formatErrorSummary(err));
        // Non-retryable: skip this branch
      }
    }
    return null;
  }

  // Check if user has permission
  async hasPermission(fileId, email, role) {
    try {
      let attempt = 0;
      const maxAttempts = 5;
      while (true) {
        try {
          await this.throttle();
          const response = await this.drive.permissions.list({
            fileId: fileId,
            fields: 'permissions(emailAddress,role)',
            supportsAllDrives: true
          });
          const permissions = response.data.permissions || [];
          return permissions.some(p => 
            p.emailAddress && p.emailAddress.toLowerCase() === email.toLowerCase() && p.role === role
          );
        } catch (err) {
          this.dlog('permissions.list error:', this.formatErrorSummary(err));
          attempt++;
          if (this.isRetryableRateLimit(err) && attempt < maxAttempts) {
            const base = Math.min(60000, Math.pow(2, attempt) * 1000);
            const jitter = Math.floor(Math.random() * 500);
            await this.sleep(base + jitter);
            continue;
          }
          throw this.wrapError('drive.permissions.list', { fileId, email, role }, err);
        }
      }
    } catch (error) {
      return false;
    }
  }

  // Grant permission
  async grantPermission(fileId, email) {
    const role = 'reader';
    const dryRun = config.get('dryRun');

    if (dryRun) {
      return { status: 'DRY_RUN' };
    }

    let attempt = 0;
    const maxAttempts = 6;
    const capMs = 60000; // 60s
    while (true) {
      try {
        await this.throttle();
        const response = await this.drive.permissions.create({
          fileId: fileId,
          sendNotificationEmail: false,
          supportsAllDrives: true,
          resource: {
            type: 'user',
            role: role,
            emailAddress: email
          }
        });
        return response.data;
      } catch (error) {
        this.dlog('permissions.create error:', this.formatErrorSummary(error));
        attempt++;
        if (this.isRetryableRateLimit(error) && attempt < maxAttempts) {
          const base = Math.min(capMs, Math.pow(2, attempt) * 1000);
          const jitter = Math.floor(Math.random() * 500);
          await this.sleep(base + jitter);
          continue;
        }
        throw this.wrapError('drive.permissions.create', { fileId, email, role }, error);
      }
    }
  }

  // Update cell in spreadsheet
  async updateCell(row, col, value) {
    try {
      const range = `${config.get('sheetName')}!${col}${row}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: config.get('sheetId'),
        range: range,
        valueInputOption: 'RAW',
        resource: {
          values: [[value]]
        }
      });
    } catch (error) {
      console.log(chalk.yellow(`⚠️  Warning: Could not update cell ${col}${row}`));
    }
  }

  // Get current timestamp
  getCurrentTimestamp() {
    return new Date().toLocaleString('id-ID', { 
      timeZone: 'Asia/Jakarta',
      year: 'numeric',
      month: '2-digit', 
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  }

  // Process participants
  async processParticipants(data) {
    const { participants, headers, columns } = data;
    const parentFolderId = config.get('parentFolderId');
    const role = 'reader';
    const dryRun = config.get('dryRun');
    const throttleMs = Number(config.get('throttleMs')) || 2500;
    const envMax = process.env.MAX_PER_RUN ? Number(process.env.MAX_PER_RUN) : undefined;
    const maxPerRun = (Number.isFinite(envMax) && envMax > 0) ? envMax : (Number(config.get('maxPerRun')) || 300);

    console.log();
    console.log(chalk.blue('🔄 MEMPROSES PESERTA'));
    console.log(chalk.gray('─'.repeat(40)));
    console.log(chalk.cyan(`📁 Parent Folder: ${parentFolderId || 'All folders'}`));
    console.log(chalk.cyan(`🔗 Role: ${role}`));
    console.log(chalk.cyan(`🎯 Mode: ${dryRun ? 'Simulasi' : 'Production'}`));
    if (this.shardTotal > 0) {
      console.log(chalk.cyan(`🧩 Shard: ${this.shardIndex + 1}/${this.shardTotal}`));
    }
    console.log();

    if (dryRun) {
      console.log(chalk.yellow('🧪 MODE SIMULASI - Tidak ada perubahan aktual'));
    } else {
      // Production mode: always proceed without prompt
      console.log(chalk.yellow('🚀 PRODUCTION MODE: berjalan otomatis tanpa konfirmasi.'));
    }

    // Progress bar
    this.progressBar = new cliProgress.SingleBar({
      format: chalk.cyan('Progress') + ' |{bar}| {percentage}% | {value}/{total} | {status}',
      barCompleteChar: '█',
      barIncompleteChar: '░',
      hideCursor: true
    });

    let stats = { total: 0, done: 0, skipped: 0, errors: 0 };
    // Normalize and prepare list
    let normalized = participants.map(p => ({
      ...p,
      nama: (p.nama || '').toString().trim(),
      email: (p.email || '').toString().trim().toLowerCase()
    })).filter(p => p.nama && p.email);

    // Hanya proses yang belum dishare (case-insensitive)
    normalized = normalized.filter(p => String(p.isShared || '').toLowerCase() !== 'true');

    // Terapkan sharding (hindari overlap folder/permission antar worker)
    if (this.shardTotal > 0) {
      const before = normalized.length;
      normalized = normalized.filter(p => {
        // Kunci shard: utamakan FolderId (case-sensitive), fallback Nama (lowercase)
        const key = p.folderId ? String(p.folderId) : String(p.nama).toLowerCase();
        const h = this.hashKey(key);
        return (h % this.shardTotal) === this.shardIndex;
      });
      this.writeLog(`Sharding applied: ${normalized.length}/${before} records for shard ${this.shardIndex}/${this.shardTotal - 1}`);
    }

    // Apply batch limit
    const workingParticipants = normalized.slice(0, maxPerRun);

    this.progressBar.start(workingParticipants.length, 0, { status: 'Starting...' });
    this.writeLog(`Processing ${workingParticipants.length} participants. parentFolderId=${parentFolderId}`);

    const seen = new Set();
    for (const [index, participant] of workingParticipants.entries()) {
      const { rowIndex, nama, email } = participant;
      
      this.progressBar.update(index, { status: `Processing ${nama}...` });
      stats.total++;

      try {
        // Basic email validation
        if (!email.includes('@')) {
          stats.skipped++;
          await this.updateCell(rowIndex, columns.isSharedCol !== -1 ? columns.toCol(columns.isSharedCol) : 'D', 'FALSE');
          await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', `[${this.getCurrentTimestamp()}] SKIP: INVALID EMAIL '${email}'`);
          this.writeLog(`Row ${rowIndex} SKIP invalid email: ${email}`);
          continue;
        }

        // Deduplicate by (name+email)
        const key = `${nama.toLowerCase()}|${email}`;
        if (seen.has(key)) {
          stats.skipped++;
          await this.updateCell(rowIndex, 'F', `[${this.getCurrentTimestamp()}] SKIP: Duplicate entry`);
          this.writeLog(`Row ${rowIndex} SKIP duplicate: ${nama}|${email}`);
          continue;
        }
        seen.add(key);

        // Skip if already shared
        if (participant.isShared && participant.isShared.toLowerCase() === 'true') {
          stats.skipped++;
          await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', `[${this.getCurrentTimestamp()}] SKIP: Already shared`);
          this.writeLog(`Row ${rowIndex} SKIP already shared`);
          continue;
        }

        // Find folder
        let folderId = participant.folderId;
        if (!folderId) {
          folderId = await this.findFolderByName(nama, parentFolderId);
        }

        if (!folderId) {
          stats.errors++;
          await this.updateCell(rowIndex, columns.isFolderExistsCol !== -1 ? columns.toCol(columns.isFolderExistsCol) : 'E', 'FALSE');
          await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', `[${this.getCurrentTimestamp()}] FOLDER NOT FOUND: '${nama}'`);
          this.writeLog(`Row ${rowIndex} ERROR folder not found for name='${nama}'`, 'error');
          continue;
        }

        // Update folder exists
        await this.updateCell(rowIndex, columns.isFolderExistsCol !== -1 ? columns.toCol(columns.isFolderExistsCol) : 'E', 'TRUE');
        if (!participant.folderId) {
          await this.updateCell(rowIndex, columns.folderIdCol !== -1 ? columns.toCol(columns.folderIdCol) : 'C', folderId);
        }

        // Check existing permission
        const hasPermission = await this.hasPermission(folderId, email, role);
        if (hasPermission) {
          stats.skipped++;
          await this.updateCell(rowIndex, columns.isSharedCol !== -1 ? columns.toCol(columns.isSharedCol) : 'D', 'TRUE');
          await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', `[${this.getCurrentTimestamp()}] SKIP: Already has ${role} access`);
          this.writeLog(`Row ${rowIndex} SKIP already has ${role}`);
          continue;
        }

        // Grant permission
        await this.grantPermission(folderId, email);
        
        stats.done++;
        await this.updateCell(rowIndex, columns.isSharedCol !== -1 ? columns.toCol(columns.isSharedCol) : 'D', 'TRUE');
        const status = dryRun ? 'DRY_RUN' : 'GRANTED';
        await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', `[${this.getCurrentTimestamp()}] ${status} ${role} → ${email}`);
        this.writeLog(`Row ${rowIndex} ${status} ${role} -> ${email}`);

        // Optional steady throttle between participants (light jitter)
        const jitter = Math.floor(Math.random() * 200);
        await this.sleep(Math.max(0, Math.floor(throttleMs / 2)) + jitter);

      } catch (error) {
        stats.errors++;
        const summary = this.formatErrorSummary(error);
        const ctxInfo = error?.__op ? ` op=${error.__op}` : '';
        const more = error?.__ctx ? ` ctx=${JSON.stringify(error.__ctx)}` : '';
        const logLine = `[${this.getCurrentTimestamp()}] ERROR:${ctxInfo}${more} ${summary}`;
        console.log(chalk.red(`
❌ ERROR processing row ${rowIndex} (${nama}, ${email})
   ${logLine}
`));
        await this.updateCell(rowIndex, columns.isSharedCol !== -1 ? columns.toCol(columns.isSharedCol) : 'D', 'FALSE');
        await this.updateCell(rowIndex, columns.lastLogCol !== -1 ? columns.toCol(columns.lastLogCol) : 'F', logLine);
        this.writeLog(`Row ${rowIndex} ERROR ${ctxInfo}${more} ${summary}`, 'error');
      }
    }

    this.progressBar.update(workingParticipants.length, { status: 'Completed!' });
    this.progressBar.stop();

    // Final summary
    console.log();
    console.log(chalk.green('🎉 RINGKASAN EKSEKUSI'));
    console.log(chalk.gray('─'.repeat(40)));
    console.log(`📈 Total: ${chalk.cyan(stats.total)}`);
    console.log(`✅ Berhasil: ${chalk.green(stats.done)}`);
    console.log(`⏭️  Dilewati: ${chalk.yellow(stats.skipped)}`);
    console.log(`❌ Error: ${chalk.red(stats.errors)}`);
    
    const successRate = stats.total > 0 ? (stats.done / stats.total * 100).toFixed(1) : 0;
    console.log(`🎯 Success Rate: ${chalk.green(successRate + '%')}`);
    console.log();
    const summaryLine = `Summary: total=${stats.total} done=${stats.done} skipped=${stats.skipped} errors=${stats.errors} successRate=${successRate}%`;
    this.writeLog(summaryLine);
    if (this.logFilePath) console.log(chalk.gray(`📝 Log file: ${this.logFilePath}`));
    console.log(chalk.blue('✅ Proses selesai! Cek Google Sheet untuk detail lengkap.'));
  }

  // Main application flow
  async run() {
    try {
      this.printHeader();

      // Setup authentication
      if (!(await this.setupAuth())) return;

      // Setup configuration
      if (!(await this.setupConfig())) return;

      const loop = String(process.env.LOOP || '').toLowerCase() === 'true';
      const pollSec = Math.max(5, parseInt(process.env.POLL_INTERVAL || '30', 10) || 30);
      if (!loop) {
        // Single pass
        const data = await this.getSpreadsheetDataFlexible();
        if (!data) return;
        await this.processParticipants(data);
      } else {
        console.log(chalk.cyan(`🔁 Loop mode aktif. Interval: ${pollSec}s`));
        this.writeLog(`Loop mode enabled. Interval=${pollSec}s`);
        while (true) {
          try {
            const data = await this.getSpreadsheetDataFlexible();
            if (data) {
              await this.processParticipants(data);
            }
          } catch (err) {
            console.log(chalk.red(`Loop error: ${err.message}`));
            this.writeLog(`Loop error: ${err.message}`, 'error');
          }
          await this.sleepWithCountdown(pollSec);
        }
      }

    } catch (error) {
      console.log();
      console.log(chalk.red(`❌ Unexpected Error: ${error.message}`));
      process.exit(1);
    }
  }
}

// Run the application
if (require.main === module) {
  const app = new CertificateSharing();
  
  app.run()
    .then(() => {
      console.log();
      console.log(chalk.gray('Press any key to exit...'));
      if (process.stdin.isTTY && typeof process.stdin.setRawMode === 'function') {
        process.stdin.setRawMode(true);
        process.stdin.resume();
        process.stdin.once('data', () => process.exit(0));
      } else {
        process.exit(0);
      }
    })
    .catch(error => {
      console.log(chalk.red(`Fatal Error: ${error.message}`));
      process.exit(1);
    });
}

module.exports = CertificateSharing;
