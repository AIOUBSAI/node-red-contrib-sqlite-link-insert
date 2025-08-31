/**
 * node-red-contrib-sqlite-link-insert (inter-group lookup edition) + CONFIG FILE
 * -------------------------------------------------------------------------------
 * Keeps your original insert/lookup logic intact.
 * Adds:
 *  - useConfigFile, configPath, lockToFile, watchFile (relative to userDir, .json only)
 *  - Admin endpoints:
 *      GET  /sqlite-link-insert/template                 -> default template
 *      GET  /sqlite-link-insert/config?file=path.json    -> load JSON under userDir
 *      POST /sqlite-link-insert/config {file,config}     -> save JSON under userDir
 *  - Hot-reload when watchFile=true (affects next messages)
 */

module.exports = function (RED) {
  const fs = require('fs');
  const fsp = fs.promises;
  const path = require('path');
  const sqlite3 = require('sqlite3');

  // -----------------------------
  // Small helpers for config-file
  // -----------------------------
  const watchers = new Map(); // absPath -> {count, watcher}

  function ensureJsonExt(p) {
    if (!p || typeof p !== 'string') throw new Error('Config path is empty');
    if (!p.toLowerCase().endsWith('.json')) throw new Error('Config path must end with .json');
  }
  function resolveUnderUserDir(rel) {
    const userDir = RED.settings.userDir || process.cwd();
    const abs = path.resolve(userDir, rel);
    if (!abs.startsWith(path.resolve(userDir))) throw new Error('Config path must be under userDir');
    return abs;
  }
  async function readJsonIfExists(abs) {
    try { return JSON.parse(await fsp.readFile(abs, 'utf8')); }
    catch (e) { if (e.code === 'ENOENT') return null; throw e; }
  }
  async function writeJson(abs, obj) {
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, JSON.stringify(obj, null, 2), 'utf8');
  }

  const DEFAULT_TEMPLATE = {
    txMode: 'perTable',
    chunkSize: 500,
    continueOnError: false,
    enableWAL: true,
    syncMode: 'NORMAL',
    extraPragmas: '',
    mirrorToPayload: false,
    groups: [
      {
        table: 'Example',
        sourceType: 'msg',
        source: 'payload.items',
        autoMap: true,
        mapping: [],
        conflict: 'none',
        upsertKeys: [],
        updateColumns: [],
        keySpec: { enabled:false, mode:'byColumns', columns:[], separator:'|' },
        returnRows: { mode:'none', idColumn:'id', pathType:'msg', path:'sqlite.Example.rows' }
      }
    ]
  };

  // Admin endpoints
  RED.httpAdmin.get('/sqlite-link-insert/template', RED.auth.needsPermission('flows.read'), async (_req, res) => {
    res.json({ ok:true, template: DEFAULT_TEMPLATE });
  });
  RED.httpAdmin.get('/sqlite-link-insert/config', RED.auth.needsPermission('flows.read'), async (req, res) => {
    try {
      const rel = String(req.query.file || '').trim();
      ensureJsonExt(rel);
      const abs = resolveUnderUserDir(rel);
      const cfg = await readJsonIfExists(abs);
      res.json({ ok:true, config: cfg || DEFAULT_TEMPLATE });
    } catch (e) {
      res.status(400).json({ ok:false, error: e.message });
    }
  });
  RED.httpAdmin.post('/sqlite-link-insert/config', RED.auth.needsPermission('flows.write'), async (req, res) => {
    try {
      const { file, config } = req.body || {};
      const rel = String(file || '').trim();
      ensureJsonExt(rel);
      const abs = resolveUnderUserDir(rel);
      await writeJson(abs, config || {});
      res.json({ ok:true });
    } catch (e) {
      res.status(400).json({ ok:false, error: e.message });
    }
  });

  // -----------------------------
  // ORIGINAL UTILITIES (UNCHANGED)
  // -----------------------------
  const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
  const isArr = v => Array.isArray(v);
  const uniq = arr => Array.from(new Set(arr));
  const chunkify = (arr, n) => { const out=[]; for (let i=0;i<arr.length;i+=n) out.push(arr.slice(i,i+n)); return out; };
  const qid = (name) => { if (typeof name !== 'string' || !name.length) throw new Error(`Invalid identifier: ${name}`); return `"${name.replace(/"/g, '""')}"`; };

  const transforms = {
    none: (v) => v,
    trim: (v) => (v == null ? v : String(v).trim()),
    upper: (v) => (v == null ? v : String(v).toUpperCase()),
    lower: (v) => (v == null ? v : String(v).toLowerCase()),
    nz: (v) => { if (v == null) return null; const s = String(v).trim(); if (!s || /^N\/?A$/i.test(s)) return null; return v; },
    bool01: (v) => (v === true || v === 1 || v === '1' || /^true$/i.test(String(v)) ? 1 : 0),
    number: (v) => { if (v == null || String(v).trim() === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; },
    string: (v) => (v == null ? '' : String(v)),
  };

  async function typedGet(RED, node, msg, type, value, rowCtx) {
    switch (type) {
      case 'num':   return Number(value);
      case 'bool':  return !!value;
      case 'json':  try { return JSON.parse(value); } catch { return undefined; }
      case 'env':   return process.env[String(value)] || '';
      case 'msg':   return RED.util.getMessageProperty(msg, String(value));
      case 'flow':  return node.context().flow.get(String(value));
      case 'global':return node.context().global.get(String(value));
      case 'jsonata': {
        try {
          const expr = RED.util.prepareJSONataExpression(String(value), node);
          const dataRoot = (rowCtx !== undefined) ? { ...rowCtx, msg } : msg;
          return await new Promise((resolve, reject) => {
            RED.util.evaluateJSONataExpression(expr, dataRoot, (err, res) => {
              if (err) reject(err); else resolve(res);
            });
          });
        } catch (e) {
          node.warn(`jsonata error: ${e.message}`);
          return undefined;
        }
      }
      case 'str':
      default: return value;
    }
  }

  function applyTransform(v, name) {
    const fn = transforms[name || 'none'] || transforms.none;
    try { return fn(v); } catch { return v; }
  }

  async function mapRow(RED, node, msg, group, srcRow) {
    if (group.autoMap) {
      if (!isObj(srcRow)) return {};
      const out = {};
      Object.keys(srcRow).forEach(k => { out[k] = srcRow[k]; });
      return out;
    }
    const out = {};
    for (const m of (group.mapping || [])) {
      const col = m.col;
      if (!col) continue;

      if (m.source === 'lookup') {
        const fromGroup = (m.lookup && m.lookup.fromGroup) || '';
        const strict = !!(m.lookup && m.lookup.strict);
        const val = await typedGet(RED, node, msg, m.lookup?.valueType || 'str', m.lookup?.value, srcRow);
        const id = resolveLookupId(node, fromGroup, group._ctxMaps, group._keySpecs, val, srcRow);
        if (id == null && strict) {
          const keyShow = typeof val === 'object' ? JSON.stringify(val) : String(val);
          throw new Error(`lookup failed for group "${fromGroup}" key=${keyShow}`);
        }
        out[col] = id ?? null;
      } else {
        const raw = await typedGet(RED, node, msg, m.srcType || 'str', m.src, srcRow);
        out[col] = applyTransform(raw, m.transform || 'none');
      }
    }
    return out;
  }

  function composeKeyWithSpec(spec, provided, sep) {
    const S = sep || spec?.separator || '|';
    if (!spec || !spec.mode) return provided == null ? '' : String(provided);
    if (typeof provided === 'string' || typeof provided === 'number') return String(provided);
    if (spec.mode === 'byColumns') {
      if (Array.isArray(provided)) return provided.map(v => String(v ?? '')).join(S);
      if (isObj(provided)) return (spec.columns || []).map(c => String(provided[c] ?? '')).join(S);
      return String(provided ?? '');
    }
    return String(provided ?? '');
  }

  function resolveLookupId(node, fromAlias, ctxMaps, keySpecs, provided, _childRow) {
    const ctx = ctxMaps[fromAlias] || ctxMaps[`${fromAlias}`];
    const spec = keySpecs[fromAlias];
    if (!ctx || !ctx.map) {
      node.warn(`lookup map missing for "${fromAlias}"`);
      return null;
    }
    const key = composeKeyWithSpec(spec, provided, spec?.separator);
    return ctx.map.get(key) ?? null;
  }

  function buildUpsert(table, keys, updateCols) {
    const k = (keys || []).filter(Boolean);
    if (!k.length) return '';
    const qKeys = k.map(qid).join(', ');
    if (!updateCols || !updateCols.length) {
      return ` ON CONFLICT (${qKeys}) DO NOTHING`;
    }
    const set = updateCols.map(c => `${qid(c)}=excluded.${qid(c)}`).join(', ');
    return ` ON CONFLICT (${qKeys}) DO UPDATE SET ${set}`;
  }

  function buildInsertSQL(group, cols) {
    const table = qid(group.table);
    const colList = cols.map(qid).join(', ');
    const params = cols.map(_ => '?').join(', ');
    let head = `INSERT `;
    if (group.conflict === 'ignore') head += `OR IGNORE `;
    if (group.conflict === 'replace') head += `OR REPLACE `;
    head += `INTO ${table} (${colList}) VALUES (${params})`;
    if (group.conflict === 'upsert') {
      head += buildUpsert(group.table, group.upsertKeys || [], group.updateColumns || []);
    }
    return head;
  }

  function normKeySpec(group) {
    const ks = group.keySpec || {};
    if (!ks.enabled) return null;
    const spec = {
      enabled: true,
      alias: ks.alias || group.alias || group.table,
      mode: ks.mode || 'byColumns',
      columns: Array.isArray(ks.columns) ? ks.columns.slice() : [],
      template: ks.template || '',
      jsonata: ks.jsonata || '',
      separator: ks.separator || '|',
      selectMissing: !!ks.selectMissing,
      returnPath: ks.returnPath || null,
      idColumn: ks.idColumn || 'id'
    };
    if ((!ks.mode || ks.mode === 'byColumns') &&
        (!spec.columns || !spec.columns.length) &&
        group.conflict === 'upsert' &&
        Array.isArray(group.upsertKeys) && group.upsertKeys.length) {
      spec.columns = group.upsertKeys.slice();
    }
    return spec;
  }

  function keyOfMappedRow(mapped, spec) {
    if (!spec) return '';
    const S = spec.separator || '|';
    if (spec.mode === 'byColumns' && spec.columns && spec.columns.length) {
      return spec.columns.map(c => String(mapped[c] ?? '')).join(S);
    }
    if (spec.mode === 'byTemplate' && spec.template) {
      return String(spec.template).replace(/\{\{([^}]+)\}\}/g, (_, k) => String(mapped[k.trim()] ?? ''));
    }
    return '';
  }

  function buildKeyConcatExpr(spec) {
    const S = String(spec.separator || '|').replace(/'/g, "''");
    const parts = (spec.columns || []).map(c => `COALESCE(${qid(c)},'')`);
    return parts.join(` || '${S}' || `);
  }

  const dbRun = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, function (err) { if (err) reject(err); else resolve(this); }));
  const dbAll = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (err, rows) => { if (err) reject(err); else resolve(rows || []); }));
  const dbPrepare = (db, sql) => new Promise((resolve, reject) => db.prepare(sql, function (err) { if (err) reject(err); else resolve(this); }));
  const dbFinalize = (stmt) => new Promise((resolve, reject) => stmt.finalize((err) => err ? reject(err) : resolve()));

  async function beginTx(mode, db) { if (mode === 'off') return; await dbRun(db, 'BEGIN'); }
  async function commitTx(mode, db) { if (mode === 'off') return; await dbRun(db, 'COMMIT'); }
  async function rollbackTx(mode, db) { if (mode === 'off') return; try { await dbRun(db, 'ROLLBACK'); } catch {} }

  // -----------------------------
  // Node implementation (with config-file wrapper)
  // -----------------------------
  function SqliteLinkInsert(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Saved editor config
    node.dbPathType = config.dbPathType || 'str';
    node.dbPath = config.dbPath || '';
    node.txMode = config.txMode || 'perTable';
    node.chunkSize = Number(config.chunkSize || 500);
    node.continueOnError = !!config.continueOnError;

    node.enableWAL = !!config.enableWAL;
    node.syncMode = config.syncMode || '';
    node.extraPragmas = config.extraPragmas || '';

    node.mirrorToPayload = !!config.mirrorToPayload;
    node.groups = Array.isArray(config.groups) ? config.groups : [];

    // New config-file fields
    node.useConfigFile = !!config.useConfigFile;
    node.configPath = config.configPath || '';
    node.lockToFile = !!config.lockToFile;
    node.watchFile = !!config.watchFile;

    let lockedCfg = null;

    async function loadLockedConfigIfNeeded() {
      if (!node.useConfigFile || !node.lockToFile) { lockedCfg = null; return; }
      ensureJsonExt(node.configPath);
      const abs = resolveUnderUserDir(node.configPath);
      const cfg = await readJsonIfExists(abs);
      lockedCfg = cfg || null;
      node.status(lockedCfg ? {fill:'blue',shape:'dot',text:'cfg loaded'} : {fill:'yellow',shape:'ring',text:'cfg missing → node cfg'});
    }

    function startWatchingIfNeeded() {
      if (!node.useConfigFile || !node.watchFile) return;
      try {
        ensureJsonExt(node.configPath);
        const abs = resolveUnderUserDir(node.configPath);
        let info = watchers.get(abs);
        if (!info) {
          const w = fs.watch(abs, { persistent:false }, async (ev) => {
            if (ev === 'change' || ev === 'rename') {
              try { await loadLockedConfigIfNeeded(); node.trace('config reloaded'); }
              catch (e) { node.warn('config reload failed: '+e.message); }
            }
          });
          info = { count:0, watcher:w };
          watchers.set(abs, info);
        }
        info.count += 1;
      } catch { /* ignore if file not there yet */ }
    }
    function stopWatching() {
      if (!node.useConfigFile || !node.configPath) return;
      try {
        const abs = resolveUnderUserDir(node.configPath);
        const info = watchers.get(abs);
        if (info) {
          info.count -= 1;
          if (info.count <= 0) { info.watcher.close(); watchers.delete(abs); }
        }
      } catch {}
    }

    loadLockedConfigIfNeeded().finally(startWatchingIfNeeded);
    node.on('close', stopWatching);

    // --- INPUT (original pipeline; we only resolve effective config here) ---
    node.on('input', async (msg, send, done) => {
      const started = Date.now();
      const timings = { msOpen: 0, msExec: 0, msTotal: 0 };

      // Choose effective runtime config
      const runCfg = lockedCfg || {
        txMode: node.txMode,
        chunkSize: node.chunkSize,
        continueOnError: node.continueOnError,
        enableWAL: node.enableWAL,
        syncMode: node.syncMode,
        extraPragmas: node.extraPragmas,
        mirrorToPayload: node.mirrorToPayload,
        groups: node.groups
      };

      // Copy originals to local vars used below (so original logic stays intact)
      const local = {
        txMode: runCfg.txMode || 'perTable',
        chunkSize: Number(runCfg.chunkSize || 500),
        continueOnError: !!runCfg.continueOnError,
        enableWAL: !!runCfg.enableWAL,
        syncMode: runCfg.syncMode || '',
        extraPragmas: runCfg.extraPragmas || '',
        mirrorToPayload: !!runCfg.mirrorToPayload,
        groups: Array.isArray(runCfg.groups) ? runCfg.groups : []
      };

      try {
        // Resolve DB path using original typedGet
        const dbPath = await typedGet(RED, node, msg, node.dbPathType, node.dbPath, null);
        if (!dbPath || typeof dbPath !== 'string') throw new Error('Invalid database path');

        const db = new sqlite3.Database(dbPath);
        timings.msOpen = Date.now() - started;
        const totals = { inserted: 0, updated: 0, errors: 0, skipped: 0 };
        const byTable = {};
        const ctxMaps = {};
        const keySpecs = {};

        // PRAGMAs (from effective config)
        try {
          if (local.enableWAL) await dbRun(db, 'PRAGMA journal_mode=WAL;');
          if (local.syncMode) await dbRun(db, `PRAGMA synchronous=${local.syncMode};`);
          if (local.extraPragmas && String(local.extraPragmas).trim()) {
            for (const stmt of String(local.extraPragmas).split(';')) {
              const s = stmt.trim();
              if (s) await dbRun(db, `PRAGMA ${s};`);
            }
          }
        } catch (e) {
          node.warn(`PRAGMA warning: ${e.message}`);
        }

        if (local.txMode === 'all') await beginTx('all', db);

        for (let gi = 0; gi < local.groups.length; gi++) {
          const g = local.groups[gi];
          if (!g || !g.table) continue;

          const tableName = g.table;
          const alias = g.alias || g.table;

          const conflict = g.conflict || 'none';
          g.conflict = conflict;
          g.upsertKeys = Array.isArray(g.upsertKeys) ? g.upsertKeys : [];
          g.updateColumns = Array.isArray(g.updateColumns) ? g.updateColumns : [];

          const sourceArr = await typedGet(RED, node, msg, g.sourceType || 'msg', g.source, null);
          const rowsIn = isArr(sourceArr) ? sourceArr : [];
          const per = byTable[tableName] = (byTable[tableName] || { inserted: 0, updated: 0, errors: 0, skipped: 0, total: 0 });
          per.total += rowsIn.length;

          const ks = normKeySpec(g);
          if (ks && !ks.alias) ks.alias = alias;
          if (ks && !ks.idColumn) ks.idColumn = 'id';
          if (ks?.enabled) keySpecs[alias] = ks;

          const mapped = [];
          for (let i = 0; i < rowsIn.length; i++) {
            try {
              const m = await mapRow(RED, node, msg, Object.assign({}, g, { _ctxMaps: ctxMaps, _keySpecs: keySpecs }), rowsIn[i]);
              mapped.push(m);
            } catch (e) {
              per.errors++; totals.errors++;
              if (!local.continueOnError) throw e;
            }
          }
          if (!mapped.length) {
            if (ks?.enabled && ks?.selectMissing) ctxMaps[alias] = { map: new Map() };
            continue;
          }

          const cols = g.autoMap ? Object.keys(mapped[0]) : (g.mapping || []).map(m => m.col).filter(Boolean);
          if (!cols.length) { node.warn(`Group "${alias}" has no columns`); continue; }

          const txMode = local.txMode === 'all' ? 'off' : (local.txMode || 'perTable');
          const chunks = local.txMode === 'chunk' ? chunkify(mapped, Math.max(1, local.chunkSize)) : [mapped];

          for (const ch of chunks) {
            await beginTx(txMode, db);
            try {
              const sql = buildInsertSQL(g, cols);
              const stmt = await dbPrepare(db, sql);
              for (const r of ch) {
                const params = cols.map(c => r[c] === undefined ? null : r[c]);
                try {
                  const res = await new Promise((resolve, reject) => {
                    stmt.run(params, function (err) { if (err) reject(err); else resolve(this); });
                  });
                  if (g.conflict === 'replace') { per.updated++; totals.updated++; }
                  else if (g.conflict === 'upsert') { per.updated++; totals.updated++; }
                  else { per.inserted++; totals.inserted++; }
                } catch (e) {
                  per.errors++; totals.errors++;
                  if (!local.continueOnError) throw e;
                }
              }
              await dbFinalize(stmt);
              await commitTx(txMode, db);
            } catch (e) {
              await rollbackTx(txMode, db);
              if (!local.continueOnError) throw e;
            }
          }

          if (ks && ks.enabled) {
            const keys = [];
            if (ks.mode === 'byColumns' && ks.columns && ks.columns.length) {
              for (const r of mapped) keys.push(keyOfMappedRow(r, ks));
            } else if (ks.mode === 'byTemplate' && ks.template) {
              for (const r of mapped) keys.push(keyOfMappedRow(r, ks));
            }
            let map = new Map();
            if (ks.mode === 'byColumns' && ks.columns?.length) {
              map = await selectIdsByKeys(db, g, ks, keys);
            } else {
              node.warn(`KeySpec for "${alias}" is not byColumns — skipping SELECT map build`);
            }
            ctxMaps[alias] = { map };
            if (ks.returnPath) {
              const obj = Object.fromEntries(map);
              await assignTypedPath(RED, node, msg, ks.returnPathType || 'flow', ks.returnPath, obj);
            }
          }

          const rr = g.returnRows || { mode: 'none' };
          if (rr.mode && rr.mode !== 'none') {
            const ks2 = keySpecs[alias];
            if (ks2 && ks2.mode === 'byColumns' && ks2.columns?.length) {
              const keys = [];
              for (const r of mapped) keys.push(keyOfMappedRow(r, ks2));
              const map = await selectIdsByKeys(db, g, ks2, keys);
              const outRows = [];
              for (let i = 0; i < mapped.length; i++) {
                const k = keyOfMappedRow(mapped[i], ks2);
                const id = map.get(k);
                outRows.push({ action: 'affected', id, data: mapped[i] });
              }
              const pathType = rr.pathType || 'msg';
              const path = rr.path || `sqlite.${alias}.rows`;
              await assignTypedPath(RED, node, msg, pathType, path, outRows);
            } else {
              node.warn(`returnRows requires a byColumns KeySpec for group "${alias}" — skipped`);
            }
          }
        }
        const _end = Date.now();
        timings.msExec = _end - started - timings.msOpen;
        timings.msTotal = _end - started;
        db.close();

        const summary = {
          ok: totals.errors === 0,
          counts: totals,
          tables: byTable,
          timings
        };
        msg.sqlite = summary;
        if (local.mirrorToPayload) msg.payload = summary;

        node.status({ fill: summary.ok ? 'green' : 'red', shape: 'dot', text: `E:${totals.errors} U:${totals.updated} I:${totals.inserted}` });
        send(msg); done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: err.message });
        done(err);
      }
    });
  }

  async function selectIdsByKeys(db, group, spec, keys, chunkSize = 500) {
    const out = new Map();
    if (!keys.length) return out;
    if (spec.mode !== 'byColumns' || !spec.columns || !spec.columns.length) return out;
    const idCol = qid(spec.idColumn || 'id');
    const expr = buildKeyConcatExpr(spec);
    const table = qid(group.table);
    const chunks = chunkify(uniq(keys), chunkSize);
    for (const ch of chunks) {
      const placeholders = ch.map(_ => '?').join(', ');
      const sql = `SELECT ${idCol} AS id, (${expr}) AS _k FROM ${table} WHERE (${expr}) IN (${placeholders})`;
      const rows = await dbAll(db, sql, ch);
      for (const r of rows) out.set(String(r._k), r.id);
    }
    return out;
  }

  async function assignTypedPath(RED, node, msg, type, path, value) {
    if (!path) return;
    if (type === 'msg') RED.util.setMessageProperty(msg, path, value, true);
    else if (type === 'flow') node.context().flow.set(path, value);
    else if (type === 'global') node.context().global.set(path, value);
    else RED.util.setMessageProperty(msg, path, value, true);
  }

  RED.nodes.registerType('sqlite-link-insert', SqliteLinkInsert);
};
