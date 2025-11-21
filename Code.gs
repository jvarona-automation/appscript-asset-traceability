// ================================================================================= 
// ASSET TRACEABILITY SYSTEM
// ---------------------------------------------------------------------------------
// Author: [Javier Varona / jvarona-automation] 
// Date: November 2025
//
// Description:
// Backend for Google Sheets that manages the priority of asset collection
// (for example, pallets) in a production environment. It is designed to work
// together with a front-end app (e.g. AppSheet) used by the team in charge
// of moving those assets.
//
// Core idea:
// - Give visibility to the material handling team about:
//   * How many assets are pending.
//   * Which one has the highest priority based on a due datetime or manual input.
// - Keep the queue ordered by priority and clean it automatically using TTL.
//
// Key Features:
// 1. Two-Source Merge: Combines data from a source system (ERP export, etc.)
//    with manual inputs captured from an external app (AppSheet or similar).
// 2. Priority Logic: Sorts tasks by urgency (manual overrides > source).
// 3. Smart TTL (Time-To-Live): Cleans outdated records automatically while
//    keeping the main traceability log consistent.
// 4. Concurrency: Uses LockService to avoid concurrent write conflicts.
// 5. Self-Healing: Creates or repairs the manual input sheet when headers change.
// ---------------------------------------------------------------------------------
// GENERICITY NOTE:
// - All sheet and column names are generic and can be customized.
// - To adapt this to your environment, adjust the CONFIG section.
// =================================================================================


// =================================================================================
// CONFIGURATION
// =================================================================================

// Name of the sheet where the prioritized work queue is written
const SHEET_WORK_QUEUE = 'Work_Queue';

// Name of the sheet that acts as the data source (e.g. ERP export)
const SHEET_SOURCE = 'DataSource_Table';

// Name of the sheet where manual requests or overrides are stored
// (e.g. data coming from an AppSheet app)
const SHEET_MANUAL = 'Manual_Input_Table';

// -----------------------------------------------------------------------------
// Business Rules
// -----------------------------------------------------------------------------

// EN_LINEA_REGEX:
//   - Used to decide whether a "LOCATION" belongs to the production
//     area / line of interest or should be ignored.
//   - By default, /.*/ accepts ALL rows (fully generic).
//   - In a real plant, you might use something like:
//       const EN_LINEA_REGEX = /LINE-[0-9]+/;
const EN_LINEA_REGEX = /.*/;

// EXCEPCIONES_MAP:
//   - Optional mapping for special location codes -> human-friendly line names.
//   - Example in a real setup:
//       const EXCEPCIONES_MAP = {
//         'SPECIAL-CODE-001': 'Line 1 - Odd Side'
//       };
//   - Here we keep it empty to avoid any plant-specific content.
const EXCEPCIONES_MAP = {};

const EXCEPCIONES_EN_LINEA = Object.keys(EXCEPCIONES_MAP);

// Time To Live (TTL) settings
const TTL_MINUTOS = 30;        // To release long-held, incomplete claims in the work queue
const TTL_MANUAL_MINUTOS = 30; // To expire orphan manual records in the manual sheet

// Columns for the main work queue sheet
// You can rename these headers freely, as long as your sheet matches them.
const HEADERS = [
  'ID',              // Asset ID / Pallet ID
  'LOCATION',        // Location
  'HOURS_REMAINING', // Time left until due datetime
  'TYPE',            // Deviation / type
  'SHIFT',           // Shift
  'ORIGIN',          // Derived origin / line
  'CLAIMED_BY',      // Who claimed the task
  'CLAIM_TIME',      // Claim timestamp
  'NEW_LOCATION',    // New location after handling
  'NOTES',           // Notes from the operator
  'CLAIMED',         // Boolean flag
  'ARRIVAL_TIME'     // Arrival timestamp
];

// Minimum required columns in the source sheet
// Adjust names if your source sheet uses different column labels.
const REQUIRED_SOURCE_COLS = ['ID', 'LOCATION'];

// Expected headers in the manual input sheet
const HEADERS_MANUAL = [
  'ID',
  'LOCATION',
  'CLAIMED_BY',
  'NEW_LOCATION',
  'ARRIVAL_TIME',
  'NOTES',
  'CLAIM_TIME',
  'CLAIMED',
  'FIRST_SEEN_AT',    // Critical column for manual TTL
  'MANUAL_RECORD_ID'  // Optional internal ID for the manual record
];


// =================================================================================
// MENU AND TRIGGERS
// =================================================================================

function buildMenuTrazabilidad() {
  SpreadsheetApp.getUi()
    .createMenu('Traceability')
    .addItem('Update Queue', 'actualizarHojaTrazabilidad')
    .addItem('Release Old Tasks', 'liberarTareasAntiguas')
    .addSeparator()
    .addItem('Create/Repair Manual Sheet', 'setupHojasAdicionales')
    .addToUi();
}

function setupTriggerTrazabilidad() {
  const id = SpreadsheetApp.getActive().getId();
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'buildMenuTrazabilidad')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('buildMenuTrazabilidad')
    .forSpreadsheet(id)
    .onOpen()
    .create();
}

/**
 * Creates OR REPAIRS the manual input sheet if it does not exist or is outdated.
 * Dynamically updates headers if it detects configuration changes.
 */
function setupHojasAdicionales() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = SHEET_MANUAL;
  const expectedHeaders = HEADERS_MANUAL;

  let manualSheet = ss.getSheetByName(sheetName);

  if (!manualSheet) {
    // Create sheet if it does not exist
    manualSheet = ss.insertSheet(sheetName);
    manualSheet.getRange(1, 1, 1, expectedHeaders.length)
      .setValues([expectedHeaders])
      .setFontWeight('bold');
    manualSheet.setFrozenRows(1);
    ss.toast('Sheet "' + sheetName + '" created successfully.', 'Success', 5);
  } else {
    // Repair existing sheet if headers mismatch
    const lastCol = Math.max(1, manualSheet.getLastColumn());
    const currentHeaders = manualSheet
      .getRange(1, 1, 1, lastCol)
      .getValues()[0];

    let headersMatch = currentHeaders.length === expectedHeaders.length;
    if (headersMatch) {
      for (let i = 0; i < expectedHeaders.length; i++) {
        if (currentHeaders[i] !== expectedHeaders[i]) {
          headersMatch = false;
          break;
        }
      }
    }

    if (headersMatch) {
      ss.toast('Sheet "' + sheetName + '" already exists and is up to date.', 'Info', 5);
    } else {
      ss.toast(
        'Outdated headers detected. Repairing "' + sheetName + '"...',
        'Repairing',
        5
      );
      manualSheet.getRange(1, 1, 1, lastCol).clearContent();
      manualSheet.getRange(1, 1, 1, expectedHeaders.length)
        .setValues([expectedHeaders])
        .setFontWeight('bold');
      if (manualSheet.getFrozenRows() === 0) {
        manualSheet.setFrozenRows(1);
      }
      ss.toast('Headers for "' + sheetName + '" were repaired.', 'Success', 5);
    }
  }
}

// LockService wrapper (avoids race conditions in concurrent executions)
function withDocLock(fn) {
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(20000)) {
    SpreadsheetApp.getActive().toast(
      'System busy. Please try again in a few seconds.',
      'Lock',
      5
    );
    return;
  }
  try {
    fn();
  } finally {
    lock.releaseLock();
  }
}

// Public functions (exposed to the menu)
function actualizarHojaTrazabilidad() {
  withDocLock(_actualizarHojaTrazabilidadImpl);
}

function liberarTareasAntiguas() {
  withDocLock(_liberarTareasAntiguasImpl);
}


// =================================================================================
// MAIN LOGIC
// =================================================================================

function _actualizarHojaTrazabilidadImpl() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName(SHEET_SOURCE);
  const manualSheet = ss.getSheetByName(SHEET_MANUAL);

  if (!sourceSheet) {
    SpreadsheetApp.getUi().alert(
      'Critical Error: Source sheet "' + SHEET_SOURCE + '" was not found.'
    );
    return;
  }

  ss.toast('Synchronizing data...', 'Processing', 20);

  // 1. Preserve current state (user inputs) from work queue and manual sheet
  const estadoPrevioWorkQueue = getEstadoPrevio(ss, SHEET_WORK_QUEUE);
  const estadoPrevioManual = getEstadoPrevio(ss, SHEET_MANUAL);
  const estadoPrevio = { ...estadoPrevioWorkQueue, ...estadoPrevioManual };

  // 2. Process source data (Truth Source #1)
  const lastRowSource = sourceSheet.getLastRow();
  const lastColSource = sourceSheet.getLastColumn();
  const idsFromSource = new Set();
  const workQueue = [];

  if (lastRowSource >= 2 && lastColSource >= 1) {
    const range = sourceSheet.getRange(1, 1, lastRowSource, lastColSource);
    const allData = range.getValues();
    const header = allData[0];

    try {
      requireColumns(header, REQUIRED_SOURCE_COLS);
    } catch (e) {
      return;
    }

    const colMap = {
      ID: header.indexOf('ID'),
      LOCATION: header.indexOf('LOCATION'),
      DUE_DATETIME: header.indexOf('DUE_DATETIME'),
      TYPE: header.indexOf('TYPE'),
      SHIFT: header.indexOf('SHIFT')
    };

    const now = new Date();

    for (let i = 1; i < allData.length; i++) {
      const row = allData[i];

      const location = String(row[colMap.LOCATION] || '')
        .toUpperCase()
        .trim();
      if (!location) continue;

      const isInScope =
        EN_LINEA_REGEX.test(location) ||
        EXCEPCIONES_EN_LINEA.indexOf(location) !== -1;
      if (!isInScope) continue;

      const recordId = String(row[colMap.ID] || '').trim();
      if (!recordId) continue;

      idsFromSource.add(recordId);

      let hoursRemaining = 0;
      if (colMap.DUE_DATETIME >= 0) {
        const due = toDateOrNull(row[colMap.DUE_DATETIME]);
        hoursRemaining = due ? horasRestantesSeguras(due, now) : 0;
      }

      const estado = estadoPrevio[recordId] || {};

      workQueue.push({
        ID: recordId,
        LOCATION: location,
        HOURS_REMAINING: hoursRemaining,
        TYPE:
          colMap.TYPE >= 0
            ? String(row[colMap.TYPE] || '').trim()
            : '',
        SHIFT:
          colMap.SHIFT >= 0
            ? String(row[colMap.SHIFT] || '').trim()
            : '',
        ORIGIN: getSmartOrigin(location),
        CLAIMED_BY: estado.CLAIMED_BY || '',
        CLAIM_TIME: estado.CLAIM_TIME || '',
        NEW_LOCATION: estado.NEW_LOCATION || '',
        NOTES: estado.NOTES || '',
        CLAIMED: Boolean(
          estado.CLAIMED_BY ||
          estado.NEW_LOCATION ||
          estado.ARRIVAL_TIME
        ),
        ARRIVAL_TIME: estado.ARRIVAL_TIME || ''
      });
    }
  }

  // 3. Process Manual Input (Truth Source #2) and apply TTL
  if (manualSheet && manualSheet.getLastRow() > 1) {
    const manualDataRange = manualSheet.getRange(
      1,
      1,
      manualSheet.getLastRow(),
      manualSheet.getLastColumn()
    );
    const manualData = manualDataRange.getValues();
    const manualHeader = manualData[0];

    const now = new Date();
    const ttlManualMs = TTL_MANUAL_MINUTOS * 60000;

    const manualIdx = {
      ID: manualHeader.indexOf('ID'),
      LOCATION: manualHeader.indexOf('LOCATION'),
      FIRST_SEEN_AT: manualHeader.indexOf('FIRST_SEEN_AT') // TTL index
    };

    if (manualIdx.ID !== -1 && manualIdx.LOCATION !== -1) {
      // Traverse bottom-up to safely delete rows
      for (let i = manualData.length - 1; i >= 1; i--) {
        const row = manualData[i];
        const recordId = String(row[manualIdx.ID] || '').trim();
        if (!recordId) continue;

        const rowAbs = i + 1;

        // A) If already present in source, remove from manual (cleanup)
        if (idsFromSource.has(recordId)) {
          manualSheet.deleteRow(rowAbs);
          continue;
        }

        // B) TTL logic on manual sheet
        if (manualIdx.FIRST_SEEN_AT !== -1) {
          const firstSeen = toDateOrNull(row[manualIdx.FIRST_SEEN_AT]);

          if (!firstSeen) {
            // First time seen: stamp timestamp
            manualSheet
              .getRange(rowAbs, manualIdx.FIRST_SEEN_AT + 1)
              .setValue(now);
          } else {
            // Check expiration
            const elapsedMs = now.getTime() - firstSeen.getTime();
            if (elapsedMs > ttlManualMs) {
              // Manual record expired
              manualSheet.deleteRow(rowAbs);
              continue;
            }
          }
        }

        // C) Add to work queue (if survived TTL)
        const location = String(row[manualIdx.LOCATION] || '')
          .toUpperCase()
          .trim();
        if (!location) continue;

        const estado = estadoPrevio[recordId] || {};

        workQueue.push({
          ID: recordId,
          LOCATION: location,
          HOURS_REMAINING: -100, // High priority for manual records
          TYPE: 'MANUAL',
          SHIFT: '',
          ORIGIN: getSmartOrigin(location),
          CLAIMED_BY: estado.CLAIMED_BY || '',
          CLAIM_TIME: estado.CLAIM_TIME || '',
          NEW_LOCATION: estado.NEW_LOCATION || '',
          NOTES: estado.NOTES || '',
          CLAIMED: Boolean(
            estado.CLAIMED_BY ||
            estado.NEW_LOCATION ||
            estado.ARRIVAL_TIME
          ),
          ARRIVAL_TIME: estado.ARRIVAL_TIME || ''
        });
      }
    }
  }

  // 4. Sorting (Priority: Manual overrides / negative hours first, then by hours remaining)
  workQueue.sort((a, b) => {
    const ax = isFinite(a.HOURS_REMAINING) ? a.HOURS_REMAINING : 0;
    const bx = isFinite(b.HOURS_REMAINING) ? b.HOURS_REMAINING : 0;
    return ax - bx;
  });

  // 5. Render queue into the work sheet
  escribirHojaDeTrabajo(ss, SHEET_WORK_QUEUE, workQueue);
  ss.toast(
    'Sync completed. (' + workQueue.length + ' tasks)',
    'Success',
    5
  );
}

function _liberarTareasAntiguasImpl() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_WORK_QUEUE);

  if (!sheet || sheet.getLastRow() < 2) {
    ss.toast('No tasks to release.', 'TTL', 5);
    return;
  }

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  const data = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  const header = data[0];

  const iClaimedBy = header.indexOf('CLAIMED_BY');
  const iClaimTime = header.indexOf('CLAIM_TIME');
  const iNewLoc = header.indexOf('NEW_LOCATION');
  const iArrival = header.indexOf('ARRIVAL_TIME');
  const iClaimed = header.indexOf('CLAIMED');

  if (iClaimedBy === -1 || iClaimTime === -1) {
    ss.toast('Invalid structure for cleanup.', 'Error', 5);
    return;
  }

  const now = Date.now();
  const ttlMs = TTL_MINUTOS * 60000;

  const rowsToClearClaimedBy = [];
  const rowsToClearClaimTime = [];
  const rowsToSetClaimedFalse = [];

  for (let r = 1; r < data.length; r++) {
    const claimedBy = data[r][iClaimedBy];
    const claimTime = data[r][iClaimTime];

    const completed = Boolean(
      (iNewLoc !== -1 && data[r][iNewLoc]) ||
      (iArrival !== -1 && data[r][iArrival])
    );

    const claimOk = claimTime instanceof Date && !isNaN(claimTime);

    // Rule: if claimed for too long without completion -> release
    if (claimedBy && claimOk && now - claimTime.getTime() > ttlMs && !completed) {
      rowsToClearClaimedBy.push(columnToLetter(iClaimedBy + 1) + (r + 1));
      rowsToClearClaimTime.push(columnToLetter(iClaimTime + 1) + (r + 1));
      if (iClaimed !== -1) {
        rowsToSetClaimedFalse.push(columnToLetter(iClaimed + 1) + (r + 1));
      }
    }
  }

  if (rowsToClearClaimedBy.length) {
    sheet.getRangeList(rowsToClearClaimedBy).clearContent();
    sheet.getRangeList(rowsToClearClaimTime).clearContent();
    if (rowsToSetClaimedFalse.length) {
      sheet.getRangeList(rowsToSetClaimedFalse).setValue(false);
    }
    ss.toast(
      rowsToClearClaimedBy.length + ' tasks were released due to inactivity.',
      'Maintenance',
      5
    );
  } else {
    ss.toast('Nothing to clean up.', 'Maintenance', 3);
  }
}


// =================================================================================
// UTILITIES
// =================================================================================

function requireColumns(header, required) {
  const missing = required.filter(c => header.indexOf(c) === -1);
  if (missing.length) {
    SpreadsheetApp.getUi().alert(
      'Missing required columns in "' + SHEET_SOURCE + '": ' + missing.join(', ')
    );
    throw new Error('Missing required columns.');
  }
}

function toDateOrNull(v) {
  if (v instanceof Date && !isNaN(v)) return v;

  if (typeof v === 'number' && isFinite(v)) {
    const d = new Date(v);
    return isNaN(d) ? null : d;
  }

  if (typeof v === 'string' && v.trim()) {
    const d = new Date(v);
    return isNaN(d) ? null : d;
  }

  return null;
}

function horasRestantesSeguras(dueDatetime, now) {
  return (dueDatetime.getTime() - now.getTime()) / 3600000;
}

function getEstadoPrevio(spreadsheet, sheetName) {
  const sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() < 2) return {};

  const data = sheet.getRange(
    1,
    1,
    sheet.getLastRow(),
    sheet.getLastColumn()
  ).getValues();
  const header = data[0];

  const iID = header.indexOf('ID');
  if (iID === -1) return {};

  const idx = {
    CLAIMED_BY:   header.indexOf('CLAIMED_BY'),
    CLAIM_TIME:   header.indexOf('CLAIM_TIME'),
    NEW_LOCATION: header.indexOf('NEW_LOCATION'),
    NOTES:        header.indexOf('NOTES'),
    CLAIMED:      header.indexOf('CLAIMED'),
    ARRIVAL_TIME: header.indexOf('ARRIVAL_TIME')
  };

  const out = {};
  for (let r = 1; r < data.length; r++) {
    const recordId = String(data[r][iID] || '');
    if (!recordId) continue;

    out[recordId] = {
      CLAIMED_BY:   idx.CLAIMED_BY   === -1 ? ''    : data[r][idx.CLAIMED_BY],
      CLAIM_TIME:   idx.CLAIM_TIME   === -1 ? ''    : data[r][idx.CLAIM_TIME],
      NEW_LOCATION: idx.NEW_LOCATION === -1 ? ''    : data[r][idx.NEW_LOCATION],
      NOTES:        idx.NOTES        === -1 ? ''    : data[r][idx.NOTES],
      CLAIMED:      idx.CLAIMED      === -1 ? false : data[r][idx.CLAIMED],
      ARRIVAL_TIME: idx.ARRIVAL_TIME === -1 ? ''    : data[r][idx.ARRIVAL_TIME]
    };
  }

  return out;
}

function escribirHojaDeTrabajo(spreadsheet, sheetName, data) {
  let sheet = spreadsheet.getSheetByName(sheetName);

  if (sheet) {
    sheet.clearContents();
  } else {
    sheet = spreadsheet.insertSheet(sheetName);
  }

  if (!data.length) {
    sheet.getRange('A1').setValue('No pending tasks.');
    return;
  }

  const rows = data.map(obj =>
    HEADERS.map(h => (obj[h] !== undefined ? obj[h] : ''))
  );

  sheet.getRange(1, 1, 1, HEADERS.length)
    .setValues([HEADERS])
    .setFontWeight('bold');
  sheet.setFrozenRows(1);

  sheet.getRange(2, 1, rows.length, HEADERS.length)
    .setValues(rows);

  const hoursCol = HEADERS.indexOf('HOURS_REMAINING') + 1;
  if (hoursCol > 0) {
    sheet
      .getRange(2, hoursCol, rows.length, 1)
      .setNumberFormat('0.00 "h"');
  }

  sheet.autoResizeColumns(1, HEADERS.length);
}

function getSmartOrigin(location) {
  try {
    const loc = String(location || '').toUpperCase().trim();
    if (!loc) return '';

    // If the address is in the exceptions map, use that
    if (EXCEPCIONES_MAP[loc]) return EXCEPCIONES_MAP[loc];

    // Generic logic:
    // Assumes the last segment may indicate a line or zone number.
    const parts = loc.split(/[-\s]/); // split by dash or space
    const last = parts[parts.length - 1];
    const num = parseInt(last, 10);

    if (!isFinite(num)) {
      // If we can't infer a number, just return the original string
      return loc;
    }

    return 'Line ' + num;
  } catch (e) {
    return location;
  }
}

function columnToLetter(column) {
  let temp = '';
  let col = column;

  while (col > 0) {
    const remainder = (col - 1) % 26;
    temp = String.fromCharCode(65 + remainder) + temp;
    col = Math.floor((col - 1) / 26);
  }

  return temp;
}
