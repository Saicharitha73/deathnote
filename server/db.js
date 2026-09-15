const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');

const dbPath = process.env.DATABASE_PATH || path.join(__dirname, 'death_code.sqlite');
const db = new DatabaseSync(dbPath);

// Enable WAL mode, busy_timeout, and normal sync to avoid database locks
try {
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA busy_timeout = 5000;
    PRAGMA synchronous = NORMAL;
  `);
} catch (pragmaErr) {
  console.warn('Warning configuring SQLite PRAGMAs:', pragmaErr.message);
}

// Initialize schema
function initDatabase() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT DEFAULT 'DEATH CODE: THE KIRA PROTOCOL',
      status TEXT DEFAULT 'LOBBY',
      previous_status TEXT,
      total_duration_minutes INTEGER DEFAULT 45,
      round_duration_r1 INTEGER DEFAULT 10,
      round_duration_r2 INTEGER DEFAULT 10,
      round_duration_r3 INTEGER DEFAULT 10,
      round_duration_r4 INTEGER DEFAULT 15,
      time_remaining_seconds INTEGER DEFAULT 2700,
      is_paused INTEGER DEFAULT 0,
      suspense_mode INTEGER DEFAULT 1,
      anti_cheat_enabled INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS questions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      round_id INTEGER NOT NULL,
      category TEXT NOT NULL,
      difficulty TEXT NOT NULL,
      concept TEXT NOT NULL,
      question_text TEXT NOT NULL,
      code_snippet TEXT,
      option_a TEXT NOT NULL,
      option_b TEXT NOT NULL,
      option_c TEXT NOT NULL,
      option_d TEXT NOT NULL,
      correct_option TEXT NOT NULL,
      points INTEGER DEFAULT 1,
      hint_text TEXT,
      explanation TEXT
    );

    CREATE TABLE IF NOT EXISTS cases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_code TEXT UNIQUE NOT NULL,
      binary_code TEXT NOT NULL,
      decimal_id INTEGER NOT NULL,
      kira_suspect_code TEXT NOT NULL,
      kira_name TEXT NOT NULL,
      story_title TEXT,
      description TEXT
    );

    CREATE TABLE IF NOT EXISTS suspects (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_id INTEGER NOT NULL,
      suspect_code TEXT NOT NULL,
      name TEXT NOT NULL,
      age INTEGER NOT NULL,
      city TEXT NOT NULL,
      crimes INTEGER NOT NULL,
      occupation TEXT,
      status TEXT
    );

    CREATE TABLE IF NOT EXISTS login_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_id INTEGER NOT NULL,
      suspect_code TEXT NOT NULL,
      login_time TEXT NOT NULL,
      device TEXT NOT NULL,
      ip_address TEXT,
      location TEXT
    );

    CREATE TABLE IF NOT EXISTS crime_records (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_id INTEGER NOT NULL,
      suspect_code TEXT NOT NULL,
      crime_type TEXT NOT NULL,
      crime_count INTEGER NOT NULL,
      target_type TEXT,
      weapon_or_method TEXT
    );

    CREATE TABLE IF NOT EXISTS transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_id INTEGER NOT NULL,
      suspect_code TEXT NOT NULL,
      amount INTEGER NOT NULL,
      merchant TEXT NOT NULL,
      location TEXT NOT NULL,
      time_stamp TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS investigation_clues (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_id INTEGER NOT NULL,
      clue_number INTEGER NOT NULL,
      clue_text TEXT NOT NULL,
      target_table TEXT NOT NULL,
      question_text TEXT NOT NULL,
      option_a TEXT NOT NULL,
      option_b TEXT NOT NULL,
      option_c TEXT NOT NULL,
      option_d TEXT NOT NULL,
      correct_option TEXT NOT NULL,
      bit_value INTEGER NOT NULL,
      hint_text TEXT
    );

    CREATE TABLE IF NOT EXISTS participants (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      reg_id TEXT UNIQUE NOT NULL,
      team_name TEXT NOT NULL,
      member_names TEXT,
      college TEXT,
      email TEXT,
      assigned_case_id INTEGER,
      current_round INTEGER DEFAULT 1,
      score INTEGER DEFAULT 0,
      round1_score INTEGER DEFAULT 0,
      round2_score INTEGER DEFAULT 0,
      round3_score INTEGER DEFAULT 0,
      round4_score INTEGER DEFAULT 0,
      lives INTEGER DEFAULT 10,
      is_eliminated INTEGER DEFAULT 0,
      ryuk_deal_active INTEGER DEFAULT 0,
      curse_active INTEGER DEFAULT 0,
      hint_used_r2 INTEGER DEFAULT 0,
      binary_bits TEXT DEFAULT '______',
      identified_kira_code TEXT,
      is_completed INTEGER DEFAULT 0,
      tab_switches INTEGER DEFAULT 0,
      start_time DATETIME,
      finish_time DATETIME,
      last_active DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS participant_questions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      participant_id INTEGER NOT NULL,
      round_id INTEGER NOT NULL,
      question_id INTEGER,
      clue_id INTEGER,
      order_num INTEGER NOT NULL,
      shuffled_option_a TEXT NOT NULL,
      shuffled_option_b TEXT NOT NULL,
      shuffled_option_c TEXT NOT NULL,
      shuffled_option_d TEXT NOT NULL,
      mapped_correct_option TEXT NOT NULL,
      selected_option TEXT,
      is_answered INTEGER DEFAULT 0,
      is_correct INTEGER DEFAULT 0,
      points_awarded INTEGER DEFAULT 0,
      answered_at DATETIME
    );

    CREATE TABLE IF NOT EXISTS audit_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      participant_id INTEGER,
      event_type TEXT NOT NULL,
      details TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Safe schema migrations for existing database
  try { db.exec('ALTER TABLE participants ADD COLUMN round4_score INTEGER DEFAULT 0;'); } catch (_) {}
  try { db.exec('ALTER TABLE participants ADD COLUMN is_eliminated INTEGER DEFAULT 0;'); } catch (_) {}
  try { db.exec('ALTER TABLE events ADD COLUMN round_duration_r4 INTEGER DEFAULT 15;'); } catch (_) {}

  // Ensure an event record exists and reset clock to full 45:00 at ROUND_1
  const event = db.prepare(`SELECT * FROM events LIMIT 1`).get();
  if (!event) {
    db.prepare(`
      INSERT INTO events (title, status, time_remaining_seconds, is_paused)
      VALUES ('DEATH CODE: THE KIRA PROTOCOL', 'ROUND_1', 2700, 1)
    `).run();
  } else {
    db.prepare(`UPDATE events SET status = 'ROUND_1', time_remaining_seconds = 2700, is_paused = 1 WHERE id = ?`).run(event.id);
  }
}

initDatabase();

module.exports = {
  db,
  initDatabase
};
