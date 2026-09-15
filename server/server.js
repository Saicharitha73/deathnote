const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const { db, initDatabase } = require('./db');
const { generateParticipantQuestions } = require('./randomizer');
const { seedAll } = require('./seed_data');

const path = require('path');
const fs = require('fs');

initDatabase();

// Ensure the database is seeded if it's empty (e.g. first run or fresh deployment)
const qCount = db.prepare('SELECT COUNT(*) as count FROM questions').get();
if (!qCount || qCount.count === 0) {
  console.log('Database questions empty. Running initial seed...');
  seedAll();
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(cors());
app.use(express.json());

// Healthcheck endpoint for Railway & monitoring services
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', service: 'death-code-kira-protocol', timestamp: new Date().toISOString() });
});

// Serve static client bundle if built (no-cache enabled to prevent stale bundles)
const clientDistPath = path.join(__dirname, '..', 'client', 'dist');
if (fs.existsSync(clientDistPath)) {
  app.use(express.static(clientDistPath, {
    etag: false,
    maxAge: 0,
    setHeaders: (res) => {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  }));
}

const PORT = process.env.PORT || 5000;

// Prevent any unhandled error from crashing the server process
process.on('uncaughtException', (err) => {
  console.error('Server unhandled exception caught:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('Server unhandled rejection caught:', reason);
});

// Broadcast helper for WebSockets
function broadcast(data) {
  const message = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// -------------------------------------------------------------
// TIMER & ROUND STATE MACHINE (IN-MEMORY WITH PERIODIC PERSIST)
// -------------------------------------------------------------
let timerInterval = null;
let cachedEventState = null;
let timerTickCount = 0;

function getEventState() {
  if (!cachedEventState) {
    try {
      cachedEventState = db.prepare('SELECT * FROM events ORDER BY id DESC LIMIT 1').get();
    } catch (err) {
      console.error('Error fetching event state from DB:', err.message);
    }
  }
  return cachedEventState;
}

function updateEventState(updates) {
  if (cachedEventState) {
    Object.assign(cachedEventState, updates);
  }
  try {
    const keys = Object.keys(updates);
    const setClauses = keys.map((k) => `${k} = ?`).join(', ');
    const values = Object.values(updates);
    db.prepare(`UPDATE events SET ${setClauses} WHERE id = (SELECT id FROM events ORDER BY id DESC LIMIT 1)`).run(...values);
  } catch (err) {
    console.warn('Non-fatal updateEventState DB notice:', err.message);
  }
  return getEventState();
}

function startTimer() {
  if (timerInterval) clearInterval(timerInterval);

  timerInterval = setInterval(() => {
    const event = getEventState();
    if (!event || event.status === 'LOBBY' || event.status === 'ENDED' || event.is_paused === 1) {
      return;
    }

    let timeLeft = event.time_remaining_seconds - 1;
    let newStatus = event.status;
    let statusChanged = false;

    // Time boundaries (45:00 total = 2700s)
    // R1: 45:00 - 35:00 (remaining: 2700s down to 2100s)
    // R2: 35:00 - 25:00 (remaining: 2100s down to 1500s)
    // R3: 25:00 - 15:00 (remaining: 1500s down to 900s)
    // R4: 15:00 - 00:00 (remaining: 900s down to 0s)
    if (timeLeft <= 0) {
      timeLeft = 0;
      newStatus = 'ENDED';
      statusChanged = true;
      clearInterval(timerInterval);
    } else if (timeLeft <= 900 && event.status !== 'ROUND_4') {
      newStatus = 'ROUND_4';
      statusChanged = true;
      broadcast({ type: 'ANNOUNCEMENT', title: 'ROUND 4: FINAL INVESTIGATION', message: "L's Final Investigation initiated. Examine the suspect database and extract the binary key to arrest Kira." });
    } else if (timeLeft <= 1500 && timeLeft > 900 && event.status !== 'ROUND_3' && event.status !== 'ROUND_4') {
      newStatus = 'ROUND_3';
      statusChanged = true;
      broadcast({ type: 'ANNOUNCEMENT', title: 'ROUND 3: SPK SURVEILLANCE', message: "Near's Deduction & SPK Surveillance activated. Analyze complex relational data pipelines." });
    } else if (timeLeft <= 2100 && timeLeft > 1500 && event.status !== 'ROUND_2' && event.status !== 'ROUND_3' && event.status !== 'ROUND_4') {
      newStatus = 'ROUND_2';
      statusChanged = true;
      broadcast({ type: 'ANNOUNCEMENT', title: 'ROUND 2: KIRA\'S CODE', message: "Kira's Code has commenced. Reconstruct incomplete SQL queries." });
    }

    // In-memory update
    event.time_remaining_seconds = timeLeft;
    event.status = newStatus;

    // Only write to SQLite disk on round change or every 30 seconds to prevent OneDrive locking
    timerTickCount++;
    if (statusChanged || timerTickCount % 30 === 0) {
      try {
        db.prepare('UPDATE events SET time_remaining_seconds = ?, status = ? WHERE id = (SELECT id FROM events ORDER BY id DESC LIMIT 1)').run(timeLeft, newStatus);
      } catch (err) {
        console.warn('Periodic timer DB checkpoint deferred:', err.message);
      }
    }

    broadcast({
      type: 'TICK',
      time_remaining_seconds: timeLeft,
      status: newStatus
    });
  }, 1000);
}

// Start timer loop initially
startTimer();

// -------------------------------------------------------------
// WEBSOCKET HANDLERS
// -------------------------------------------------------------
wss.on('connection', (ws) => {
  const event = getEventState();
  ws.send(JSON.stringify({
    type: 'INIT',
    event
  }));
});

// -------------------------------------------------------------
// REST APIS: AUTHENTICATION & EVENT CONTROLS
// -------------------------------------------------------------

// Get event status
app.get('/api/event/status', (req, res) => {
  res.json({ event: getEventState() });
});

// Reset event timer to 45:00 at ROUND_1
app.post('/api/event/reset', (req, res) => {
  try {
    const event = updateEventState({
      status: 'ROUND_1',
      time_remaining_seconds: 2700,
      is_paused: 0
    });
    startTimer();
    broadcast({ type: 'EVENT_UPDATE', event });
    broadcast({ type: 'TICK', time_remaining_seconds: 2700, status: 'ROUND_1' });
    broadcast({
      type: 'ANNOUNCEMENT',
      title: 'EVENT RESET',
      message: 'The Kira Protocol clock has been reset to 45:00. Round 1 is ACTIVE.'
    });
    res.json({ success: true, event });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Prepare pre-entry: pauses the timer and locks at 45:00 until participant clicks Enter The Notebook
app.post('/api/event/prepare-entry', (req, res) => {
  try {
    if (timerInterval) {
      clearInterval(timerInterval);
      timerInterval = null;
    }
    const event = updateEventState({
      status: 'ROUND_1',
      time_remaining_seconds: 2700,
      is_paused: 1
    });
    broadcast({ type: 'EVENT_UPDATE', event });
    broadcast({ type: 'TICK', time_remaining_seconds: 2700, status: 'ROUND_1' });
    res.json({ success: true, event });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Register or Login participant
app.post('/api/auth/register', (req, res) => {
  const { reg_id, team_name, member_names, college, email } = req.body || {};

  const cleanRegId = (reg_id && typeof reg_id === 'string' && reg_id.trim())
    ? reg_id.trim().toUpperCase()
    : ('DC-' + Math.floor(1000 + Math.random() * 9000));
  const finalTeamName = (team_name && typeof team_name === 'string' && team_name.trim())
    ? team_name.trim()
    : 'Task Force Investigator';

  let participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(cleanRegId);

  if (!participant) {
    const insertRes = db.prepare(`
      INSERT INTO participants (reg_id, team_name, member_names, college, email, lives, start_time)
      VALUES (?, ?, ?, ?, ?, 10, CURRENT_TIMESTAMP)
    `).run(cleanRegId, finalTeamName, member_names || '', college || '', email || '');

    participant = db.prepare('SELECT * FROM participants WHERE id = ?').get(insertRes.lastInsertRowid);
  } else {
    // Reset existing participant completely so they start fresh from the beginning
    db.prepare(`
      UPDATE participants SET
        team_name = ?,
        member_names = ?,
        college = ?,
        email = ?,
        current_round = 1,
        score = 0,
        round1_score = 0,
        round2_score = 0,
        round3_score = 0,
        round4_score = 0,
        lives = 10,
        is_eliminated = 0,
        ryuk_deal_active = 0,
        curse_active = 0,
        hint_used_r2 = 0,
        binary_bits = '______',
        identified_kira_code = NULL,
        is_completed = 0,
        tab_switches = 0,
        start_time = CURRENT_TIMESTAMP,
        finish_time = NULL
      WHERE id = ?
    `).run(finalTeamName, member_names || '', college || '', email || '', participant.id);

    // Delete old questions so a fresh set is generated
    db.prepare('DELETE FROM participant_questions WHERE participant_id = ?').run(participant.id);
  }

  // Pre-generate randomized questions and assign case
  generateParticipantQuestions(participant.id);

  // Reset clock to 45:00 at ROUND_1 and start fresh
  let event = updateEventState({
    status: 'ROUND_1',
    time_remaining_seconds: 2700,
    is_paused: 0
  });
  startTimer();
  broadcast({
    type: 'EVENT_UPDATE',
    event
  });
  broadcast({
    type: 'TICK',
    time_remaining_seconds: 2700,
    status: 'ROUND_1'
  });
  broadcast({
    type: 'ANNOUNCEMENT',
    title: 'INVESTIGATION COMMENCED',
    message: 'Investigation started. The Kira Protocol is now LIVE in Round 1: The Notebook!'
  });

  // Re-fetch updated participant
  participant = db.prepare('SELECT * FROM participants WHERE id = ?').get(participant.id);

  res.json({
    participant,
    event
  });
});

// Get current participant profile
app.get('/api/auth/me', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  if (!reg_id) {
    return res.status(401).json({ error: 'Registration ID required in header.' });
  }

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) {
    return res.status(404).json({ error: 'Participant not found.' });
  }

  const event = getEventState();
  res.json({ participant, event });
});

// -------------------------------------------------------------
// REST APIS: QUIZ & INVESTIGATION
// -------------------------------------------------------------

// Get questions for active round
app.get('/api/quiz/current', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  if (!reg_id) return res.status(401).json({ error: 'Unauthorized.' });

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  const event = getEventState();
  let activeRound = participant.current_round || 1;

  if (activeRound === 1 || activeRound === 2 || activeRound === 3) {
    // Fetch participant questions for active round without revealing correct options!
    const questions = db.prepare(`
      SELECT
        pq.id as participant_question_id,
        pq.order_num,
        pq.selected_option,
        pq.is_answered,
        pq.is_correct,
        pq.points_awarded,
        q.id as question_id,
        q.round_id,
        q.category,
        q.difficulty,
        q.concept,
        q.question_text,
        q.code_snippet,
        q.points,
        pq.shuffled_option_a as option_a,
        pq.shuffled_option_b as option_b,
        pq.shuffled_option_c as option_c,
        pq.shuffled_option_d as option_d
      FROM participant_questions pq
      JOIN questions q ON pq.question_id = q.id
      WHERE pq.participant_id = ? AND pq.round_id = ?
      ORDER BY pq.order_num ASC
    `).all(participant.id, activeRound);

    const mappedQuestions = questions.map((q) => {
      const isR3 = q.round_id === 3;
      const item = {
        participant_question_id: q.participant_question_id,
        order_num: q.order_num,
        selected_option: q.selected_option,
        is_answered: q.is_answered,
        is_correct: q.is_correct,
        points_awarded: q.points_awarded,
        question_id: q.question_id,
        round_id: q.round_id,
        category: q.category,
        difficulty: q.difficulty,
        concept: q.concept,
        question_text: q.question_text,
        code_snippet: q.code_snippet,
        points: q.points,
        option_a: q.option_a,
        option_b: q.option_b,
        option_c: q.option_c,
        option_d: q.option_d,
        question_type: isR3 ? 'ARRANGE_KEYWORDS' : 'MCQ',
        keywords: isR3 ? [q.option_a, q.option_b, q.option_c, q.option_d] : undefined
      };

      if (q.is_answered === 1) {
        const orig = db.prepare('SELECT option_a, option_b, option_c, option_d, explanation FROM questions WHERE id = ?').get(q.question_id);
        if (orig) {
          item.explanation = orig.explanation;
          if (isR3) {
            item.correct_sequence = [orig.option_a, orig.option_b, orig.option_c, orig.option_d];
          }
        }
      }

      return item;
    });

    return res.json({
      round: activeRound,
      questions: mappedQuestions,
      participant: {
        score: participant.score,
        lives: participant.lives,
        is_eliminated: participant.is_eliminated || (participant.lives <= 0 ? 1 : 0),
        ryuk_active: participant.ryuk_deal_active,
        curse_active: participant.curse_active,
        hint_used: participant.hint_used_r2
      }
    });
  } else if (activeRound === 4) {
    // Round 4 Investigation Package
    const caseId = participant.assigned_case_id || 1;
    const caseData = db.prepare('SELECT * FROM cases WHERE id = ?').get(caseId);

    // Clues
    const clues = db.prepare('SELECT * FROM investigation_clues WHERE case_id = ? ORDER BY clue_number ASC').all(caseId);
    // Sanitize clues: do not reveal correct_option or bit_value ahead of solving!
    const sanitizedClues = clues.map((c, idx) => {
      const bitChar = participant.binary_bits[idx] || '_';
      const isSolved = (bitChar === '0' || bitChar === '1');
      return {
        id: c.id,
        clue_number: c.clue_number,
        clue_text: c.clue_text,
        target_table: c.target_table,
        question_text: c.question_text,
        option_a: c.option_a,
        option_b: c.option_b,
        option_c: c.option_c,
        option_d: c.option_d,
        is_solved: isSolved,
        unlocked_bit: isSolved ? Number(bitChar) : null
      };
    });

    // Fetch the 4 Investigation Tables for this case
    const suspects = db.prepare('SELECT suspect_code, name, age, city, crimes, occupation, status FROM suspects WHERE case_id = ?').all(caseId);
    const loginLogs = db.prepare('SELECT suspect_code, login_time, device, ip_address, location FROM login_logs WHERE case_id = ?').all(caseId);
    const crimeRecords = db.prepare('SELECT suspect_code, crime_type, crime_count, target_type, weapon_or_method FROM crime_records WHERE case_id = ?').all(caseId);
    const transactions = db.prepare('SELECT suspect_code, amount, merchant, location, time_stamp FROM transactions WHERE case_id = ?').all(caseId);

    return res.json({
      round: 4,
      caseData: {
        case_code: caseData.case_code,
        story_title: caseData.story_title,
        description: caseData.description
      },
      clues: sanitizedClues,
      tables: {
        suspects,
        login_logs: loginLogs,
        crime_records: crimeRecords,
        transactions
      },
      participant: {
        score: participant.score,
        lives: participant.lives,
        is_eliminated: participant.is_eliminated || (participant.lives <= 0 ? 1 : 0),
        binary_bits: participant.binary_bits,
        identified_kira_code: participant.identified_kira_code,
        is_completed: participant.is_completed
      }
    });
  } else {
    return res.json({ round: 0, status: event.status, message: 'Event not currently in an active round.' });
  }
});

// Submit MCQ Answer (Round 1, 2 & 3)
app.post('/api/quiz/submit', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const { participant_question_id, selected_option, selected_keywords } = req.body;
  const userChoice = selected_keywords || selected_option;

  if (!reg_id || !participant_question_id || !userChoice) {
    return res.status(400).json({ error: 'Missing required submission payload.' });
  }

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({
      error: 'ELIMINATED',
      message: 'Investigator eliminated. Your name has been inscribed in the Death Note.',
      is_eliminated: 1
    });
  }

  const pq = db.prepare('SELECT * FROM participant_questions WHERE id = ? AND participant_id = ?').get(participant_question_id, participant.id);
  if (!pq) return res.status(404).json({ error: 'Question assignment not found.' });

  if (pq.is_answered === 1) {
    return res.status(400).json({ error: 'This question has already been answered.' });
  }

  const question = db.prepare('SELECT * FROM questions WHERE id = ?').get(pq.question_id);
  let isCorrect = false;
  let storedOption = selected_option ? String(selected_option).trim().toUpperCase() : '';

  if (pq.round_id === 3) {
    // Round 3: Arrange Missing Keywords in Order
    const rawSelected = req.body.selected_keywords || req.body.selected_option || selected_option;
    let userKeywords = [];
    if (Array.isArray(rawSelected)) {
      userKeywords = rawSelected.map((k) => String(k).trim());
    } else if (typeof rawSelected === 'string') {
      userKeywords = rawSelected.split(/\s*\|\|\|\s*|\s*,\s*/).map((k) => k.trim());
    }
    storedOption = userKeywords.join(' ||| ');

    const expectedKeywords = [question.option_a, question.option_b, question.option_c, question.option_d];
    isCorrect = (
      userKeywords.length === 4 &&
      userKeywords.every((kw, idx) => kw.toUpperCase() === expectedKeywords[idx].trim().toUpperCase())
    );
  } else {
    isCorrect = (selected_option && selected_option.trim().toUpperCase() === pq.mapped_correct_option.trim().toUpperCase());
  }

  let pointsEarned = 0;
  let newLives = participant.lives;

  if (pq.round_id === 1) {
    // Round 1: +1 if correct, -1 life if wrong
    if (isCorrect) {
      pointsEarned = 1;
    } else {
      pointsEarned = 0;
      newLives = Math.max(0, participant.lives - 1);
    }
  } else if (pq.round_id === 2) {
    // Round 2: +2 if correct, -1 life if wrong
    let multiplier = 1;
    if (participant.ryuk_deal_active === 1) multiplier *= 2;
    if (participant.curse_active === 1) multiplier *= 2;

    if (isCorrect) {
      pointsEarned = 2 * multiplier;
    } else {
      pointsEarned = 0;
      newLives = Math.max(0, participant.lives - 1);
    }
  } else if (pq.round_id === 3) {
    // Round 3: +4 if correct, -1 life if wrong
    if (isCorrect) {
      pointsEarned = 4;
    } else {
      pointsEarned = 0;
      newLives = Math.max(0, participant.lives - 1);
    }
  }

  const isEliminated = (newLives <= 0) ? 1 : 0;

  // Update participant_questions record
  db.prepare(`
    UPDATE participant_questions
    SET is_answered = 1,
        selected_option = ?,
        is_correct = ?,
        points_awarded = ?,
        answered_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(storedOption, isCorrect ? 1 : 0, pointsEarned, pq.id);

  // Update participant totals and reset one-time powerups
  const scoreField = pq.round_id === 1 ? 'round1_score' : pq.round_id === 2 ? 'round2_score' : 'round3_score';
  db.prepare(`
    UPDATE participants
    SET score = score + ?,
        ${scoreField} = ${scoreField} + ?,
        lives = ?,
        is_eliminated = ?,
        ryuk_deal_active = 0,
        curse_active = 0,
        last_active = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(pointsEarned, pointsEarned, newLives, isEliminated, participant.id);

  if (isEliminated) {
    broadcast({
      type: 'PARTICIPANT_ELIMINATED',
      team_name: participant.team_name,
      reg_id: participant.reg_id
    });
    broadcast({
      type: 'ANNOUNCEMENT',
      title: 'HEART ATTACK ELIMINATION',
      message: `Investigator ${participant.team_name} has lost all 10 lives. Name inscribed in the Death Note.`
    });
  }

  const updatedParticipant = db.prepare('SELECT * FROM participants WHERE id = ?').get(participant.id);

  res.json({
    success: true,
    is_correct: isCorrect,
    points_awarded: pointsEarned,
    new_score: updatedParticipant.score,
    new_lives: updatedParticipant.lives,
    is_eliminated: isEliminated,
    explanation: question.explanation,
    correct_sequence: [question.option_a, question.option_b, question.option_c, question.option_d]
  });
});

// Advance participant to next round
app.post('/api/quiz/advance-round', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  if (!reg_id) return res.status(401).json({ error: 'Unauthorized.' });

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  const nextRound = Math.min((participant.current_round || 1) + 1, 4);
  db.prepare('UPDATE participants SET current_round = ? WHERE id = ?').run(nextRound, participant.id);

  const updatedParticipant = db.prepare('SELECT * FROM participants WHERE id = ?').get(participant.id);
  res.json({ success: true, current_round: nextRound, participant: updatedParticipant });
});

// "ASK L" / "L's Eye" Hint (Round 2)
app.post('/api/quiz/hint', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const { question_id } = req.body;

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({ error: 'ELIMINATED', message: 'Investigator eliminated. Your name has been inscribed in the Death Note.', is_eliminated: 1 });
  }

  if (participant.hint_used_r2 >= 1) {
    return res.status(400).json({ error: "You have already used your single 'ASK L' hint for Round 2." });
  }

  const question = db.prepare('SELECT * FROM questions WHERE id = ?').get(question_id);
  if (!question) return res.status(404).json({ error: 'Question not found.' });

  // Deduct 1 point penalty and mark hint used
  const newScore = Math.max(0, participant.score - 1);
  db.prepare(`
    UPDATE participants
    SET score = ?,
        hint_used_r2 = 1,
        last_active = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(newScore, participant.id);

  // Audit log
  db.prepare(`
    INSERT INTO audit_logs (participant_id, event_type, details)
    VALUES (?, 'HINT_USED', ?)
  `).run(participant.id, `Used Ask L on Question #${question_id}`);

  res.json({
    hint: question.hint_text || "L says: 'Re-examine standard SQL syntax and logical clause ordering.'",
    new_score: newScore
  });
});

// Ryuk's Deal Powerup (Round 2: Gamble for 2x points)
app.post('/api/quiz/powerup/ryuk', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({ error: 'ELIMINATED', message: 'Investigator eliminated. Your name has been inscribed in the Death Note.', is_eliminated: 1 });
  }

  db.prepare('UPDATE participants SET ryuk_deal_active = 1 WHERE id = ?').run(participant.id);

  db.prepare(`
    INSERT INTO audit_logs (participant_id, event_type, details)
    VALUES (?, 'RYUK_USED', 'Activated Ryuk Deal: 2x points on next question')
  `).run(participant.id);

  res.json({
    success: true,
    message: "Ryuk laughs: 'Deal sealed! Answer correctly for double points, but fail and reap nothing!'"
  });
});

// Safe Read-Only SQL Query Sandbox for Round 4 Investigation
app.post('/api/quiz/investigate/query', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const { sql } = req.body;

  if (!sql) return res.status(400).json({ error: 'SQL query string required.' });

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({ error: 'ELIMINATED', message: 'Investigator eliminated. Your name has been inscribed in the Death Note.', is_eliminated: 1 });
  }

  const cleanedSQL = sql.trim();
  const forbidden = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|ATTACH|DETACH|PRAGMA)\b/i;
  if (forbidden.test(cleanedSQL)) {
    return res.status(403).json({ error: 'Forbidden query: Only read-only SELECT queries are authorized in the investigation terminal.' });
  }

  try {
    // Execute query scoped to the participant's assigned case tables
    const caseId = participant.assigned_case_id || 1;
    const { DatabaseSync } = require('node:sqlite');
    const sandboxDb = new DatabaseSync(':memory:');

    // Create case tables inside sandbox
    sandboxDb.exec(`
      CREATE TABLE suspects (id INT, suspect_code TEXT, name TEXT, age INT, city TEXT, crimes INT, occupation TEXT, status TEXT);
      CREATE TABLE login_logs (id INT, suspect_code TEXT, login_time TEXT, device TEXT, ip_address TEXT, location TEXT);
      CREATE TABLE crime_records (id INT, suspect_code TEXT, crime_type TEXT, crime_count INT, target_type TEXT, weapon_or_method TEXT);
      CREATE TABLE transactions (id INT, suspect_code TEXT, amount INT, merchant TEXT, location TEXT, time_stamp TEXT);
    `);

    // Populate sandbox with case data
    const sList = db.prepare('SELECT * FROM suspects WHERE case_id = ?').all(caseId);
    const insS = sandboxDb.prepare('INSERT INTO suspects VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    sList.forEach(s => insS.run(s.id, s.suspect_code, s.name, s.age, s.city, s.crimes, s.occupation, s.status));

    const lList = db.prepare('SELECT * FROM login_logs WHERE case_id = ?').all(caseId);
    const insL = sandboxDb.prepare('INSERT INTO login_logs VALUES (?, ?, ?, ?, ?, ?)');
    lList.forEach(l => insL.run(l.id, l.suspect_code, l.login_time, l.device, l.ip_address, l.location));

    const cList = db.prepare('SELECT * FROM crime_records WHERE case_id = ?').all(caseId);
    const insC = sandboxDb.prepare('INSERT INTO crime_records VALUES (?, ?, ?, ?, ?, ?)');
    cList.forEach(c => insC.run(c.id, c.suspect_code, c.crime_type, c.crime_count, c.target_type, c.weapon_or_method));

    const tList = db.prepare('SELECT * FROM transactions WHERE case_id = ?').all(caseId);
    const insT = sandboxDb.prepare('INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?)');
    tList.forEach(t => insT.run(t.id, t.suspect_code, t.amount, t.merchant, t.location, t.time_stamp));

    // Execute user query
    const rows = sandboxDb.prepare(cleanedSQL).all();
    sandboxDb.close();

    res.json({
      success: true,
      row_count: rows.length,
      rows: rows.slice(0, 50)
    });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// Submit Clue Answer (Round 4)
app.post('/api/quiz/investigate/submit-clue', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const { clue_id, selected_option } = req.body;

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({ error: 'ELIMINATED', message: 'Investigator eliminated. Your name has been inscribed in the Death Note.', is_eliminated: 1 });
  }

  const clue = db.prepare('SELECT * FROM investigation_clues WHERE id = ? AND case_id = ?').get(clue_id, participant.assigned_case_id);
  if (!clue) return res.status(404).json({ error: 'Investigation clue not found.' });

  const isCorrect = (selected_option.trim().toUpperCase() === clue.correct_option.trim().toUpperCase());
  let currentBits = (participant.binary_bits || '______').split('');

  let pointsEarned = 0;
  let newLives = participant.lives;

  if (isCorrect) {
    pointsEarned = 3;
    const bitIndex = clue.clue_number - 1;
    currentBits[bitIndex] = clue.bit_value.toString();
  } else {
    newLives = Math.max(0, participant.lives - 1);
  }

  const isEliminated = (newLives <= 0) ? 1 : 0;
  const updatedBits = currentBits.join('');

  db.prepare(`
    UPDATE participants
    SET score = score + ?,
        round4_score = round4_score + ?,
        lives = ?,
        is_eliminated = ?,
        binary_bits = ?,
        last_active = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(pointsEarned, pointsEarned, newLives, isEliminated, updatedBits, participant.id);

  if (isEliminated) {
    broadcast({
      type: 'PARTICIPANT_ELIMINATED',
      team_name: participant.team_name,
      reg_id: participant.reg_id
    });
    broadcast({
      type: 'ANNOUNCEMENT',
      title: 'HEART ATTACK ELIMINATION',
      message: `Investigator ${participant.team_name} has lost all 10 lives in Round 4. Name inscribed in the Death Note.`
    });
  }

  const updatedParticipant = db.prepare('SELECT * FROM participants WHERE id = ?').get(participant.id);

  res.json({
    is_correct: isCorrect,
    points_awarded: pointsEarned,
    unlocked_bit: isCorrect ? clue.bit_value : null,
    binary_bits: updatedBits,
    new_score: updatedParticipant.score,
    new_lives: updatedParticipant.lives,
    is_eliminated: isEliminated
  });
});

// Final Accusation Submission (Round 4: Identify Kira)
app.post('/api/quiz/investigate/submit-criminal', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  const { suspect_code, binary_code } = req.body;

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  if (participant.is_eliminated === 1 || participant.lives <= 0) {
    return res.status(403).json({ error: 'ELIMINATED', message: 'Investigator eliminated. Your name has been inscribed in the Death Note.', is_eliminated: 1 });
  }

  // REQUIREMENT: Only after clearing at least 3 out of 6 secret codes can he win!
  const participantBits = participant.binary_bits || '______';
  const clearedBitsCount = (participantBits.match(/[01]/g) || []).length;
  if (clearedBitsCount < 3) {
    return res.status(400).json({
      error: 'INCOMPLETE_SECRET_CODES',
      message: `You must clear at least 3 out of 6 secret codes before you can execute Kira identification and win! (Currently cleared: ${clearedBitsCount}/6)`
    });
  }

  const caseData = db.prepare('SELECT * FROM cases WHERE id = ?').get(participant.assigned_case_id);
  const isKiraIdentified = (
    suspect_code.trim().toUpperCase() === caseData.kira_suspect_code.trim().toUpperCase()
  );

  let bonusPoints = 0;
  let newLives = participant.lives;

  if (isKiraIdentified) {
    bonusPoints = 10;
  } else {
    newLives = Math.max(0, participant.lives - 1);
  }

  const isEliminated = (newLives <= 0) ? 1 : 0;

  db.prepare(`
    UPDATE participants
    SET score = score + ?,
        round4_score = round4_score + ?,
        lives = ?,
        is_eliminated = ?,
        identified_kira_code = ?,
        is_completed = 1,
        finish_time = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(bonusPoints, bonusPoints, newLives, isEliminated, suspect_code.trim().toUpperCase(), participant.id);

  const finalParticipant = db.prepare('SELECT * FROM participants WHERE id = ?').get(participant.id);

  if (isEliminated) {
    broadcast({
      type: 'PARTICIPANT_ELIMINATED',
      team_name: participant.team_name,
      reg_id: participant.reg_id
    });
  }

  // Broadcast leaderboard update
  broadcast({
    type: 'CRIMINAL_IDENTIFIED',
    team_name: finalParticipant.team_name,
    is_correct: isKiraIdentified
  });

  if (isKiraIdentified) {
    broadcast({
      type: 'ANNOUNCEMENT',
      title: 'VICTORY — KIRA APPREHENDED',
      message: `Investigator ${finalParticipant.team_name} has decrypted ${clearedBitsCount}/6 secret codes and captured Kira! YOU WIN!`
    });
  }

  res.json({
    is_correct: isKiraIdentified,
    is_winner: isKiraIdentified,
    cleared_bits_count: clearedBitsCount,
    all_codes_cleared: clearedBitsCount >= 3,
    bonus_points: bonusPoints,
    kira_name: caseData.kira_name,
    kira_code: caseData.kira_suspect_code,
    binary_code: caseData.binary_code,
    final_score: finalParticipant.score,
    new_lives: finalParticipant.lives,
    is_eliminated: isEliminated
  });
});

// -------------------------------------------------------------
// REST APIS: LEADERBOARD & STATS
// -------------------------------------------------------------
app.get('/api/leaderboard', (req, res) => {
  const event = getEventState();
  const participants = db.prepare(`
    SELECT
      id, reg_id, team_name, college, score,
      round1_score, round2_score, round3_score, round4_score,
      lives, is_eliminated, binary_bits, is_completed, finish_time, start_time,
      (strftime('%s', COALESCE(finish_time, CURRENT_TIMESTAMP)) - strftime('%s', start_time)) as time_taken_seconds
    FROM participants
    ORDER BY
      is_eliminated ASC,
      score DESC,
      time_taken_seconds ASC,
      round4_score DESC,
      round3_score DESC,
      finish_time ASC
  `).all();

  // Suspense Mode: Hide exact scores in Round 4 for non-admins
  const isAdmin = req.query.admin === 'true';
  const shouldMask = (event.status === 'ROUND_4' && event.suspense_mode === 1 && !isAdmin);

  const leaderboard = participants.map((p, index) => {
    let statusText = '🟢 INVESTIGATING';
    if (p.is_eliminated === 1 || p.lives <= 0) {
      statusText = '💀 DECEASED (HEART ATTACK)';
    } else if (p.is_completed === 1) {
      statusText = '☠️ KIRA ARRESTED';
    }

    return {
      rank: index + 1,
      reg_id: p.reg_id,
      team_name: p.team_name,
      college: p.college,
      lives: p.lives,
      is_eliminated: p.is_eliminated === 1 || p.lives <= 0 ? 1 : 0,
      score: shouldMask ? '???' : p.score,
      round1_score: shouldMask ? '???' : p.round1_score,
      round2_score: shouldMask ? '???' : p.round2_score,
      round3_score: shouldMask ? '???' : p.round3_score,
      round4_score: shouldMask ? '???' : p.round4_score,
      status: statusText,
      binary_bits: p.binary_bits,
      time_taken_seconds: p.time_taken_seconds
    };
  });

  res.json({
    suspense_mode: shouldMask,
    leaderboard
  });
});

// -------------------------------------------------------------
// REST APIS: ANTI-CHEAT TELEMETRY
// -------------------------------------------------------------
app.post('/api/telemetry/tab-switch', (req, res) => {
  const reg_id = req.headers['x-registration-id'];
  if (!reg_id) return res.status(400).json({ error: 'Missing reg_id.' });

  const participant = db.prepare('SELECT * FROM participants WHERE reg_id = ?').get(reg_id.trim().toUpperCase());
  if (!participant) return res.status(404).json({ error: 'Participant not found.' });

  db.prepare(`
    UPDATE participants
    SET tab_switches = tab_switches + 1
    WHERE id = ?
  `).run(participant.id);

  db.prepare(`
    INSERT INTO audit_logs (participant_id, event_type, details)
    VALUES (?, 'TAB_SWITCH', 'Participant switched tabs or lost focus')
  `).run(participant.id);

  res.json({ success: true });
});

// -------------------------------------------------------------
// REST APIS: ORGANIZER ADMIN PANEL
// -------------------------------------------------------------
app.get('/api/admin/overview', (req, res) => {
  const event = getEventState();
  const totalParticipants = db.prepare('SELECT COUNT(*) as count FROM participants').get().count;
  const completedCount = db.prepare('SELECT COUNT(*) as count FROM participants WHERE is_completed = 1').get().count;
  const avgScore = db.prepare('SELECT AVG(score) as avg FROM participants').get().avg || 0;
  const auditLogs = db.prepare(`
    SELECT a.*, p.team_name, p.reg_id
    FROM audit_logs a
    JOIN participants p ON a.participant_id = p.id
    ORDER BY a.id DESC LIMIT 30
  `).all();

  res.json({
    event,
    total_participants: totalParticipants,
    completed_participants: completedCount,
    average_score: Math.round(avgScore * 10) / 10,
    recent_logs: auditLogs
  });
});

app.post('/api/admin/event/status', (req, res) => {
  const { status, is_paused } = req.body;
  const updates = {};
  if (status) updates.status = status;
  if (is_paused !== undefined) updates.is_paused = is_paused ? 1 : 0;

  const event = updateEventState(updates);
  broadcast({
    type: 'EVENT_UPDATE',
    event
  });
  res.json({ event });
});

app.post('/api/admin/event/timer', (req, res) => {
  const { minutes } = req.body;
  const seconds = Math.max(0, parseInt(minutes, 10) * 60);
  const event = updateEventState({ time_remaining_seconds: seconds });
  broadcast({
    type: 'TICK',
    time_remaining_seconds: seconds,
    status: event.status
  });
  res.json({ event });
});

app.post('/api/admin/event/toggles', (req, res) => {
  const { suspense_mode, anti_cheat_enabled } = req.body;
  const updates = {};
  if (suspense_mode !== undefined) updates.suspense_mode = suspense_mode ? 1 : 0;
  if (anti_cheat_enabled !== undefined) updates.anti_cheat_enabled = anti_cheat_enabled ? 1 : 0;

  const event = updateEventState(updates);
  res.json({ event });
});

// Full Question Bank Management
app.get('/api/admin/questions', (req, res) => {
  const round_id = req.query.round_id;
  const category = req.query.category;

  let query = 'SELECT * FROM questions WHERE 1=1';
  const params = [];

  if (round_id) {
    query += ' AND round_id = ?';
    params.push(round_id);
  }
  if (category) {
    query += ' AND category = ?';
    params.push(category);
  }

  query += ' ORDER BY id ASC';
  const questions = db.prepare(query).all(...params);
  res.json({ questions });
});

app.post('/api/admin/questions', (req, res) => {
  const {
    round_id, category, difficulty, concept, question_text,
    code_snippet, option_a, option_b, option_c, option_d,
    correct_option, points, hint_text, explanation
  } = req.body;

  const insertRes = db.prepare(`
    INSERT INTO questions (
      round_id, category, difficulty, concept, question_text,
      code_snippet, option_a, option_b, option_c, option_d,
      correct_option, points, hint_text, explanation
    ) VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?,
      ?, ?, ?, ?
    )
  `).run(
    round_id || 1, category || 'GENERAL', difficulty || 'medium', concept || '', question_text,
    code_snippet || '', option_a, option_b, option_c, option_d,
    correct_option || 'A', points || 1, hint_text || '', explanation || ''
  );

  res.json({ success: true, id: insertRes.lastInsertRowid });
});

app.delete('/api/admin/questions/:id', (req, res) => {
  db.prepare('DELETE FROM questions WHERE id = ?').run(req.params.id);
  res.json({ success: true });
});

// CSV Export Endpoint
app.get('/api/admin/export/csv', (req, res) => {
  const participants = db.prepare(`
    SELECT
      id, reg_id, team_name, member_names, college, email,
      score, round1_score, round2_score, round3_score, lives,
      binary_bits, identified_kira_code, is_completed, tab_switches,
      start_time, finish_time
    FROM participants
    ORDER BY score DESC, finish_time ASC
  `).all();

  let csv = 'Rank,Registration ID,Team Name,Members,College,Email,Total Score,R1 Score,R2 Score,R3 Score,Lives Remaining,Binary Bits,Kira Code,Completed,Tab Switches,Start Time,Finish Time\n';

  participants.forEach((p, index) => {
    csv += `${index + 1},"${p.reg_id}","${p.team_name}","${p.member_names || ''}","${p.college || ''}","${p.email || ''}",${p.score},${p.round1_score},${p.round2_score},${p.round3_score},${p.lives},"${p.binary_bits}","${p.identified_kira_code || ''}",${p.is_completed},${p.tab_switches},"${p.start_time || ''}","${p.finish_time || ''}"\n`;
  });

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="death_code_results.csv"');
  res.send(csv);
});

// SPA Fallback routing for client app
if (fs.existsSync(clientDistPath)) {
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/ws')) {
      return next();
    }
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(clientDistPath, 'index.html'));
  });
}

// -------------------------------------------------------------
// START SERVER
// -------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`====================================================`);
  console.log(`☠️ DEATH CODE: THE KIRA PROTOCOL SERVER ONLINE ☠️`);
  console.log(`REST API & WebSocket listening on http://localhost:${PORT}`);
  console.log(`Database engine: native node:sqlite`);
  console.log(`====================================================`);
});
