const { db } = require('./db');

// Fisher-Yates shuffle
function shuffleArray(array) {
  const arr = [...array];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

// Shuffle 4 options and return shuffled object + new mapped correct option ('A'|'B'|'C'|'D')
function shuffleOptions(optA, optB, optC, optD, originalCorrect) {
  const originalMap = {
    'A': optA,
    'B': optB,
    'C': optC,
    'D': optD
  };
  const correctText = originalMap[originalCorrect.trim().toUpperCase()];

  const entries = [
    { key: 'A', text: optA },
    { key: 'B', text: optB },
    { key: 'C', text: optC },
    { key: 'D', text: optD }
  ];

  const shuffled = shuffleArray(entries);
  const letters = ['A', 'B', 'C', 'D'];
  const result = {};
  let mappedCorrect = 'A';

  shuffled.forEach((item, index) => {
    const letter = letters[index];
    result[`shuffled_option_${letter.toLowerCase()}`] = item.text;
    if (item.text === correctText) {
      mappedCorrect = letter;
    }
  });

  result.mapped_correct_option = mappedCorrect;
  return result;
}

// Generate personalized question set for a participant
function generateParticipantQuestions(participantId) {
  // Check if questions already generated
  const existing = db.prepare('SELECT COUNT(*) as count FROM participant_questions WHERE participant_id = ?').get(participantId);
  if (existing && existing.count > 0) {
    return;
  }

  // --- ROUND 1 BALANCED SELECTION (10 Questions) ---
  const r1Distribution = [
    { cat: 'SELECT', count: 2 },
    { cat: 'WHERE', count: 2 },
    { cat: 'JOIN', count: 1 },
    { cat: 'GROUP_BY', count: 1 },
    { cat: 'HAVING', count: 1 },
    { cat: 'AGGREGATE', count: 1 },
    { cat: 'LIKE', count: 1 },
    { cat: 'ORDER_BY', count: 1 }
  ];

  let r1SelectedQuestions = [];
  for (const rule of r1Distribution) {
    const pool = db.prepare('SELECT * FROM questions WHERE round_id = 1 AND category = ?').all(rule.cat);
    const shuffledPool = shuffleArray(pool);
    r1SelectedQuestions.push(...shuffledPool.slice(0, rule.count));
  }

  // If we need extra to ensure exactly 10
  if (r1SelectedQuestions.length < 10) {
    const remainingCount = 10 - r1SelectedQuestions.length;
    const existingIds = r1SelectedQuestions.map(q => q.id);
    let extra = [];
    if (existingIds.length > 0) {
      const placeholders = existingIds.map(() => '?').join(',');
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 1 AND id NOT IN (${placeholders}) LIMIT ?`).all(...existingIds, remainingCount);
    } else {
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 1 LIMIT ?`).all(remainingCount);
    }
    r1SelectedQuestions.push(...extra);
  }

  // Level 2: Shuffle Question Order
  r1SelectedQuestions = shuffleArray(r1SelectedQuestions);

  // --- ROUND 2 BALANCED SELECTION (8 Questions) ---
  const r2Distribution = [
    { cat: 'MISSING_KEYWORD', count: 2 },
    { cat: 'MISSING_CONDITION', count: 1 },
    { cat: 'MISSING_FUNCTION', count: 1 },
    { cat: 'MISSING_JOIN', count: 1 },
    { cat: 'MISSING_GROUP_BY', count: 1 },
    { cat: 'DEBUGGING', count: 2 }
  ];

  let r2SelectedQuestions = [];
  for (const rule of r2Distribution) {
    const pool = db.prepare('SELECT * FROM questions WHERE round_id = 2 AND category = ?').all(rule.cat);
    const shuffledPool = shuffleArray(pool);
    r2SelectedQuestions.push(...shuffledPool.slice(0, rule.count));
  }

  if (r2SelectedQuestions.length < 8) {
    const remainingCount = 8 - r2SelectedQuestions.length;
    const existingIds = r2SelectedQuestions.map(q => q.id);
    let extra = [];
    if (existingIds.length > 0) {
      const placeholders = existingIds.map(() => '?').join(',');
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 2 AND id NOT IN (${placeholders}) LIMIT ?`).all(...existingIds, remainingCount);
    } else {
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 2 LIMIT ?`).all(remainingCount);
    }
    r2SelectedQuestions.push(...extra);
  }

  // --- ROUND 3 BALANCED SELECTION (8 Questions: SPK Surveillance) ---
  const r3Distribution = [
    { cat: 'WINDOW_FUNCTION', count: 2 },
    { cat: 'SUBQUERY_CTE', count: 2 },
    { cat: 'COMPLEX_JOIN', count: 1 },
    { cat: 'CONDITIONAL_LOGIC', count: 1 },
    { cat: 'SET_OPERATIONS', count: 1 },
    { cat: 'GROUP_HAVING', count: 1 }
  ];

  let r3SelectedQuestions = [];
  for (const rule of r3Distribution) {
    const pool = db.prepare('SELECT * FROM questions WHERE round_id = 3 AND category = ?').all(rule.cat);
    const shuffledPool = shuffleArray(pool);
    r3SelectedQuestions.push(...shuffledPool.slice(0, rule.count));
  }

  if (r3SelectedQuestions.length < 8) {
    const remainingCount = 8 - r3SelectedQuestions.length;
    const existingIds = r3SelectedQuestions.map(q => q.id);
    let extra = [];
    if (existingIds.length > 0) {
      const placeholders = existingIds.map(() => '?').join(',');
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 3 AND id NOT IN (${placeholders}) LIMIT ?`).all(...existingIds, remainingCount);
    } else {
      extra = db.prepare(`SELECT * FROM questions WHERE round_id = 3 LIMIT ?`).all(remainingCount);
    }
    r3SelectedQuestions.push(...extra);
  }

  r3SelectedQuestions = shuffleArray(r3SelectedQuestions);

  // Insert into participant_questions with Level 3 Option Shuffle
  const insertPQ = db.prepare(`
    INSERT INTO participant_questions (
      participant_id, round_id, question_id, order_num,
      shuffled_option_a, shuffled_option_b, shuffled_option_c, shuffled_option_d,
      mapped_correct_option
    ) VALUES (
      ?, ?, ?, ?,
      ?, ?, ?, ?,
      ?
    )
  `);

  // Insert Round 1
  r1SelectedQuestions.forEach((q, idx) => {
    const sh = shuffleOptions(q.option_a, q.option_b, q.option_c, q.option_d, q.correct_option);
    insertPQ.run(
      participantId, 1, q.id, idx + 1,
      sh.shuffled_option_a, sh.shuffled_option_b, sh.shuffled_option_c, sh.shuffled_option_d,
      sh.mapped_correct_option
    );
  });

  // Insert Round 2
  r2SelectedQuestions.forEach((q, idx) => {
    const sh = shuffleOptions(q.option_a, q.option_b, q.option_c, q.option_d, q.correct_option);
    insertPQ.run(
      participantId, 2, q.id, idx + 1,
      sh.shuffled_option_a, sh.shuffled_option_b, sh.shuffled_option_c, sh.shuffled_option_d,
      sh.mapped_correct_option
    );
  });

  // Insert Round 3
  r3SelectedQuestions.forEach((q, idx) => {
    const sh = shuffleOptions(q.option_a, q.option_b, q.option_c, q.option_d, q.correct_option);
    insertPQ.run(
      participantId, 3, q.id, idx + 1,
      sh.shuffled_option_a, sh.shuffled_option_b, sh.shuffled_option_c, sh.shuffled_option_d,
      sh.mapped_correct_option
    );
  });

  // Level 4: Assign Random Case for Round 4 Investigation
  const totalCases = db.prepare('SELECT COUNT(*) as count FROM cases').get().count;
  const randomCaseOffset = Math.floor(Math.random() * totalCases);
  const assignedCase = db.prepare('SELECT id FROM cases LIMIT 1 OFFSET ?').get(randomCaseOffset);

  db.prepare('UPDATE participants SET assigned_case_id = ? WHERE id = ?').run(
    assignedCase ? assignedCase.id : 1,
    participantId
  );
}

module.exports = {
  shuffleArray,
  shuffleOptions,
  generateParticipantQuestions
};
