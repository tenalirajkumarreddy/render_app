'use strict';
const PG = require('pg');
const { Pool } = PG;
const DSN = process.env.ECI_PG_DSN || 'postgresql://eci_app:Raj%40A2Nkufyg@129.225.75.85:5432/old_eci';
const POOL = new Pool({ connectionString: DSN, application_name: 'test', max: 2 });

async function run() {
  const client = await POOL.connect();
  try {
    // the exact query from server.js electors handler (no filters)
    const limit = 3, offset = 0;
    const idx = 1;
    const cond = 'where true';
    const sql = `select source_id, state_cd, ac_no, part_no, serial_no, epic_2003,\n         marked_by_blo, cur_state_cd, cur_ac_no, cur_part_no, cur_epic,\n         first_seen, last_seen, full_name, full_name_l1, relative_name,\n         relation_type, gender, age_snapshot\n         from electors ${cond} order by first_seen desc limit $${idx} offset $${idx+1}`;
    console.log('SQL (first 120 chars):', sql.replace(/\\n/g,' ').slice(0,120));
    console.log('params:', [limit, offset]);
    const r = await client.query({ text: sql, values: [limit, offset] });
    console.log('rows:', r.rows.length);
    if (r.rows.length) {
      console.log('first row keys:', Object.keys(r.rows[0]));
      console.log('first row cur_epic:', r.rows[0].cur_epic);
      console.log('first row part_no:', r.rows[0].part_no);
    }
  } finally {
    client.release();
  }
  await POOL.end();
}
run().catch(e => { console.error(e && e.stack ? e.stack : e); process.exit(1); });
