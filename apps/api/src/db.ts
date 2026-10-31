import pg from 'pg';
export const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 12,
  connectionTimeoutMillis: 5000, idleTimeoutMillis: 30000 });
