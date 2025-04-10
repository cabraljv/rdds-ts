import pg from 'pg';

class DatabaseService {
  private database: pg.Pool;

  constructor(database: pg.Pool) {
    this.database = database;
  }

  async executeQuery(query: string) {
    const result = await this.database.query(query);
    return result.rows;
  }

  async healthCheck() {
    try {
      await this.database.query('SELECT 1');
      return true;
    } catch (error) {
      return false;
    }
  }
}

export default DatabaseService; 