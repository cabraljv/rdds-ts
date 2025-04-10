import { Pool } from 'pg';
import { v4 as uuidv4 } from 'uuid';

// Database configuration using environment variables
const pool = new Pool({
  user: process.env.DB_USER || 'your_username',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'your_database',
  password: process.env.DB_PASSWORD || 'your_password',
  port: parseInt(process.env.DB_PORT || '5432'),
});

async function insertUsers(users: {userId: string, name: string, email: string}[]) {
  await new Promise(resolve => setTimeout(resolve, 5 * 1000));
  try {
    const query = `
      INSERT INTO users (id, name, email)
      VALUES ${users.map(user => `('${user.userId}', '${user.name}', '${user.email}')`).join(',')}
    `;
    
    const timeoutTime = 10000;
    const timeout = setTimeout(() => {
      console.log('Query timed out after 10 seconds');
      throw new Error('Query timed out');
    }, timeoutTime);
    const result = await pool.query(query);
    clearTimeout(timeout);
    
    console.log('User inserted successfully:', result.rows);
    return result.rows[0];
  } catch (error) {
    console.error('Error inserting user:', error);
    throw error;
  }
}

async function getUsers() {
  await new Promise(resolve => setTimeout(resolve, 5 * 1000));
  const result = await pool.query('SELECT * FROM users;');
  return result.rows;
}

// Example usage
// console.log('Inserting user...');
// insertUsers([
//   {userId: uuidv4(), name: 'John Doe', email: 'john@example.com'},
//   {userId: uuidv4(), name: 'Jane ', email: 'jane@example.com'},
//   {userId: uuidv4(), name: 'Jim Beam', email: 'jim@example.com'},
// ]).then(() => {
//   console.log('User inserted successfully');
// }).catch(error => console.error('Failed to insert user:', error))
//   .finally(() => pool.end());

getUsers().then(users => console.log(users));
