const fs = require('fs');
const path = require('path');
require('dotenv').config();
const csvFilePath = process.env.CSV_FILE_PATH;
const jsonFilePath = path.join(process.cwd(), 'output.json');
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

fs.readFile(csvFilePath, 'utf-8', (err, data) => {
  if (err) {
    console.error('Error reading CSV file:', err);
    return;
  }
  const rows = data.trim().split('\n');
  const headers = rows[0].split(',');
  const users = [];

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i].split(',');
    const user = {};

    for (let j = 0; j < headers.length; j++) {
      const header = headers[j];
      const value = row[j];

      if (header === 'name.firstName' || header === 'name.lastName') {
        if (!user.name) {
          user.name = {};
        }
        user.name[header.split('.')[1]] = value;
      } else if (header === 'age') {
        user.age = parseInt(value, 10);
      } else if (header.startsWith('address.')) {
        if (!user.address) {
          user.address = {};
        }
        user.address[header.split('.')[1]] = value;
      } else {
        user[header] = value;
      }
    }

    users.push(user);
  }

  // Convert users data to JSON string
  const jsonData = JSON.stringify(users);

  // Upload JSON data to PostgreSQL
  pool.query('CREATE TABLE IF NOT EXISTS users_data (data jsonb)', (err, result) => {
    if (err) {
      console.error('Error creating table:', err);
      return;
    }

    pool.query('INSERT INTO users_data (data) VALUES ($1)', [jsonData], (err, result) => {
      if (err) {
        console.error('Error inserting JSON data into table:', err);
        return;
      }

      console.log('JSON data successfully uploaded to PostgreSQL table.');
    });
  });
});
