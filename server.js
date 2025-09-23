const express = require('express');
const { Pool } = require('pg');
const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Test database connection
app.get('/test-db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json({ 
      status: 'Database connected!',
      time: result.rows[0].now 
    });
  } catch (error) {
    res.status(500).json({ 
      status: 'Database connection failed',
      error: error.message 
    });
  }
});

// Create a sample table
app.get('/setup-db', async (req, res) => {
  try {
    // Create stockists table if it doesn't exist
    await pool.query(`
      CREATE TABLE IF NOT EXISTS stockists (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        address TEXT,
        city VARCHAR(100),
        country VARCHAR(100),
        phone VARCHAR(50),
        email VARCHAR(255),
        website VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    res.json({ status: 'Database table created successfully!' });
  } catch (error) {
    res.status(500).json({ 
      status: 'Failed to create table',
      error: error.message 
    });
  }
});

// Get all stockists
app.get('/api/stockists', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM stockists ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Add a new stockist
app.post('/api/stockists', async (req, res) => {
  const { name, address, city, country, phone, email, website } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO stockists (name, address, city, country, phone, email, website) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *',
      [name, address, city, country, phone, email, website]
    );
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Home route
app.get('/', (req, res) => {
  res.send(`
    <html>
      <head>
        <title>Antenne Database</title>
        <style>
          body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
          h1 { color: #333; }
          .endpoint { background: #f0f0f0; padding: 10px; margin: 10px 0; border-radius: 5px; }
          code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
        </style>
      </head>
      <body>
        <h1>🎉 Antenne Database is Live!</h1>
        <p>Your database-connected application is running successfully.</p>
        
        <h2>Available Endpoints:</h2>
        <div class="endpoint">
          <strong>GET</strong> <code>/test-db</code> - Test database connection
        </div>
        <div class="endpoint">
          <strong>GET</strong> <code>/setup-db</code> - Create the stockists table
        </div>
        <div class="endpoint">
          <strong>GET</strong> <code>/api/stockists</code> - Get all stockists
        </div>
        <div class="endpoint">
          <strong>POST</strong> <code>/api/stockists</code> - Add a new stockist
        </div>
        
        <h2>Quick Test:</h2>
        <p>
          <a href="/test-db" target="_blank">Click here to test database connection</a><br>
          <a href="/setup-db" target="_blank">Click here to setup database table</a>
        </p>
      </body>
    </html>
  `);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
