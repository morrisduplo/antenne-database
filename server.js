const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Test database connection
pool.connect((err, client, release) => {
    if (err) {
        console.error('Error connecting to database:', err.stack);
    } else {
        console.log('Connected to PostgreSQL database');
        release();
    }
});

// Middleware
app.use(express.static('public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Configure multer for file uploads (keeping for Booksonix and potential future use)
const upload = multer({ dest: 'uploads/' });

// Initialize Booksonix table
async function initBooksonixTable() {
    try {
        // Check if the table exists
        const tableCheck = await pool.query(`
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'booksonix_records'
            ORDER BY ordinal_position;
        `);
        
        if (tableCheck.rows.length === 0) {
            // Create table with SKU-based structure
            console.log('Creating new Booksonix table with SKU-based structure...');
            await pool.query(`
                CREATE TABLE booksonix_records (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(100) UNIQUE NOT NULL,
                    isbn VARCHAR(50),
                    title VARCHAR(500),
                    author VARCHAR(500),
                    publisher VARCHAR(500),
                    price DECIMAL(10,2),
                    quantity INTEGER DEFAULT 0,
                    format VARCHAR(100),
                    publication_date DATE,
                    description TEXT,
                    category VARCHAR(200),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `);
            
            // Create indexes
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_sku ON booksonix_records(sku)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_isbn ON booksonix_records(isbn)`);
            
            console.log('Booksonix table created successfully');
        } else {
            // Table exists, ensure it has the correct structure
            const hasSkuColumn = tableCheck.rows.some(row => row.column_name === 'sku');
            
            if (!hasSkuColumn) {
                console.log('Migrating Booksonix table to SKU-based structure...');
                
                // Add SKU column
                await pool.query(`ALTER TABLE booksonix_records ADD COLUMN IF NOT EXISTS sku VARCHAR(100)`);
                
                // Copy ISBN to SKU for existing records
                await pool.query(`UPDATE booksonix_records SET sku = isbn WHERE sku IS NULL AND isbn IS NOT NULL`);
                
                // For any remaining null SKUs, generate a temporary SKU
                await pool.query(`UPDATE booksonix_records SET sku = 'TEMP_' || id::text WHERE sku IS NULL`);
                
                // Make SKU NOT NULL and add unique constraint
                await pool.query(`ALTER TABLE booksonix_records ALTER COLUMN sku SET NOT NULL`);
                
                try {
                    await pool.query(`ALTER TABLE booksonix_records ADD CONSTRAINT booksonix_records_sku_key UNIQUE (sku)`);
                } catch (e) {
                    // Constraint might already exist
                }
                
                console.log('Migration complete: Booksonix table now uses SKU as primary identifier');
            }
            
            // Create indexes if they don't exist
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_sku ON booksonix_records(sku)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_isbn ON booksonix_records(isbn)`);
        }
        
        // Verify final structure
        const finalCheck = await pool.query(`SELECT COUNT(*) as total FROM booksonix_records`);
        console.log(`Booksonix table ready with ${finalCheck.rows[0].total} existing records`);
        
    } catch (err) {
        console.error('Error initializing Booksonix table:', err);
    }
}

// Initialize database tables
async function initDatabase() {
    try {
        console.log('Starting database initialization...');

        // Create users table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'editor',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        `);

        // Create customer name mappings table (for settings)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_mappings (
                id SERIAL PRIMARY KEY,
                original_name VARCHAR(500) NOT NULL,
                display_name VARCHAR(500) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Initialize Booksonix table
        await initBooksonixTable();

        console.log('Database tables created successfully');
        
        // Check if admin user exists
        const adminCheck = await pool.query("SELECT * FROM users WHERE username = 'admin'");
        
        if (adminCheck.rows.length === 0) {
            console.log('Creating admin user...');
            
            // Create admin user with password 'admin123'
            const hashedPassword = await bcrypt.hash('admin123', 10);
            
            await pool.query(
                'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4)',
                ['admin', 'admin@antennebooks.com', hashedPassword, 'admin']
            );
            
            console.log('=================================');
            console.log('Admin user created successfully!');
            console.log('Username: admin');
            console.log('Password: admin123');
            console.log('IMPORTANT: Please change this password immediately after first login!');
            console.log('=================================');
        } else {
            console.log('Admin user already exists');
        }
        
    } catch (err) {
        console.error('Error initializing database:', err);
        throw err;
    }
}

// Initialize database on startup
initDatabase().catch(err => {
    console.error('Failed to initialize database:', err);
});

// =============================================
// PAGE ROUTES
// =============================================

// Home page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Customers page
app.get('/customers', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'customers.html'));
});

// Reports page
app.get('/reports', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'reports.html'));
});

// Settings page
app.get('/settings', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'settings.html'));
});

// Login page
app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.get('/login.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Data Upload main page
app.get('/data-upload', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload.html'));
});

// Data Upload sub-pages
app.get('/data-upload/page2', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page2.html'));
});

app.get('/data-upload/page3', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page3.html'));
});

app.get('/data-upload/page4', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page4.html'));
});

app.get('/data-upload/page5', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page5.html'));
});

app.get('/data-upload/page6', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page6.html'));
});

// Booksonix page
app.get('/booksonix', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'booksonix.html'));
});

// =============================================
// AUTHENTICATION ROUTES
// =============================================

app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;

    console.log('Login attempt for username:', username);

    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password required' });
    }

    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE username = $1 OR email = $1',
            [username]
        );

        if (result.rows.length === 0) {
            console.log('No user found with username:', username);
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        const user = result.rows[0];
        const validPassword = await bcrypt.compare(password, user.password);
        
        if (!validPassword) {
            console.log('Invalid password for user:', username);
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        // Update last login
        await pool.query(
            'UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = $1',
            [user.id]
        );

        console.log('Login successful for:', username);

        res.json({
            success: true,
            user: {
                id: user.id,
                username: user.username,
                email: user.email,
                role: user.role
            }
        });
    } catch (err) {
        console.error('Login database error:', err);
        res.status(500).json({ error: 'Database error during login' });
    }
});

// =============================================
// BOOKSONIX ROUTES
// =============================================

app.post('/api/booksonix/upload', upload.single('booksonixFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('Processing Booksonix file:', req.file.originalname);

    try {
        const workbook = xlsx.readFile(req.file.path);
        const sheetName = workbook.SheetNames[0];
        const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

        console.log('Total rows in Excel file:', data.length);

        let newRecords = 0;
        let duplicates = 0;
        let errors = 0;
        let skippedNoSku = 0;

        for (const row of data) {
            // Look for SKU in multiple possible column names
            const sku = row['SKU'] || row['sku'] || row['Sku'] || 
                       row['Product SKU'] || row['Product Code'] || 
                       row['Item Code'] || row['Code'] || '';
            
            if (!sku) {
                skippedNoSku++;
                errors++;
                continue;
            }

            const isbn = row['ISBN-13'] || row['ISBN13'] || row['isbn-13'] || row['ISBN'] || row['EAN'] || '';
            const title = row['Title'] || row['TITLE'] || row['Product Title'] || '';
            const publisher = row['Publishers'] || row['Publisher'] || row['PUBLISHER'] || '';
            
            let price = 0;
            const priceValue = row['Prices'] || row['Price'] || row['PRICE'] || row['RRP'] || '';
            if (priceValue) {
                const cleanPrice = String(priceValue)
                    .replace(/GBP/gi, '')
                    .replace(/£/g, '')
                    .replace(/,/g, '')
                    .replace(/[^\d.]/g, '')
                    .trim();
                price = parseFloat(cleanPrice) || 0;
            }

            try {
                const result = await pool.query(
                    `INSERT INTO booksonix_records 
                    (sku, isbn, title, author, publisher, price, quantity) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (sku) 
                    DO UPDATE SET 
                        isbn = COALESCE(NULLIF(EXCLUDED.isbn, ''), booksonix_records.isbn),
                        title = EXCLUDED.title,
                        publisher = EXCLUDED.publisher,
                        price = EXCLUDED.price,
                        last_updated = CURRENT_TIMESTAMP
                    RETURNING id, (xmax = 0) AS inserted`,
                    [sku, isbn || null, title, '', publisher, price, 0]
                );

                if (result.rows[0].inserted) {
                    newRecords++;
                } else {
                    duplicates++;
                }
            } catch (err) {
                console.error('Error inserting Booksonix record:', err.message);
                errors++;
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        res.json({ 
            success: true, 
            message: `Processed ${data.length} records. New: ${newRecords}, Updated: ${duplicates}, Errors: ${errors}`,
            newRecords: newRecords,
            duplicates: duplicates,
            errors: errors
        });

    } catch (error) {
        console.error('Booksonix upload error:', error);
        if (fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/booksonix/records', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM booksonix_records ORDER BY upload_date DESC LIMIT 500'
        );
        res.json({ records: result.rows });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.get('/api/booksonix/stats', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT sku) as unique_skus,
                COUNT(DISTINCT publisher) as publishers
            FROM booksonix_records
        `);
        
        res.json({
            totalRecords: result.rows[0].total_records || 0,
            uniqueSKUs: result.rows[0].unique_skus || 0,
            totalPublishers: result.rows[0].publishers || 0
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// CUSTOMER API ROUTES (Placeholder - to be connected to your data source)
// =============================================

app.get('/api/customers', async (req, res) => {
    try {
        // TODO: Replace this with your actual customer data source query
        // For now, returning empty structure
        res.json({
            customers: [],
            stats: {
                total_customers: 0,
                total_countries: 0,
                total_orders: 0,
                total_cities: 0
            }
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// REPORT API ROUTES (Placeholder - to be configured with your data source)
// =============================================

app.get('/api/titles', async (req, res) => {
    try {
        // TODO: Replace with your actual titles data source
        res.json([]);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/generate-report', async (req, res) => {
    try {
        const { publisher, startDate, endDate, titles } = req.body;
        
        // TODO: Replace with your actual report generation logic
        // For now, returning empty structure
        res.json({
            data: [],
            totalCustomers: 0,
            publisher: publisher,
            startDate: startDate,
            endDate: endDate,
            titles: titles
        });
        
    } catch (error) {
        console.error('Generate report error:', error);
        res.status(500).json({ error: 'Failed to generate report: ' + error.message });
    }
});

// =============================================
// USER MANAGEMENT ROUTES
// =============================================

app.get('/api/users', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT id, username, email, role, created_at, last_login FROM users ORDER BY id'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/users', async (req, res) => {
    const { username, email, password, role } = req.body;

    if (!username || !email || !password) {
        return res.status(400).json({ error: 'Username, email, and password are required' });
    }

    try {
        const hashedPassword = await bcrypt.hash(password, 10);
        const result = await pool.query(
            'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4) RETURNING id',
            [username, email, hashedPassword, role || 'editor']
        );
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        if (err.code === '23505') {
            res.status(400).json({ error: 'Username or email already exists' });
        } else {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        }
    }
});

app.put('/api/users/:id', async (req, res) => {
    const { id } = req.params;
    const { username, email, password, role } = req.body;

    try {
        let query = 'UPDATE users SET username = $1, email = $2, role = $3';
        let params = [username, email, role];
        let paramIndex = 4;

        if (password) {
            const hashedPassword = await bcrypt.hash(password, 10);
            query += `, password = $${paramIndex}`;
            params.push(hashedPassword);
            paramIndex++;
        }

        query += ` WHERE id = $${paramIndex}`;
        params.push(id);

        const result = await pool.query(query, params);

        if (result.rowCount === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        res.json({ success: true });
    } catch (err) {
        if (err.code === '23505') {
            res.status(400).json({ error: 'Username or email already exists' });
        } else {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        }
    }
});

app.delete('/api/users/:id', async (req, res) => {
    const { id } = req.params;

    try {
        const userResult = await pool.query('SELECT role FROM users WHERE id = $1', [id]);
        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        if (userResult.rows[0].role === 'admin') {
            const adminCount = await pool.query(
                "SELECT COUNT(*) as count FROM users WHERE role = 'admin' AND id != $1",
                [id]
            );
            
            if (adminCount.rows[0].count === '0') {
                return res.status(400).json({ error: 'Cannot delete the last admin user' });
            }
        }

        await pool.query('DELETE FROM users WHERE id = $1', [id]);
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// SETTINGS ROUTES
// =============================================

app.get('/api/mappings', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM customer_mappings ORDER BY id'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Error loading mappings:', err);
        res.status(500).json({ error: 'Error loading mappings' });
    }
});

app.post('/api/mappings', async (req, res) => {
    const { original_name, display_name } = req.body;
    try {
        await pool.query(
            'INSERT INTO customer_mappings (original_name, display_name) VALUES ($1, $2)',
            [original_name, display_name]
        );
        res.json({ success: true });
    } catch (err) {
        console.error('Error adding mapping:', err);
        res.status(500).json({ error: 'Error adding mapping' });
    }
});

app.delete('/api/mappings/:id', async (req, res) => {
    const { id } = req.params;
    try {
        await pool.query('DELETE FROM customer_mappings WHERE id = $1', [id]);
        res.json({ success: true });
    } catch (err) {
        console.error('Error deleting mapping:', err);
        res.status(500).json({ error: 'Error deleting mapping' });
    }
});

app.get('/api/stats', async (req, res) => {
    try {
        // Get Booksonix stats for the settings page
        const booksonixResult = await pool.query(
            'SELECT COUNT(*) as total FROM booksonix_records'
        );
        
        res.json({
            total_records: 0,  // Placeholder for your data
            total_customers: 0,  // Placeholder for your data
            total_titles: 0,  // Placeholder for your data
            excluded_customers: 0,  // Placeholder for your data
            total_booksonix: booksonixResult.rows[0].total || 0
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.put('/api/settings', async (req, res) => {
    // In a production app, you'd save these settings to the database
    console.log('Settings update:', req.body);
    res.json({ success: true });
});

app.delete('/api/clear-booksonix', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM booksonix_records');
        res.json({ 
            success: true, 
            message: `Successfully cleared ${result.rowCount} Booksonix records`,
            deletedCount: result.rowCount
        });
    } catch (err) {
        console.error('Error clearing Booksonix records:', err);
        res.status(500).json({ error: 'Failed to clear Booksonix records' });
    }
});

// Export data endpoints (placeholders for now)
app.get('/api/export-all', async (req, res) => {
    try {
        // TODO: Implement export based on your data structure
        res.status(501).json({ message: 'Export functionality to be implemented' });
    } catch (err) {
        res.status(500).json({ error: 'Export failed' });
    }
});

app.get('/api/backup', async (req, res) => {
    try {
        // TODO: Implement backup based on your needs
        res.json({ success: true, message: 'Backup functionality to be implemented' });
    } catch (err) {
        res.status(500).json({ error: 'Backup failed' });
    }
});

// Clear data endpoints (modified for your structure)
app.delete('/api/clear-data', async (req, res) => {
    try {
        // TODO: Implement based on your data tables
        res.json({ 
            success: true, 
            message: 'Clear data functionality to be implemented for your tables' 
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to clear data' });
    }
});

app.post('/api/reset-exclusions', async (req, res) => {
    try {
        // Placeholder - implement if you need exclusions functionality
        res.json({ 
            success: true, 
            message: 'Exclusions functionality not implemented in this version' 
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to reset exclusions' });
    }
});

// =============================================
// HEALTH CHECK
// =============================================

app.get('/health', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.json({ 
            status: 'healthy', 
            timestamp: new Date().toISOString(),
            database: 'connected'
        });
    } catch (err) {
        res.status(503).json({ 
            status: 'unhealthy', 
            timestamp: new Date().toISOString(),
            database: 'disconnected',
            error: err.message
        });
    }
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Visit http://localhost:${PORT} to access the application`);
    console.log('=================================');
    console.log('IMPORTANT: Default admin credentials');
    console.log('Username: admin');
    console.log('Password: admin123');
    console.log('Please change this password after first login!');
    console.log('=================================');
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down gracefully...');
    pool.end(() => {
        console.log('Database pool closed.');
        process.exit(0);
    });
});
