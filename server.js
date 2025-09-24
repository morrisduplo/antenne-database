const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const multer = require('multer');
const XLSX = require('xlsx');
const bcrypt = require('bcryptjs');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Multer configuration for file uploads
const upload = multer({ 
    dest: 'uploads/',
    limits: { fileSize: 50 * 1024 * 1024 } // 50MB limit
});

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
    if (err) {
        console.error('Database connection error:', err);
    } else {
        console.log('Database connected successfully at:', res.rows[0].now);
    }
});

// Initialize database tables
async function initializeDatabase() {
    try {
        // Users table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                role VARCHAR(50) DEFAULT 'viewer',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Check if admin user exists
        const adminCheck = await pool.query('SELECT * FROM users WHERE username = $1', ['admin']);
        if (adminCheck.rows.length === 0) {
            const hashedPassword = await bcrypt.hash('admin123', 10);
            await pool.query(
                'INSERT INTO users (username, password, email, role) VALUES ($1, $2, $3, $4)',
                ['admin', hashedPassword, 'admin@antennebooks.com', 'admin']
            );
            console.log('Default admin user created (username: admin, password: admin123)');
        }

        // Gazelle sales table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS gazelle_sales (
                id SERIAL PRIMARY KEY,
                order_ref VARCHAR(255),
                order_date DATE,
                customer_code VARCHAR(100),
                customer_number VARCHAR(100),
                customer_name VARCHAR(255),
                invoice VARCHAR(100),
                title TEXT,
                imprint VARCHAR(255),
                isbn13 VARCHAR(20),
                quantity INTEGER DEFAULT 0,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                carrier VARCHAR(100),
                tracking VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(100),
                publisher VARCHAR(255),
                format VARCHAR(100),
                discount DECIMAL(5,2),
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR(255),
                UNIQUE(order_ref, invoice, isbn13)
            )
        `);

        // Booksonix table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS booksonix (
                id SERIAL PRIMARY KEY,
                sku VARCHAR(100) UNIQUE,
                isbn VARCHAR(20),
                title VARCHAR(500),
                publisher VARCHAR(255),
                price DECIMAL(10,2),
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Customer name mappings
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_name_mappings (
                id SERIAL PRIMARY KEY,
                original_name VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        console.log('Database tables initialized successfully');
    } catch (error) {
        console.error('Error initializing database:', error);
    }
}

// Initialize database on startup
initializeDatabase();

// Login endpoint
app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        
        const result = await pool.query('SELECT * FROM users WHERE username = $1', [username]);
        
        if (result.rows.length === 0) {
            return res.status(401).json({ error: 'Invalid username or password' });
        }
        
        const user = result.rows[0];
        const passwordMatch = await bcrypt.compare(password, user.password);
        
        if (!passwordMatch) {
            return res.status(401).json({ error: 'Invalid username or password' });
        }
        
        res.json({ 
            user: {
                id: user.id,
                username: user.username,
                email: user.email,
                role: user.role
            }
        });
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// GAZELLE UPLOAD - FIXED FOR YOUR FORMAT
app.post('/api/gazelle/upload', upload.single('gazelleFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    try {
        console.log('Processing Gazelle file:', req.file.originalname);
        
        // Read the Excel file
        const workbook = XLSX.readFile(req.file.path);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        // Convert to JSON - DO NOT SKIP HEADER ROW
        // Use header: 1 to get raw arrays, then process manually
        const rawData = XLSX.utils.sheet_to_json(worksheet, { 
            header: 1,
            defval: '',
            raw: false,
            dateNF: 'yyyy-mm-dd'
        });

        console.log('Raw data rows:', rawData.length);
        
        if (rawData.length < 2) {
            fs.unlinkSync(req.file.path);
            return res.status(400).json({ error: 'File appears to be empty or invalid' });
        }

        // Skip first row (it might contain other info), headers are on row 2 (index 1)
        const headers = rawData[1]; // Second row contains headers
        const dataRows = rawData.slice(2); // Data starts from third row
        
        console.log('Headers found:', headers);
        console.log('Data rows to process:', dataRows.length);

        let newRecords = 0;
        let duplicates = 0;
        let errors = 0;

        // Process each data row
        for (const row of dataRows) {
            // Skip empty rows
            if (!row || row.length === 0 || !row[1]) continue; // Check if row has data
            
            try {
                // Map columns based on your specification
                // Note: Excel columns are 0-indexed in the array
                const recordData = {
                    order_date: row[0] || null, // Column A - Date
                    customer_code: row[1] || '', // Column B - Cus
                    customer_number: row[2] || '', // Column C - Cus No
                    customer_name: row[3] || '', // Column D - Name
                    invoice: row[4] || '', // Column E - Invoice
                    title: row[5] || '', // Column F - Title
                    imprint: row[6] || '', // Column G - Imprint
                    isbn13: row[7] || '', // Column H - Book EAN
                    quantity: parseInt(row[8]) || 0, // Column I - Quantity
                    total_amount: parseFloat(row[9]) || 0, // Column J - TOTAL
                    carrier: row[10] || '', // Column K - Carrier
                    tracking: row[11] || '', // Column L - Tracking
                    file_name: req.file.originalname
                };

                // Parse date if it exists
                if (recordData.order_date) {
                    // Handle Excel serial dates
                    if (!isNaN(recordData.order_date)) {
                        const excelDate = parseInt(recordData.order_date);
                        const date = new Date((excelDate - 25569) * 86400 * 1000);
                        recordData.order_date = date.toISOString().split('T')[0];
                    } else if (recordData.order_date.includes('/')) {
                        // Handle date strings like "19/09/2025"
                        const parts = recordData.order_date.split('/');
                        if (parts.length === 3) {
                            recordData.order_date = `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
                        }
                    }
                }

                // Calculate unit price if we have quantity and total
                if (recordData.quantity > 0 && recordData.total_amount > 0) {
                    recordData.unit_price = recordData.total_amount / recordData.quantity;
                } else {
                    recordData.unit_price = recordData.total_amount;
                }

                // Generate order reference from invoice if needed
                recordData.order_ref = recordData.invoice || `ORD-${Date.now()}`;

                // Apply customer name mappings
                const mappingResult = await pool.query(
                    'SELECT display_name FROM customer_name_mappings WHERE original_name = $1',
                    [recordData.customer_name]
                );

                if (mappingResult.rows.length > 0) {
                    recordData.customer_name = mappingResult.rows[0].display_name;
                }

                // Insert or update the record
                const insertQuery = `
                    INSERT INTO gazelle_sales (
                        order_ref, order_date, customer_code, customer_number,
                        customer_name, invoice, title, imprint, isbn13,
                        quantity, unit_price, total_amount, carrier, tracking,
                        file_name, upload_date
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP)
                    ON CONFLICT (order_ref, invoice, isbn13) 
                    DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        total_amount = EXCLUDED.total_amount,
                        upload_date = CURRENT_TIMESTAMP
                    RETURNING id
                `;

                const values = [
                    recordData.order_ref,
                    recordData.order_date,
                    recordData.customer_code,
                    recordData.customer_number,
                    recordData.customer_name,
                    recordData.invoice,
                    recordData.title,
                    recordData.imprint,
                    recordData.isbn13,
                    recordData.quantity,
                    recordData.unit_price,
                    recordData.total_amount,
                    recordData.carrier,
                    recordData.tracking,
                    recordData.file_name
                ];

                await pool.query(insertQuery, values);
                newRecords++;

            } catch (recordError) {
                console.error('Error processing record:', recordError);
                errors++;
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        console.log(`Upload complete: ${newRecords} new, ${duplicates} duplicates, ${errors} errors`);

        res.json({
            success: true,
            message: `Successfully processed ${req.file.originalname}`,
            newRecords,
            duplicates,
            errors
        });

    } catch (error) {
        console.error('Gazelle upload error:', error);
        
        // Clean up file on error
        if (req.file && req.file.path) {
            try {
                fs.unlinkSync(req.file.path);
            } catch (unlinkError) {
                console.error('Error deleting file:', unlinkError);
            }
        }
        
        res.status(500).json({ 
            error: 'Failed to process file',
            details: error.message 
        });
    }
});

// Test parse endpoint for debugging
app.post('/api/test-gazelle-parse', upload.single('testFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    try {
        const workbook = XLSX.readFile(req.file.path);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        // Get raw data
        const rawData = XLSX.utils.sheet_to_json(worksheet, { 
            header: 1,
            defval: '',
            raw: false
        });

        // Parse with headers on row 2
        const headers = rawData[1];
        const dataRows = rawData.slice(2);
        
        const parsedSample = dataRows.slice(0, 5).map(row => {
            return {
                Date: row[0],
                Cus: row[1],
                'Cus No': row[2],
                Name: row[3],
                Invoice: row[4],
                Title: row[5],
                Imprint: row[6],
                'Book EAN': row[7],
                Quantity: row[8],
                TOTAL: row[9],
                Carrier: row[10],
                Tracking: row[11]
            };
        });

        fs.unlinkSync(req.file.path);

        res.json({
            rawRowCount: rawData.length,
            headers: headers,
            firstRawRows: rawData.slice(0, 5),
            method2Count: dataRows.length,
            method2Sample: parsedSample
        });

    } catch (error) {
        if (req.file && req.file.path) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: error.message });
    }
});

// Get Gazelle records
app.get('/api/gazelle/records', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 1000;
        const offset = parseInt(req.query.offset) || 0;
        
        const result = await pool.query(
            'SELECT * FROM gazelle_sales ORDER BY upload_date DESC, id DESC LIMIT $1 OFFSET $2',
            [limit, offset]
        );
        
        const countResult = await pool.query('SELECT COUNT(*) FROM gazelle_sales');
        
        res.json({
            records: result.rows,
            totalRecords: parseInt(countResult.rows[0].count)
        });
    } catch (error) {
        console.error('Error fetching Gazelle records:', error);
        res.status(500).json({ error: 'Failed to fetch records' });
    }
});

// Get Gazelle statistics
app.get('/api/gazelle/stats', async (req, res) => {
    try {
        const stats = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT order_ref) as unique_orders,
                SUM(quantity) as total_quantity,
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT customer_name) as unique_customers,
                COUNT(DISTINCT title) as unique_titles
            FROM gazelle_sales
        `);
        
        res.json({
            totalRecords: parseInt(stats.rows[0].total_records),
            uniqueOrders: parseInt(stats.rows[0].unique_orders),
            totalQuantity: parseInt(stats.rows[0].total_quantity) || 0,
            totalRevenue: parseFloat(stats.rows[0].total_revenue) || 0,
            uniqueCustomers: parseInt(stats.rows[0].unique_customers),
            uniqueTitles: parseInt(stats.rows[0].unique_titles)
        });
    } catch (error) {
        console.error('Error fetching stats:', error);
        res.status(500).json({ error: 'Failed to fetch statistics' });
    }
});

// Booksonix upload endpoint
app.post('/api/booksonix/upload', upload.single('booksonixFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    try {
        const workbook = XLSX.readFile(req.file.path);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        const data = XLSX.utils.sheet_to_json(worksheet);
        
        let newRecords = 0;
        let duplicates = 0;

        for (const row of data) {
            const sku = row.SKU || row.sku || row['SKU'] || '';
            const cleanedSKU = sku.toString().replace(/-/g, '');
            
            if (!cleanedSKU) continue;
            
            try {
                await pool.query(
                    `INSERT INTO booksonix (sku, isbn, title, publisher, price)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (sku) DO NOTHING`,
                    [
                        cleanedSKU,
                        row.ISBN || row.isbn || '',
                        row.Title || row.title || '',
                        row.Publisher || row.publisher || '',
                        parseFloat(row.Price || row.price || 0)
                    ]
                );
                newRecords++;
            } catch (err) {
                if (err.code === '23505') {
                    duplicates++;
                } else {
                    throw err;
                }
            }
        }
        
        fs.unlinkSync(req.file.path);
        
        res.json({
            success: true,
            message: `Processed ${req.file.originalname}`,
            newRecords,
            duplicates
        });
    } catch (error) {
        if (req.file && req.file.path) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: error.message });
    }
});

// Get Booksonix records
app.get('/api/booksonix/records', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 500;
        const offset = (page - 1) * limit;
        
        const result = await pool.query(
            'SELECT * FROM booksonix ORDER BY upload_date DESC LIMIT $1 OFFSET $2',
            [limit, offset]
        );
        
        const countResult = await pool.query('SELECT COUNT(*) FROM booksonix');
        
        res.json({
            records: result.rows,
            totalRecords: parseInt(countResult.rows[0].count)
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get Booksonix statistics
app.get('/api/booksonix/stats', async (req, res) => {
    try {
        const stats = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT sku) as unique_skus,
                COUNT(DISTINCT isbn) as unique_isbns
            FROM booksonix
        `);
        
        res.json({
            totalRecords: parseInt(stats.rows[0].total_records),
            uniqueSKUs: parseInt(stats.rows[0].unique_skus),
            uniqueISBNs: parseInt(stats.rows[0].unique_isbns)
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Clear data endpoint
app.delete('/api/clear-data', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM gazelle_sales');
        res.json({ 
            success: true,
            message: 'All Gazelle records cleared',
            deletedCount: result.rowCount
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Clear Booksonix data
app.delete('/api/clear-booksonix', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM booksonix');
        res.json({ 
            success: true,
            message: 'All Booksonix records cleared',
            deletedCount: result.rowCount
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get titles for reports
app.get('/api/titles', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT DISTINCT title FROM gazelle_sales WHERE title IS NOT NULL ORDER BY title'
        );
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Generate report endpoint
app.post('/api/generate-report', async (req, res) => {
    try {
        const { publisher, startDate, endDate, titles } = req.body;
        
        const query = `
            SELECT DISTINCT
                customer_name,
                MAX(customer_code) as customer_code,
                MAX(city) as city,
                MAX(country) as country,
                COUNT(DISTINCT order_ref) as total_orders,
                SUM(quantity) as total_quantity,
                MAX(order_date) as last_order
            FROM gazelle_sales
            WHERE title = ANY($1)
            AND order_date >= $2
            AND order_date <= $3
            GROUP BY customer_name
            ORDER BY country, city, customer_name
        `;
        
        const result = await pool.query(query, [titles, startDate, endDate]);
        
        res.json({
            success: true,
            data: result.rows,
            totalCustomers: result.rows.length
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Customer name mappings
app.get('/api/mappings', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM customer_name_mappings ORDER BY original_name');
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/mappings', async (req, res) => {
    try {
        const { original_name, display_name } = req.body;
        await pool.query(
            'INSERT INTO customer_name_mappings (original_name, display_name) VALUES ($1, $2)',
            [original_name, display_name]
        );
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/mappings/:id', async (req, res) => {
    try {
        await pool.query('DELETE FROM customer_name_mappings WHERE id = $1', [req.params.id]);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// User management
app.get('/api/users', async (req, res) => {
    try {
        const result = await pool.query('SELECT id, username, email, role, created_at FROM users ORDER BY username');
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/users', async (req, res) => {
    try {
        const { username, password, email, role } = req.body;
        const hashedPassword = await bcrypt.hash(password, 10);
        
        await pool.query(
            'INSERT INTO users (username, password, email, role) VALUES ($1, $2, $3, $4)',
            [username, hashedPassword, email, role]
        );
        
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/users/:id', async (req, res) => {
    try {
        await pool.query('DELETE FROM users WHERE id = $1', [req.params.id]);
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Default route
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});
