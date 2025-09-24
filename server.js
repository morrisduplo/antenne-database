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

// Configure multer for file uploads
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

// Initialize Gazelle Sales table - FIXED VERSION WITHOUT UNIQUE CONSTRAINT
async function initGazelleTable() {
    try {
        // First check if table exists
        const tableExists = await pool.query(`
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'gazelle_sales'
            );
        `);

        if (!tableExists.rows[0].exists) {
            // Create table WITHOUT the problematic unique constraint
            console.log('Creating Gazelle Sales table...');
            await pool.query(`
                CREATE TABLE gazelle_sales (
                    id SERIAL PRIMARY KEY,
                    order_ref VARCHAR(100),
                    order_date DATE,
                    customer_name VARCHAR(500),
                    city VARCHAR(200),
                    country VARCHAR(100),
                    title VARCHAR(500),
                    isbn13 VARCHAR(50),
                    quantity INTEGER DEFAULT 0,
                    unit_price DECIMAL(10,2),
                    discount DECIMAL(5,2) DEFAULT 0,
                    publisher VARCHAR(500),
                    format VARCHAR(100),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `);
            
            console.log('Gazelle Sales table created successfully');
        } else {
            // Table exists - remove the problematic constraint if it exists
            console.log('Gazelle Sales table exists, checking constraints...');
            try {
                await pool.query(`
                    ALTER TABLE gazelle_sales 
                    DROP CONSTRAINT IF EXISTS gazelle_sales_order_ref_isbn13_customer_name_key
                `);
                console.log('Removed unique constraint if it existed');
            } catch (e) {
                // Constraint didn't exist, that's fine
            }
        }
        
        // Create indexes for better performance
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_gazelle_order_ref ON gazelle_sales(order_ref)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_gazelle_customer ON gazelle_sales(customer_name)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_gazelle_isbn ON gazelle_sales(isbn13)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_gazelle_date ON gazelle_sales(order_date)`);
        
        console.log('Gazelle Sales table initialized successfully');
        
    } catch (err) {
        console.error('Error initializing Gazelle Sales table:', err);
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
        
        // Initialize Gazelle table
        await initGazelleTable();

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

// Gazelle page
app.get('/gazelle', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'gazelle.html'));
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
            let sku = row['SKU'] || row['sku'] || row['Sku'] || 
                       row['Product SKU'] || row['Product Code'] || 
                       row['Item Code'] || row['Code'] || '';
            
            // Clean SKU - remove hyphens
            if (sku) {
                sku = String(sku).replace(/-/g, '').trim();
            }
            
            if (!sku) {
                skippedNoSku++;
                errors++;
                continue;
            }

            // Get ISBN and remove hyphens from it
            let isbn = row['ISBN-13'] || row['ISBN13'] || row['isbn-13'] || row['ISBN'] || row['EAN'] || '';
            if (isbn) {
                // Remove hyphens from ISBN
                isbn = String(isbn).replace(/-/g, '').trim();
            }
            
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
        const limit = parseInt(req.query.limit) || 500;
        const page = parseInt(req.query.page) || 1;
        const offset = (page - 1) * limit;
        
        // Get total count
        const countResult = await pool.query('SELECT COUNT(*) as total FROM booksonix_records');
        const totalRecords = parseInt(countResult.rows[0].total);
        
        // Get records
        const result = await pool.query(
            'SELECT * FROM booksonix_records ORDER BY upload_date DESC LIMIT $1 OFFSET $2',
            [limit, offset]
        );
        
        res.json({ 
            records: result.rows,
            totalRecords: totalRecords,
            page: page,
            limit: limit
        });
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
                COUNT(DISTINCT isbn) as unique_isbns,
                COUNT(DISTINCT publisher) as publishers
            FROM booksonix_records
        `);
        
        res.json({
            totalRecords: result.rows[0].total_records || 0,
            uniqueSKUs: result.rows[0].unique_skus || 0,
            uniqueISBNs: result.rows[0].unique_isbns || 0,
            totalPublishers: result.rows[0].publishers || 0
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// GAZELLE SALES ROUTES - COMPLETELY FIXED VERSION
// =============================================

// Helper function to parse Excel dates
function parseExcelDate(value) {
    if (!value) return null;
    
    // If it's already a Date object
    if (value instanceof Date) {
        return value;
    }
    
    // If it's a number (Excel serial date)
    if (typeof value === 'number') {
        // Excel dates start from 1900-01-01
        // The difference is 25569 days for Windows Excel
        const excelDate = new Date((value - 25569) * 86400 * 1000);
        if (!isNaN(excelDate.getTime())) {
            return excelDate;
        }
    }
    
    // If it's a string
    if (typeof value === 'string') {
        const trimmedValue = value.trim();
        
        // Try parsing as ISO date first
        let date = new Date(trimmedValue);
        if (!isNaN(date.getTime())) {
            return date;
        }
        
        // Try DD/MM/YYYY format (UK format) - most common in your data
        if (trimmedValue.match(/^\d{1,2}\/\d{1,2}\/\d{4}$/)) {
            const parts = trimmedValue.split('/');
            const day = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10);
            const year = parseInt(parts[2], 10);
            
            // Create date (month is 0-indexed in JavaScript)
            date = new Date(year, month - 1, day);
            if (!isNaN(date.getTime())) {
                return date;
            }
        }
        
        // Try YYYY-MM-DD format
        if (trimmedValue.match(/^\d{4}-\d{2}-\d{2}$/)) {
            date = new Date(trimmedValue);
            if (!isNaN(date.getTime())) {
                return date;
            }
        }
    }
    
    return null;
}

// Upload Gazelle Sales file - COMPLETELY REWRITTEN
app.post('/api/gazelle/upload', upload.single('gazelleFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('========================================');
    console.log('Processing Gazelle Sales file:', req.file.originalname);
    console.log('File path:', req.file.path);

    try {
        // Read the Excel file with specific options for better compatibility
        const workbook = xlsx.readFile(req.file.path, {
            type: 'buffer',
            cellDates: true,
            cellNF: false,
            cellText: false,
            raw: false
        });
        
        const sheetName = workbook.SheetNames[0];
        console.log('Sheet name:', sheetName);
        
        // Convert to JSON - let xlsx handle the parsing
        const rawData = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName], { 
            raw: false,
            defval: '',
            blankrows: false
        });

        console.log('Total rows found in Excel:', rawData.length);
        
        // Log all column names from first row to debug
        if (rawData.length > 0) {
            console.log('Available columns:', Object.keys(rawData[0]));
        }

        let newRecords = 0;
        let updatedRecords = 0;
        let errors = 0;
        let skippedRows = 0;
        const errorDetails = [];

        // Process each row
        for (let i = 0; i < rawData.length; i++) {
            const row = rawData[i];
            
            try {
                // Map columns - being flexible with column names
                // Try multiple possible column name variations
                const orderRef = (
                    row['Order Ref'] || 
                    row['OrderRef'] || 
                    row['order_ref'] || 
                    row['Order Reference'] ||
                    row['Order No'] ||
                    row['Order Number'] ||
                    ''
                ).toString().trim();
                
                const customerName = (
                    row['Customer Name'] || 
                    row['CustomerName'] || 
                    row['customer_name'] || 
                    row['Customer'] ||
                    row['Retailer Name'] ||
                    ''
                ).toString().trim();
                
                const city = (
                    row['City'] || 
                    row['city'] || 
                    row['Town'] ||
                    ''
                ).toString().trim();
                
                const country = (
                    row['Country'] || 
                    row['country'] || 
                    row['Country Code'] ||
                    ''
                ).toString().trim();
                
                const title = (
                    row['Title'] || 
                    row['title'] || 
                    row['Book Title'] ||
                    row['Product Title'] ||
                    ''
                ).toString().trim();
                
                const isbn13 = (
                    row['ISBN-13'] || 
                    row['ISBN13'] || 
                    row['isbn13'] || 
                    row['ISBN'] ||
                    row['EAN'] ||
                    ''
                ).toString().replace(/-/g, '').trim();
                
                const publisher = (
                    row['Publishers'] || 
                    row['Publisher'] || 
                    row['publisher'] ||
                    ''
                ).toString().trim();
                
                const format = (
                    row['Format'] || 
                    row['format'] || 
                    row['Book Format'] ||
                    ''
                ).toString().trim();

                // Parse numeric values
                let quantity = 0;
                const quantityValue = row['Quantity'] || row['quantity'] || row['Qty'] || 0;
                if (quantityValue) {
                    quantity = parseInt(quantityValue.toString().replace(/[^\d-]/g, '')) || 0;
                }
                
                let unitPrice = 0;
                const priceValue = row['Unit Price'] || row['unit price'] || row['Price'] || 0;
                if (priceValue) {
                    const cleanPrice = priceValue.toString()
                        .replace(/[£$€]/g, '')
                        .replace(/,/g, '')
                        .trim();
                    unitPrice = parseFloat(cleanPrice) || 0;
                }
                
                let discount = 0;
                const discountValue = row['Discount %'] || row['Discount'] || row['discount'] || 0;
                if (discountValue) {
                    discount = parseFloat(discountValue.toString().replace(/%/g, '')) || 0;
                }

                // Parse order date
                let parsedDate = null;
                const dateValue = row['Order Date'] || row['OrderDate'] || row['order_date'] || row['Date'];
                if (dateValue) {
                    parsedDate = parseExcelDate(dateValue);
                }

                // Debug first 3 rows
                if (i < 3) {
                    console.log(`\nRow ${i + 1} parsed data:`, {
                        orderRef,
                        customerName,
                        city,
                        country,
                        dateRaw: dateValue,
                        dateParsed: parsedDate,
                        quantity,
                        unitPrice,
                        discount
                    });
                }

                // Validate essential fields
                if (!orderRef || !customerName) {
                    console.log(`Row ${i + 1}: Skipped - missing essential data (orderRef: "${orderRef}", customer: "${customerName}")`);
                    skippedRows++;
                    continue;
                }

                // Simple INSERT without complex constraints
                // We'll just insert everything and let duplicates be duplicates for now
                const result = await pool.query(
                    `INSERT INTO gazelle_sales 
                    (order_ref, order_date, customer_name, city, country, title, isbn13, 
                     quantity, unit_price, discount, publisher, format) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    RETURNING id`,
                    [orderRef, parsedDate, customerName, city, country, title, isbn13, 
                     quantity, unitPrice, discount, publisher, format]
                );

                if (result.rows.length > 0) {
                    newRecords++;
                    console.log(`Row ${i + 1}: Inserted new record with ID ${result.rows[0].id}`);
                }

            } catch (err) {
                // Check if it's a duplicate error or something else
                if (err.message.includes('duplicate') || err.code === '23505') {
                    updatedRecords++;
                    console.log(`Row ${i + 1}: Record already exists (duplicate)`);
                } else {
                    console.error(`Error processing row ${i + 1}:`, err.message);
                    errors++;
                    errorDetails.push({
                        row: i + 1,
                        error: err.message,
                        data: {
                            orderRef: row['Order Ref'],
                            customer: row['Customer Name']
                        }
                    });
                }
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        console.log('========================================');
        console.log('Upload Summary:');
        console.log('- Total rows:', rawData.length);
        console.log('- New records:', newRecords);
        console.log('- Duplicate records:', updatedRecords);
        console.log('- Skipped rows:', skippedRows);
        console.log('- Errors:', errors);
        if (errorDetails.length > 0) {
            console.log('Error details:', errorDetails.slice(0, 5)); // Show first 5 errors
        }
        console.log('========================================');

        // Return response
        let message = `Processed ${rawData.length} rows. `;
        if (newRecords > 0) message += `${newRecords} new records added. `;
        if (updatedRecords > 0) message += `${updatedRecords} duplicates found. `;
        if (skippedRows > 0) message += `${skippedRows} rows skipped. `;
        if (errors > 0) message += `${errors} errors occurred.`;

        res.json({ 
            success: true, 
            message: message,
            newRecords: newRecords,
            duplicates: updatedRecords,
            errors: errors,
            details: {
                totalRows: rawData.length,
                newRecords,
                updatedRecords,
                skippedRows,
                errors,
                errorSample: errorDetails.slice(0, 5)
            }
        });

    } catch (error) {
        console.error('Fatal error during Gazelle upload:', error);
        console.error('Stack trace:', error.stack);
        
        // Clean up file if it still exists
        if (fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        
        res.status(500).json({ 
            error: 'Upload failed: ' + error.message,
            details: error.stack 
        });
    }
});

// Get Gazelle Sales records
app.get('/api/gazelle/records', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 999999; // Default to all records
        const page = parseInt(req.query.page) || 1;
        const offset = (page - 1) * limit;
        
        // Get total count
        const countResult = await pool.query('SELECT COUNT(*) as total FROM gazelle_sales');
        const totalRecords = parseInt(countResult.rows[0].total);
        
        // Get records
        const result = await pool.query(
            `SELECT * FROM gazelle_sales 
             ORDER BY upload_date DESC, order_date DESC 
             LIMIT $1 OFFSET $2`,
            [limit, offset]
        );
        
        res.json({ 
            records: result.rows,
            totalRecords: totalRecords,
            page: page,
            limit: limit
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get Gazelle Sales statistics
app.get('/api/gazelle/stats', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT order_ref) as unique_orders,
                SUM(quantity) as total_quantity,
                SUM(quantity * unit_price * (1 - discount/100)) as total_revenue,
                COUNT(DISTINCT customer_name) as unique_customers,
                COUNT(DISTINCT title) as unique_titles,
                COUNT(DISTINCT publisher) as unique_publishers
            FROM gazelle_sales
        `);
        
        res.json({
            totalRecords: result.rows[0].total_records || 0,
            uniqueOrders: result.rows[0].unique_orders || 0,
            totalQuantity: result.rows[0].total_quantity || 0,
            totalRevenue: parseFloat(result.rows[0].total_revenue || 0),
            uniqueCustomers: result.rows[0].unique_customers || 0,
            uniqueTitles: result.rows[0].unique_titles || 0,
            uniquePublishers: result.rows[0].unique_publishers || 0
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// CUSTOMER API ROUTES
// =============================================

app.get('/api/customers', async (req, res) => {
    try {
        // Get customers from Gazelle Sales data
        const result = await pool.query(`
            SELECT 
                customer_name,
                city,
                country,
                COUNT(DISTINCT order_ref) as total_orders,
                SUM(quantity) as total_quantity,
                SUM(quantity * unit_price * (1 - discount/100)) as total_revenue,
                MAX(order_date) as last_order
            FROM gazelle_sales
            GROUP BY customer_name, city, country
            ORDER BY customer_name
        `);
        
        // Get statistics
        const stats = await pool.query(`
            SELECT 
                COUNT(DISTINCT customer_name) as total_customers,
                COUNT(DISTINCT country) as total_countries,
                COUNT(DISTINCT city) as total_cities,
                COUNT(DISTINCT order_ref) as total_orders
            FROM gazelle_sales
        `);
        
        res.json({
            customers: result.rows,
            stats: stats.rows[0]
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// REPORT API ROUTES
// =============================================

app.get('/api/titles', async (req, res) => {
    try {
        // Get unique titles from Gazelle Sales
        const result = await pool.query(`
            SELECT DISTINCT title 
            FROM gazelle_sales 
            WHERE title IS NOT NULL AND title != ''
            ORDER BY title
        `);
        
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/generate-report', async (req, res) => {
    try {
        const { publisher, startDate, endDate, titles } = req.body;
        
        // Build the query
        let query = `
            SELECT DISTINCT
                customer_name,
                city,
                country,
                COUNT(DISTINCT order_ref) as total_orders,
                SUM(quantity) as total_quantity,
                MAX(order_date) as last_order
            FROM gazelle_sales
            WHERE 1=1
        `;
        
        const params = [];
        let paramIndex = 1;
        
        // Add date filter if provided
        if (startDate) {
            query += ` AND order_date >= $${paramIndex}`;
            params.push(startDate);
            paramIndex++;
        }
        
        if (endDate) {
            query += ` AND order_date <= $${paramIndex}`;
            params.push(endDate);
            paramIndex++;
        }
        
        // Add title filter if provided
        if (titles && titles.length > 0) {
            const titlePlaceholders = titles.map((_, i) => `$${paramIndex + i}`).join(',');
            query += ` AND title IN (${titlePlaceholders})`;
            params.push(...titles);
            paramIndex += titles.length;
        }
        
        // Add publisher filter if provided
        if (publisher) {
            query += ` AND LOWER(publisher) LIKE LOWER($${paramIndex})`;
            params.push(`%${publisher}%`);
            paramIndex++;
        }
        
        query += ` GROUP BY customer_name, city, country ORDER BY country, city, customer_name`;
        
        const result = await pool.query(query, params);
        
        res.json({
            data: result.rows,
            totalCustomers: result.rows.length,
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
        // Get Booksonix stats
        const booksonixResult = await pool.query(
            'SELECT COUNT(*) as total FROM booksonix_records'
        );
        
        // Get Gazelle stats
        const gazelleResult = await pool.query(
            'SELECT COUNT(*) as total FROM gazelle_sales'
        );
        
        // Get customer stats from Gazelle
        const customerResult = await pool.query(
            'SELECT COUNT(DISTINCT customer_name) as total FROM gazelle_sales'
        );
        
        // Get title stats from Gazelle
        const titleResult = await pool.query(
            'SELECT COUNT(DISTINCT title) as total FROM gazelle_sales'
        );
        
        res.json({
            total_records: gazelleResult.rows[0].total || 0,
            total_customers: customerResult.rows[0].total || 0,
            total_titles: titleResult.rows[0].total || 0,
            excluded_customers: 0,
            total_booksonix: booksonixResult.rows[0].total || 0,
            total_gazelle: gazelleResult.rows[0].total || 0
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

// Clear all Gazelle sales data
app.delete('/api/clear-data', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM gazelle_sales');
        res.json({ 
            success: true, 
            message: `Successfully cleared ${result.rowCount} sales records`,
            deletedCount: result.rowCount
        });
    } catch (err) {
        console.error('Error clearing sales data:', err);
        res.status(500).json({ error: 'Failed to clear sales data' });
    }
});

// Export data endpoints
app.get('/api/export-all', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT * FROM gazelle_sales 
            ORDER BY order_date DESC, customer_name
        `);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ message: 'No data to export' });
        }
        
        // Create CSV content
        const headers = ['Order Ref', 'Order Date', 'Customer Name', 'City', 'Country', 'Title', 'ISBN-13', 'Quantity', 'Unit Price', 'Discount %', 'Publisher', 'Format'];
        let csvContent = headers.join(',') + '\n';
        
        result.rows.forEach(row => {
            const rowData = [
                row.order_ref || '',
                row.order_date ? new Date(row.order_date).toLocaleDateString('en-GB') : '',
                `"${(row.customer_name || '').replace(/"/g, '""')}"`,
                `"${(row.city || '').replace(/"/g, '""')}"`,
                row.country || '',
                `"${(row.title || '').replace(/"/g, '""')}"`,
                row.isbn13 || '',
                row.quantity || 0,
                row.unit_price || 0,
                row.discount || 0,
                `"${(row.publisher || '').replace(/"/g, '""')}"`,
                row.format || ''
            ];
            csvContent += rowData.join(',') + '\n';
        });
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="gazelle_export_${new Date().toISOString().split('T')[0]}.csv"`);
        res.send(csvContent);
        
    } catch (err) {
        console.error('Export error:', err);
        res.status(500).json({ error: 'Export failed' });
    }
});

app.get('/api/backup', async (req, res) => {
    try {
        // This is a placeholder - in production, you'd implement proper database backup
        res.json({ success: true, message: 'Backup functionality would be implemented here' });
    } catch (err) {
        res.status(500).json({ error: 'Backup failed' });
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

// TEMPORARY DATABASE FIX ROUTE - REMOVE AFTER USING
app.get('/api/fix-gazelle-table', async (req, res) => {
    try {
        console.log('Fixing Gazelle table structure...');
        
        // Drop the constraint if it exists
        await pool.query(`
            ALTER TABLE gazelle_sales 
            DROP CONSTRAINT IF EXISTS gazelle_sales_order_ref_isbn13_customer_name_key
        `);
        
        console.log('Constraint removed successfully');
        
        // Get current table structure
        const structure = await pool.query(`
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'gazelle_sales'
            ORDER BY ordinal_position
        `);
        
        res.json({
            success: true,
            message: 'Gazelle table fixed successfully',
            columns: structure.rows
        });
    } catch (err) {
        console.error('Error fixing Gazelle table:', err);
        res.status(500).json({ error: err.message });
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
