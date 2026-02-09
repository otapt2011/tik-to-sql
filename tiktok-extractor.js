// tiktok-extractor.js - Fixed to create ALL tables from schema
var TikTokExtractor = (function() {
    'use strict';
    
    // Reference to the mapper
    var mapper = TikTokMapper;
    
    // Configuration
    var config = {
        chunkSize: 1000,
        delayBetweenChunks: 0,
        useWorkers: false,
        batchSize: 100,
        memoryLimit: 100 * 1024 * 1024,
        dateFormat: 'YYYY-MM-DD HH:MM:SS',
        validateDates: true,
        generateTriggers: true
    };
    
    // State
    var extractedData = {
        user: null,
        tables: {},
        statistics: {},
        progress: {
            total: 0,
            processed: 0,
            percentage: 0,
            currentTable: '',
            currentPath: ''
        },
        warnings: [],
        errors: []
    };
    
    var sqlStatements = [];
    var isProcessing = false;
    var abortController = null;
    var progressCallbacks = [];
    var currentUserId = null; // Will be generated from username
    var currentUsername = null; // Extracted TikTok username
    var usernameToIdMap = new Map(); // Track username -> user_id mappings
    var startTime = null;
    
    // Reset function
    function reset() {
        extractedData = {
            user: null,
            tables: {},
            statistics: {},
            progress: {
                total: 0,
                processed: 0,
                percentage: 0,
                currentTable: '',
                currentPath: ''
            },
            warnings: [],
            errors: []
        };
        sqlStatements = [];
        isProcessing = false;
        currentUserId = null;
        currentUsername = null;
        usernameToIdMap.clear();
        startTime = null;
        
        if (abortController) {
            abortController.abort();
            abortController = null;
        }
        
        notifyProgress(0, 0, 0, '', '');
    }
    
    // Register progress callback
    function onProgress(callback) {
        if (typeof callback === 'function') {
            progressCallbacks.push(callback);
        }
    }
    
    // Notify progress
    function notifyProgress(processed, total, percentage, table, path) {
        extractedData.progress = {
            processed: processed,
            total: total,
            percentage: Math.min(100, Math.max(0, percentage)),
            currentTable: table || '',
            currentPath: path || ''
        };
        
        progressCallbacks.forEach(callback => {
            try {
                callback(extractedData.progress);
            } catch (error) {
                console.error('Progress callback error:', error);
            }
        });
    }
    
    // Generate consistent user_id from username
    function generateUserIdFromUsername(username) {
        if (!username) {
            // Generate random ID for users without username
            return Math.floor(Math.random() * 900000) + 100000;
        }
        
        // Create consistent hash from username (simple DJB2-like hash)
        let hash = 5381;
        for (let i = 0; i < username.length; i++) {
            hash = (hash * 33) ^ username.charCodeAt(i);
        }
        
        // Return positive hash within reasonable range
        return Math.abs(hash) % 1000000 + 1; // +1 to avoid 0
    }
    
    // Main extraction function
    function extractData(jsonData, options) {
        reset();
        
        if (!jsonData || typeof jsonData !== 'object') {
            throw new Error('Invalid JSON data provided');
        }
        
        if (options) {
            Object.assign(config, options);
        }
        
        isProcessing = true;
        startTime = Date.now();
        abortController = new AbortController();
        
        try {
            // Step 1: Extract user information with proper user_id
            extractUserData(jsonData);
            
            // Step 2: Calculate total items for progress tracking
            const totalItems = mapper.estimateTotalItems(jsonData);
            notifyProgress(0, totalItems, 0, 'Starting', '');
            
            // Step 3: Extract all data with date handling
            extractAllMappingsData(jsonData, totalItems);
            
            // Step 4: Validate dates if enabled
            if (config.validateDates) {
                validateExtractedDates();
            }
            
            // Step 5: Generate SQL with proper user_id and triggers
            generateSQLStatements();
            
            isProcessing = false;
            
            return {
                data: extractedData,
                sql: sqlStatements,
                stats: extractedData.statistics,
                progress: extractedData.progress,
                warnings: extractedData.warnings,
                errors: extractedData.errors,
                dateStats: generateDateStatistics()
            };
        } catch (error) {
            isProcessing = false;
            if (error.name !== 'AbortError') {
                extractedData.errors.push({
                    type: 'extraction',
                    message: error.message,
                    timestamp: new Date().toISOString()
                });
                throw error;
            }
            throw new Error('Extraction was aborted');
        }
    }
    
    // Extract user data with proper user_id generation
    function extractUserData(jsonData) {
        // First, extract the username from JSON
        currentUsername = mapper.extractUsername(jsonData);
        
        // Generate or retrieve user_id
        if (currentUsername) {
            // Check if we already have this user in our map
            if (usernameToIdMap.has(currentUsername)) {
                currentUserId = usernameToIdMap.get(currentUsername);
            } else {
                // New user - generate ID from username
                currentUserId = generateUserIdFromUsername(currentUsername);
                
                // Store in map for consistency
                usernameToIdMap.set(currentUsername, currentUserId);
            }
        } else {
            // No username found - generate random ID
            currentUserId = generateUserIdFromUsername(null);
            currentUsername = 'unknown_user_' + currentUserId;
        }
        
        const profilePath = 'Profile And Settings.Profile Info.ProfileMap';
        const mapping = mapper.getMapping(profilePath);
        
        if (mapping) {
            const extracted = mapper.extractDataByMapping(jsonData, mapping, profilePath);
            if (extracted.length > 0) {
                const userRow = extracted[0];
                
                // Ensure username is set
                if (!userRow.username && currentUsername) {
                    userRow.username = currentUsername;
                } else if (!userRow.username && userRow.display_name) {
                    // Fallback to display_name if no username found
                    userRow.username = userRow.display_name
                        .toLowerCase()
                        .replace(/\s+/g, '_')
                        .replace(/[^a-z0-9_]/g, '')
                        .substring(0, 50);
                } else if (!userRow.username) {
                    userRow.username = 'tiktok_user_' + currentUserId;
                }
                
                // Update currentUsername with the final value
                currentUsername = userRow.username;
                
                // Add user_id to user row
                userRow.user_id = currentUserId;
                
                extractedData.user = userRow;
                extractedData.tables['users'] = [userRow];
                extractedData.statistics['users'] = 1;
                return;
            }
        }
        
        // Create default user if extraction fails
        currentUsername = currentUsername || 'tiktok_user_' + currentUserId;
        extractedData.user = {
            user_id: currentUserId,
            username: currentUsername,
            display_name: 'Unknown User',
            email: '',
            bio_description: '',
            birth_date: '',
            account_region: '',
            follower_count: 0,
            following_count: 0,
            is_deleted: 0,
            created_at: new Date().toISOString().replace('T', ' ').split('.')[0],
            updated_at: new Date().toISOString().replace('T', ' ').split('.')[0]
        };
        extractedData.tables['users'] = [extractedData.user];
        extractedData.statistics['users'] = 1;
    }
    
    // Extract data from all mappings
    function extractAllMappingsData(jsonData, totalItems) {
        const mappings = mapper.getAllMappings();
        let processedItems = 0;
        let currentChunk = 0;
        
        // Process each mapping
        for (const path in mappings) {
            if (abortController.signal.aborted) {
                throw new DOMException('Aborted', 'AbortError');
            }
            
            const mapping = mappings[path];
            if (!mapping || path === 'Profile And Settings.Profile Info.ProfileMap') {
                continue; // Skip user profile (already processed)
            }
            
            const tableName = mapping.table;
            
            // Update progress
            notifyProgress(processedItems, totalItems, 
                totalItems > 0 ? (processedItems / totalItems * 100) : 0,
                tableName, path);
            
            try {
                const extracted = mapper.extractDataByMapping(jsonData, mapping, path, {
                    chunkSize: config.chunkSize
                });
                
                if (extracted.length > 0) {
                    // Initialize table if not exists
                    if (!extractedData.tables[tableName]) {
                        extractedData.tables[tableName] = [];
                    }
                    
                    // Add user_id to each row
                    const rowsWithUserId = extracted.map(row => {
                        return Object.assign({}, row, { user_id: currentUserId });
                    });
                    
                    // Add data to table with user_id
                    extractedData.tables[tableName].push(...rowsWithUserId);
                    
                    // Update statistics
                    if (!extractedData.statistics[tableName]) {
                        extractedData.statistics[tableName] = 0;
                    }
                    extractedData.statistics[tableName] += extracted.length;
                    
                    // Update processed items
                    processedItems += extracted.length;
                    
                    // Check memory usage and yield to prevent blocking
                    currentChunk++;
                    if (currentChunk % 10 === 0) {
                        setTimeout(() => {}, 0); // Yield to event loop
                    }
                }
            } catch (error) {
                const warning = {
                    type: 'extraction_warning',
                    path: path,
                    table: tableName,
                    message: error.message,
                    timestamp: new Date().toISOString()
                };
                extractedData.warnings.push(warning);
                console.warn(`Error extracting ${path}:`, error);
                // Continue with other paths
            }
        }
        
        // Final progress update
        notifyProgress(processedItems, totalItems, 100, 'Complete', '');
    }
    
    // Validate dates in extracted data
    function validateExtractedDates() {
        const mappings = mapper.getAllMappings();
        
        for (const tableName in extractedData.tables) {
            if (tableName === 'users') continue;
            
            const rows = extractedData.tables[tableName];
            if (rows && rows.length > 0) {
                // Find mapping for this table
                let tableMapping = null;
                for (const path in mappings) {
                    if (mappings[path].table === tableName) {
                        tableMapping = mappings[path];
                        break;
                    }
                }
                
                if (tableMapping && tableMapping.dateFields) {
                    const dateFields = tableMapping.dateFields;
                    
                    // Check first 100 rows for date validation
                    const sampleSize = Math.min(100, rows.length);
                    for (let i = 0; i < sampleSize; i++) {
                        const row = rows[i];
                        for (const dateField of dateFields) {
                            if (row[dateField] !== undefined && row[dateField] !== null && row[dateField] !== '') {
                                const value = row[dateField];
                                
                                // Check if it looks like a valid date
                                if (typeof value === 'string') {
                                    // Check for SQLite datetime format: YYYY-MM-DD HH:MM:SS
                                    if (!value.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/) && 
                                        !value.match(/^\d{4}-\d{2}-\d{2}$/)) {
                                        
                                        const warning = {
                                            type: 'date_validation',
                                            table: tableName,
                                            field: dateField,
                                            row: i,
                                            value: value,
                                            expected: 'YYYY-MM-DD HH:MM:SS or YYYY-MM-DD',
                                            timestamp: new Date().toISOString()
                                        };
                                        extractedData.warnings.push(warning);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Generate date statistics
    function generateDateStatistics() {
        const stats = {
            byTable: {},
            overall: {
                totalDateFields: 0,
                totalDateValues: 0,
                dateRange: null
            }
        };
        
        const mappings = mapper.getAllMappings();
        let allDates = [];
        
        for (const tableName in extractedData.tables) {
            const rows = extractedData.tables[tableName];
            if (rows && rows.length > 0) {
                // Find mapping for this table
                let tableMapping = null;
                for (const path in mappings) {
                    if (mappings[path].table === tableName) {
                        tableMapping = mappings[path];
                        break;
                    }
                }
                
                if (tableMapping && tableMapping.dateFields) {
                    const dateFields = tableMapping.dateFields;
                    stats.byTable[tableName] = {};
                    stats.overall.totalDateFields += dateFields.length;
                    
                    for (const dateField of dateFields) {
                        const dates = [];
                        for (const row of rows) {
                            if (row[dateField] && row[dateField] !== '') {
                                dates.push(row[dateField]);
                                allDates.push(row[dateField]);
                                stats.overall.totalDateValues++;
                            }
                        }
                        
                        if (dates.length > 0) {
                            // Find min and max dates
                            let minDate = null;
                            let maxDate = null;
                            
                            for (const dateStr of dates) {
                                try {
                                    // Parse date string to Date object for comparison
                                    let date;
                                    if (dateStr.includes(' ')) {
                                        date = new Date(dateStr.replace(' ', 'T') + 'Z');
                                    } else {
                                        date = new Date(dateStr + 'T00:00:00Z');
                                    }
                                    
                                    if (!isNaN(date.getTime())) {
                                        if (!minDate || date < minDate) minDate = date;
                                        if (!maxDate || date > maxDate) maxDate = date;
                                    }
                                } catch (e) {
                                    // Invalid date format
                                }
                            }
                            
                            stats.byTable[tableName][dateField] = {
                                count: dates.length,
                                nonNull: dates.length,
                                nullCount: rows.length - dates.length,
                                minDate: minDate ? minDate.toISOString().split('T')[0] : null,
                                maxDate: maxDate ? maxDate.toISOString().split('T')[0] : null
                            };
                        }
                    }
                }
            }
        }
        
        // Calculate overall date range
        if (allDates.length > 0) {
            let overallMin = null;
            let overallMax = null;
            
            for (const dateStr of allDates) {
                try {
                    let date;
                    if (dateStr.includes(' ')) {
                        date = new Date(dateStr.replace(' ', 'T') + 'Z');
                    } else {
                        date = new Date(dateStr + 'T00:00:00Z');
                    }
                    
                    if (!isNaN(date.getTime())) {
                        if (!overallMin || date < overallMin) overallMin = date;
                        if (!overallMax || date > overallMax) overallMax = date;
                    }
                } catch (e) {
                    // Invalid date format
                }
            }
            
            if (overallMin && overallMax) {
                stats.overall.dateRange = {
                    min: overallMin.toISOString().split('T')[0],
                    max: overallMax.toISOString().split('T')[0],
                    days: Math.round((overallMax - overallMin) / (1000 * 60 * 60 * 24))
                };
            }
        }
        
        return stats;
    }
    
    // Helper function to get primary key for a table
    function getPrimaryKeyForTable(tableName) {
        const primaryKeys = {
            'users': 'user_id',
            'posts': 'post_id',
            'comments': 'comment_id',
            'direct_messages': 'message_id',
            'group_chats': 'message_id',
            'coin_purchases': 'purchase_id',
            'favorite_collections': 'collection_id',
            'favorite_comments': 'favorite_comment_id',
            'favorite_effects': 'effect_id',
            'favorite_hashtags': 'hashtag_id',
            'favorite_sounds': 'sound_id',
            'favorite_videos': 'video_id',
            'liked_videos': 'like_id',
            'deleted_posts': 'post_id',
            'blocked_users': 'block_id',
            'followers': 'follower_id',
            'following': 'following_id',
            'live_sessions': 'live_id',
            'watched_lives': 'watch_id',
            'live_comments': 'comment_id',
            'login_history': 'login_id',
            'user_hashtags': 'hashtag_id',
            'searches': 'search_id',
            'reposts': 'repost_id',
            'share_history': 'share_id',
            'sent_gifts': 'gift_id',
            'purchased_gifts': 'purchase_id',
            'product_browsing': 'browse_id'
        };
        
        return primaryKeys[tableName] || `${tableName.slice(0, -1)}_id`;
    }
    
    // Get all table names from the mapper
    function getAllTableNames() {
        const tableNames = new Set(['users']); // Always include users table
        
        const mappings = mapper.getAllMappings();
        for (const path in mappings) {
            const mapping = mappings[path];
            if (mapping && mapping.table) {
                tableNames.add(mapping.table);
            }
        }
        
        // Add validation tables if triggers are enabled
        if (config.generateTriggers) {
            tableNames.add('date_validation_log');
            tableNames.add('data_validation_log');
        }
        
        return Array.from(tableNames).sort();
    }
    
   // Generate validation triggers
function generateValidationTriggers() {
    const triggerStatements = [];
    
    // Only generate triggers if enabled in config
    if (!config.generateTriggers) {
        return triggerStatements;
    }
    
    // Date validation log table
    triggerStatements.push(`
CREATE TABLE IF NOT EXISTS date_validation_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    invalid_value TEXT,
    row_id INTEGER,
    validation_type TEXT DEFAULT 'format_validation',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);`);
    
    // Data validation log table
    triggerStatements.push(`
CREATE TABLE IF NOT EXISTS data_validation_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    issue_type TEXT,
    invalid_value TEXT,
    validation_type TEXT DEFAULT 'data_validation',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);`);
    
    // Define tables with date fields
    const tablesWithDates = {
        'users': ['birth_date', 'created_at', 'updated_at'],
        'posts': ['post_date'],
        'comments': ['comment_date'],
        'direct_messages': ['message_date'],
        'group_chats': ['message_date'],
        'liked_videos': ['like_date'],
        'login_history': ['login_date'],
        'searches': ['search_date'],
        'coin_purchases': ['purchase_date'],
        'favorite_collections': ['favorite_date'],
        'favorite_videos': ['favorite_date'],
        'favorite_effects': ['effect_date'],
        'favorite_hashtags': ['favorite_date'],
        'favorite_sounds': ['favorite_date'],
        'followers': ['follow_date'],
        'following': ['follow_date'],
        'blocked_users': ['block_date'],
        'deleted_posts': ['post_date', 'delete_date'],
        'live_sessions': ['live_start_time', 'live_end_time'],
        'watched_lives': ['watch_time'],
        'live_comments': ['comment_time'],
        'reposts': ['repost_date'],
        'share_history': ['share_date'],
        'sent_gifts': ['send_date'],
        'purchased_gifts': ['purchase_date'],
        'product_browsing': ['browsing_date']
    };
    
    // Generate triggers for all tables in schema
    for (const [tableName, dateColumns] of Object.entries(tablesWithDates)) {
        const primaryKey = getPrimaryKeyForTable(tableName);
        
        for (const dateColumn of dateColumns) {
            triggerStatements.push(`
CREATE TRIGGER IF NOT EXISTS validate_${tableName}_${dateColumn}
BEFORE INSERT ON ${tableName}
FOR EACH ROW
WHEN (
    NEW.${dateColumn} IS NOT NULL AND
    NEW.${dateColumn} != '' AND
    NEW.${dateColumn} NOT GLOB '????-??-?? ??:??:??' AND
    NEW.${dateColumn} NOT GLOB '????-??-??T??:??:??*' AND
    NEW.${dateColumn} NOT GLOB '????-??-??'
)
BEGIN
    INSERT INTO date_validation_log (table_name, user_id, column_name, invalid_value, row_id, validation_type)
    VALUES ('${tableName}', NEW.user_id, '${dateColumn}', NEW.${dateColumn}, NEW.${primaryKey}, 'format_validation');
END;`);
        }
    }
    
    // User data validation trigger - FIXED: Using SELECT...WHERE pattern instead of IF
    triggerStatements.push(`
CREATE TRIGGER IF NOT EXISTS validate_user_data
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    -- Validate username length (3-50 characters) using SELECT...WHERE pattern
    INSERT INTO data_validation_log (table_name, user_id, column_name, issue_type, invalid_value, validation_type)
    SELECT 'users', NEW.user_id, 'username', 'length_validation', NEW.username, 'data_validation'
    WHERE LENGTH(NEW.username) < 3 OR LENGTH(NEW.username) > 50;
    
    -- Validate email format (must contain @ and .) if present
    INSERT INTO data_validation_log (table_name, user_id, column_name, issue_type, invalid_value, validation_type)
    SELECT 'users', NEW.user_id, 'email', 'format_validation', NEW.email, 'data_validation'
    WHERE NEW.email IS NOT NULL AND NEW.email != '' AND NEW.email NOT LIKE '%_@_%._%';
    
    -- Validate birth_date format if present
    INSERT INTO date_validation_log (table_name, user_id, column_name, invalid_value, row_id, validation_type)
    SELECT 'users', NEW.user_id, 'birth_date', NEW.birth_date, NEW.user_id, 'format_validation'
    WHERE NEW.birth_date IS NOT NULL 
      AND NEW.birth_date != '' 
      AND NEW.birth_date NOT GLOB '????-??-??';
END;`);
    
    // Prevent duplicate username - FIXED: Added FOR EACH ROW
    triggerStatements.push(`
CREATE TRIGGER IF NOT EXISTS prevent_duplicate_username
BEFORE INSERT ON users
FOR EACH ROW
WHEN EXISTS (SELECT 1 FROM users WHERE username = NEW.username)
BEGIN
    INSERT INTO data_validation_log (table_name, user_id, column_name, issue_type, invalid_value, validation_type)
    VALUES ('users', NEW.user_id, 'username', 'duplicate_username', NEW.username, 'data_validation');
    -- SELECT RAISE(ABORT, 'Duplicate username not allowed'); -- Uncomment to block duplicates
END;`);
    
    return triggerStatements;
}
    
    // Generate views for reporting
    function generateReportViews() {
        const viewStatements = [];
        
        viewStatements.push(`
-- User activity summary
CREATE VIEW IF NOT EXISTS vw_user_activity_summary AS
SELECT 
    u.user_id,
    u.username,
    u.display_name,
    u.follower_count,
    u.following_count,
    (SELECT COUNT(*) FROM posts p WHERE p.user_id = u.user_id) as total_posts,
    (SELECT COUNT(*) FROM comments c WHERE c.user_id = u.user_id) as total_comments,
    (SELECT COUNT(*) FROM liked_videos l WHERE l.user_id = u.user_id) as total_likes,
    (SELECT COUNT(*) FROM followers f WHERE f.user_id = u.user_id) as total_followers,
    (SELECT COUNT(*) FROM following f WHERE f.user_id = u.user_id) as total_following,
    (SELECT MAX(login_date) FROM login_history l WHERE l.user_id = u.user_id) as last_login,
    (SELECT COUNT(*) FROM searches s WHERE s.user_id = u.user_id) as total_searches
FROM users u;`);

        viewStatements.push(`
-- Date validation report
CREATE VIEW IF NOT EXISTS vw_date_validation_report AS
SELECT 
    dvl.table_name,
    dvl.user_id,
    u.username,
    dvl.column_name,
    COUNT(*) as invalid_count,
    GROUP_CONCAT(DISTINCT SUBSTR(dvl.invalid_value, 1, 50)) as sample_values,
    MIN(dvl.created_at) as first_detected,
    MAX(dvl.created_at) as last_detected
FROM date_validation_log dvl
JOIN users u ON dvl.user_id = u.user_id
GROUP BY dvl.table_name, dvl.user_id, dvl.column_name
ORDER BY invalid_count DESC, dvl.user_id;`);

        viewStatements.push(`
-- User data quality summary
CREATE VIEW IF NOT EXISTS vw_user_data_quality AS
SELECT 
    u.user_id,
    u.username,
    COALESCE(d.date_issues, 0) as date_validation_issues,
    COALESCE(v.data_issues, 0) as data_validation_issues,
    COALESCE(d.date_issues, 0) + COALESCE(v.data_issues, 0) as total_issues
FROM users u
LEFT JOIN (
    SELECT user_id, COUNT(*) as date_issues
    FROM date_validation_log
    GROUP BY user_id
) d ON u.user_id = d.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) as data_issues
    FROM data_validation_log
    GROUP BY user_id
) v ON u.user_id = v.user_id
ORDER BY total_issues DESC;`);

        return viewStatements;
    }
    
    // Add CREATE TABLE statements for ALL tables
    function addCreateTableStatements() {
        const createStatements = [];
        
        // Get all table names from mapper
        const allTableNames = getAllTableNames();
        
        // Create users table
        createStatements.push(`
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT,
    email TEXT,
    bio_description TEXT,
    birth_date TEXT,
    account_region TEXT,
    follower_count INTEGER DEFAULT 0,
    following_count INTEGER DEFAULT 0,
    is_deleted INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`);
        
        // Get column definitions from sample data where available
        const mappings = mapper.getAllMappings();
        
        // Create tables for all known tables
        for (const tableName of allTableNames) {
            if (tableName === 'users' || tableName === 'date_validation_log' || tableName === 'data_validation_log') {
                continue; // Already created or will be created by triggers
            }
            
            let columns = [];
            columns.push('user_id INTEGER NOT NULL');
            
            // Add appropriate primary key
            const primaryKey = getPrimaryKeyForTable(tableName);
            columns.push(`${primaryKey} INTEGER PRIMARY KEY AUTOINCREMENT`);
            
            // Try to get column definitions from sample data
            if (extractedData.tables[tableName] && extractedData.tables[tableName].length > 0) {
                const sampleRow = extractedData.tables[tableName][0];
                let mappingConfig = {};
                
                // Find mapping for this table to get column types
                for (const path in mappings) {
                    if (mappings[path].table === tableName) {
                        mappingConfig = mappings[path];
                        break;
                    }
                }
                
                for (const key in sampleRow) {
                    if (key !== '_index' && key !== 'user_id' && key !== '_batch' && key !== 'username' && key !== primaryKey) {
                        const value = sampleRow[key];
                        const columnType = mapper.getColumnType(key, value, mappingConfig);
                        columns.push(`${key} ${columnType}`);
                    }
                }
            } else {
                // No data for this table, use default schema from mappings
                for (const path in mappings) {
                    if (mappings[path].table === tableName) {
                        const mapping = mappings[path];
                        if (mapping.columns) {
                            for (const sourceKey in mapping.columns) {
                                const targetColumn = mapping.columns[sourceKey];
                                // Skip columns that are already added
                                if (targetColumn !== 'user_id' && targetColumn !== primaryKey) {
                                    // Determine column type based on field name
                                    let columnType = 'TEXT'; // default
                                    
                                    if (mapping.dateFields && mapping.dateFields.includes(targetColumn)) {
                                        columnType = 'TIMESTAMP';
                                    } else if (mapping.numericFields && mapping.numericFields.includes(targetColumn)) {
                                        columnType = 'INTEGER';
                                    } else if (mapping.integerFields && mapping.integerFields.includes(targetColumn)) {
                                        columnType = 'INTEGER';
                                    } else if (mapper.isNumericField(targetColumn)) {
                                        columnType = 'INTEGER';
                                    }
                                    
                                    columns.push(`${targetColumn} ${columnType}`);
                                }
                            }
                        }
                        break;
                    }
                }
            }
            
            // Add CREATE TABLE statement
            createStatements.push(`
CREATE TABLE IF NOT EXISTS ${tableName} (
    ${columns.join(',\n    ')},
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);`);
        }
        
        // Add indexes for efficient queries
        createStatements.push(`
-- User indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

-- Date-based indexes for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_comments_user_date ON comments(user_id, comment_date);
CREATE INDEX IF NOT EXISTS idx_posts_user_date ON posts(user_id, post_date);
CREATE INDEX IF NOT EXISTS idx_direct_messages_user_date ON direct_messages(user_id, message_date);
CREATE INDEX IF NOT EXISTS idx_group_chats_user_date ON group_chats(user_id, message_date);
CREATE INDEX IF NOT EXISTS idx_liked_videos_user_date ON liked_videos(user_id, like_date);
CREATE INDEX IF NOT EXISTS idx_followers_user_date ON followers(user_id, follow_date);
CREATE INDEX IF NOT EXISTS idx_following_user_date ON following(user_id, follow_date);
CREATE INDEX IF NOT EXISTS idx_login_history_user_date ON login_history(user_id, login_date);
CREATE INDEX IF NOT EXISTS idx_searches_user_date ON searches(user_id, search_date);
CREATE INDEX IF NOT EXISTS idx_coin_purchases_user_date ON coin_purchases(user_id, purchase_date);

-- General date indexes
CREATE INDEX IF NOT EXISTS idx_comments_date ON comments(comment_date);
CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(post_date);
CREATE INDEX IF NOT EXISTS idx_liked_videos_date ON liked_videos(like_date);
CREATE INDEX IF NOT EXISTS idx_login_history_date ON login_history(login_date);
CREATE INDEX IF NOT EXISTS idx_searches_date ON searches(search_date);`);
        
        // Add validation triggers
        if (config.generateTriggers) {
            const triggerStatements = generateValidationTriggers();
            createStatements.push(...triggerStatements);
        }
        
        // Add report views
        const viewStatements = generateReportViews();
        createStatements.push(...viewStatements);
        
        sqlStatements = createStatements;
        
        // Update statistics to include ALL tables
        updateTableStatistics();
    }
    
    // Update table statistics to include ALL tables, not just those with data
    function updateTableStatistics() {
        const allTableNames = getAllTableNames();
        
        // Initialize statistics for all tables
        for (const tableName of allTableNames) {
            if (!extractedData.statistics[tableName]) {
                extractedData.statistics[tableName] = 0;
            }
        }
    }
    
    // Generate SQL INSERT statements with proper user_id
    function generateSQLStatements() {
        sqlStatements = [];
        
        // Add CREATE TABLE statements for ALL tables
        addCreateTableStatements();
        
        // Add user insertion
        if (extractedData.user) {
            const userSql = generateInsertSQL('users', extractedData.user);
            if (userSql) sqlStatements.push(userSql);
        }
        
        // Generate INSERT statements for all tables with data
        let statementCount = 0;
        
        for (const tableName in extractedData.tables) {
            if (tableName === 'users') continue;
            
            const rows = extractedData.tables[tableName];
            if (rows && rows.length > 0) {
                // Process in batches to prevent memory issues
                const batchSize = config.batchSize;
                const batches = Math.ceil(rows.length / batchSize);
                
                for (let batchIndex = 0; batchIndex < batches; batchIndex++) {
                    const start = batchIndex * batchSize;
                    const end = Math.min(start + batchSize, rows.length);
                    const batch = rows.slice(start, end);
                    
                    // Generate SQL for this batch
                    for (let i = 0; i < batch.length; i++) {
                        const row = batch[i];
                        const sql = generateInsertSQL(tableName, row);
                        if (sql) {
                            sqlStatements.push(sql);
                            statementCount++;
                            
                            // Yield periodically for large datasets
                            if (statementCount % 1000 === 0) {
                                setTimeout(() => {}, 0);
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Generate a single INSERT SQL statement
    function generateInsertSQL(tableName, row) {
        const columns = [];
        const values = [];
        
        // Get date and numeric fields for this table
        let dateFields = [];
        let numericFields = [];
        const mappings = mapper.getAllMappings();
        for (const path in mappings) {
            if (mappings[path].table === tableName) {
                if (mappings[path].dateFields) {
                    dateFields = mappings[path].dateFields;
                }
                if (mappings[path].numericFields) {
                    numericFields = mappings[path].numericFields;
                }
                break;
            }
        }
        
        // Add all columns from the row
        for (const column in row) {
            if (row.hasOwnProperty(column) && 
                column !== '_index' && 
                column !== '_batch') {
                
                columns.push(column);
                values.push(formatSQLValue(column, row[column], dateFields, numericFields));
            }
        }
        
        if (columns.length === 0) return null;
        
        return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`;
    }
    
    // Format SQL value properly
    function formatSQLValue(columnName, value, dateFields, numericFields) {
        if (value === null || value === undefined || value === '') {
            return 'NULL';
        } else if (typeof value === 'number') {
            return value;
        } else if (typeof value === 'boolean') {
            return value ? 1 : 0;
        } else if (dateFields.includes(columnName)) {
            // Handle date fields
            if (value === 'N/A' || value === 'null' || value === 'NULL') {
                return 'NULL';
            } else {
                // Ensure proper SQLite datetime format
                let dateValue = value;
                if (typeof value === 'string') {
                    // If it's already in SQLite format, use it as-is
                    if (!value.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/) && 
                        !value.match(/^\d{4}-\d{2}-\d{2}$/)) {
                        // Try to parse it
                        dateValue = mapper.parseDate(value) || value;
                    }
                }
                return `'${dateValue.toString().replace(/'/g, "''")}'`;
            }
        } else if (numericFields.includes(columnName) || mapper.isNumericField(columnName)) {
            // Handle numeric fields
            if (value === 'N/A' || value === 'null' || value === 'NULL') {
                return 'NULL';
            } else {
                // Convert to number
                const num = parseFloat(value);
                if (!isNaN(num)) {
                    return num;
                } else {
                    return 'NULL';
                }
            }
        } else if (typeof value === 'object') {
            try {
                return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
            } catch {
                return "''";
            }
        } else {
            const escapedValue = value.toString().replace(/'/g, "''");
            return `'${escapedValue}'`;
        }
    }
    
    // Extract data from file (async)
    function extractFromFile(file, options) {
        return new Promise(function(resolve, reject) {
            if (!file) {
                reject(new Error('No file provided'));
                return;
            }
            
            if (!file.name.toLowerCase().endsWith('.json')) {
                reject(new Error('File must be a JSON file (.json extension)'));
                return;
            }
            
            reset();
            isProcessing = true;
            abortController = new AbortController();
            startTime = Date.now();
            
            // Read file
            const reader = new FileReader();
            
            reader.onload = function(event) {
                if (abortController.signal.aborted) {
                    reject(new DOMException('Aborted', 'AbortError'));
                    return;
                }
                
                try {
                    const jsonData = JSON.parse(event.target.result);
                    
                    // Update config
                    if (options) {
                        Object.assign(config, options);
                    }
                    
                    // Extract data
                    const result = extractData(jsonData);
                    
                    if (abortController.signal.aborted) {
                        reject(new DOMException('Aborted', 'AbortError'));
                        return;
                    }
                    
                    // Add extraction time
                    result.extractionTime = Date.now() - startTime;
                    result.totalTablesCreated = getAllTableNames().length;
                    
                    resolve(result);
                } catch (error) {
                    if (error instanceof SyntaxError) {
                        reject(new Error('Invalid JSON format: ' + error.message));
                    } else {
                        reject(error);
                    }
                } finally {
                    isProcessing = false;
                }
            };
            
            reader.onerror = function() {
                reject(new Error('Error reading file'));
                isProcessing = false;
            };
            
            // Show progress while reading
            reader.onprogress = function(event) {
                if (event.lengthComputable) {
                    const percentage = Math.round((event.loaded / event.total) * 100);
                    notifyProgress(event.loaded, event.total, percentage, 'Reading file', '');
                }
            };
            
            reader.readAsText(file);
        });
    }
    
    // Abort current extraction
    function abortExtraction() {
        if (abortController) {
            abortController.abort();
            isProcessing = false;
            return true;
        }
        return false;
    }
    
    // Get the extracted data
    function getExtractedData() {
        return extractedData;
    }
    
    // Get generated SQL
    function getSQL() {
        return sqlStatements;
    }
    
    // Get statistics - now includes ALL tables
    function getStatistics() {
        // Make sure all tables are included in statistics
        updateTableStatistics();
        return extractedData.statistics;
    }
    
    // Get total number of tables that will be created
    function getTotalTables() {
        return getAllTableNames().length;
    }
    
    // Get progress
    function getProgress() {
        return extractedData.progress;
    }
    
    // Get warnings
    function getWarnings() {
        return extractedData.warnings;
    }
    
    // Get errors
    function getErrors() {
        return extractedData.errors;
    }
    
    // Get current username
    function getCurrentUsername() {
        return currentUsername;
    }
    
    // Get current user ID
    function getCurrentUserId() {
        return currentUserId;
    }
    
    // Check if processing
    function isExtracting() {
        return isProcessing;
    }
    
    // Download SQL as file
    function downloadSQL(filename = 'tiktok_data.sql') {
        if (sqlStatements.length === 0) {
            throw new Error('No SQL statements to download');
        }
        
        // Add header comment with metadata
        const totalTables = getTotalTables();
        const tablesWithData = Object.keys(extractedData.tables).length;
        
        const header = `-- TikTok Data SQL Export
-- Generated: ${new Date().toISOString()}
-- Username: ${currentUsername || 'Unknown'}
-- User ID: ${currentUserId || 'N/A'}
-- Total Tables Created: ${totalTables}
-- Tables With Data: ${tablesWithData}
-- Empty Tables: ${totalTables - tablesWithData}
-- Total Records: ${Object.values(extractedData.statistics).reduce((a, b) => a + b, 0)}
-- Total SQL Statements: ${sqlStatements.length}
-- Multi-User Support: ENABLED
-- Data Types: Fixed - Counts are now integers, not dates

`;
        
        const sqlContent = header + sqlStatements.join('\n\n');
        const blob = new Blob([sqlContent], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        
        // Clean up
        setTimeout(() => URL.revokeObjectURL(url), 100);
    }
    
    // Download data as JSON
    function downloadJSON(filename = 'tiktok_extracted.json') {
        const data = {
            metadata: {
                generated: new Date().toISOString(),
                username: currentUsername,
                userId: currentUserId,
                extractionTime: startTime ? Date.now() - startTime : null,
                totalTables: getTotalTables(),
                tablesWithData: Object.keys(extractedData.tables).length,
                multiUserSupported: true
            },
            user: extractedData.user,
            statistics: getStatistics(),
            dateStatistics: generateDateStatistics(),
            warnings: extractedData.warnings,
            errors: extractedData.errors,
            table_counts: {},
            sample_data: {}
        };
        
        // Only include sample data for each table
        for (const tableName in extractedData.tables) {
            const rows = extractedData.tables[tableName];
            data.table_counts[tableName] = rows.length;
            
            // Include first 5 rows as sample
            if (rows.length > 0) {
                data.sample_data[tableName] = rows.slice(0, 5);
            }
        }
        
        // Include empty tables in counts
        const allTables = getAllTableNames();
        for (const tableName of allTables) {
            if (!data.table_counts[tableName]) {
                data.table_counts[tableName] = 0;
            }
        }
        
        const jsonContent = JSON.stringify(data, null, 2);
        const blob = new Blob([jsonContent], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        
        setTimeout(() => URL.revokeObjectURL(url), 100);
    }
    
    // Export data in chunks (for very large datasets)
    function exportInChunks(callback) {
        if (typeof callback !== 'function') {
            throw new Error('Callback function required');
        }
        
        const totalChunks = Math.ceil(sqlStatements.length / 1000);
        
        for (let i = 0; i < totalChunks; i++) {
            const start = i * 1000;
            const end = Math.min(start + 1000, sqlStatements.length);
            const chunk = sqlStatements.slice(start, end);
            
            callback(chunk, i, totalChunks);
            
            // Yield to prevent blocking
            if (i % 10 === 0) {
                setTimeout(() => {}, 0);
            }
        }
    }
    
    // Public API
    return {
        extract: extractData,
        extractFromFile: extractFromFile,
        abort: abortExtraction,
        getData: getExtractedData,
        getSQL: getSQL,
        getStats: getStatistics,
        getTotalTables: getTotalTables,
        getProgress: getProgress,
        getWarnings: getWarnings,
        getErrors: getErrors,
        getCurrentUserId: getCurrentUserId,
        getCurrentUsername: getCurrentUsername,
        validateDateFields: validateExtractedDates,
        generateDateStatistics: generateDateStatistics,
        isExtracting: isExtracting,
        downloadSQL: downloadSQL,
        downloadJSON: downloadJSON,
        exportInChunks: exportInChunks,
        onProgress: onProgress,
        reset: reset,
        setConfig: function(newConfig) {
            Object.assign(config, newConfig);
        },
        getConfig: function() {
            return Object.assign({}, config);
        }
    };
})();