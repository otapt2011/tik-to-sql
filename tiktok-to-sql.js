// tiktok-to-sql.js - Compact Mobile-First Version (Fixed)
// DOM Elements - Only get elements that exist
const fileInput = document.getElementById('fileInput');
const uploadArea = document.getElementById('uploadArea');
const browseBtn = document.getElementById('browseBtn');
const extractBtn = document.getElementById('extractBtn');
const abortBtn = document.getElementById('abortBtn');
const resetBtn = document.getElementById('resetBtn');
const downloadSqlBtn = document.getElementById('downloadSqlBtn');
const copySqlBtn = document.getElementById('copySqlBtn');
const downloadJsonBtn = document.getElementById('downloadJsonBtn');
const previewSqlBtn = document.getElementById('previewSqlBtn');
const saveSettingsBtn = document.getElementById('saveSettingsBtn');
const cancelBtn = document.getElementById('cancelBtn');
const sqlViewSelect = document.getElementById('sqlViewSelect');
const dataTableSelect = document.getElementById('dataTableSelect');
const newFileBtn = document.getElementById('newFileBtn'); // Only if exists

// Get other elements safely
const sqlOutput = document.getElementById('sqlOutput');
const dataOutput = document.getElementById('dataOutput');
const loadingOverlay = document.getElementById('loadingOverlay');
const fileInfo = document.getElementById('fileInfo');
const fileName = document.getElementById('fileName');
const fileSize = document.getElementById('fileSize');
const fileStructure = document.getElementById('fileStructure');

// Progress elements
const progressContainer = document.getElementById('progressContainer');
const progressFill = document.getElementById('progressFill');
const progressStatus = document.getElementById('progressStatus');
const progressPercent = document.getElementById('progressPercent');
const progressDetails = document.getElementById('progressDetails');
const progressTable = document.getElementById('progressTable');
const progressSpeed = document.getElementById('progressSpeed');

// Loading overlay elements
const loadingTitle = document.getElementById('loadingTitle');
const loadingMessage = document.getElementById('loadingMessage');
const loadingDetails = document.getElementById('loadingDetails');
const loadingProgressFill = document.getElementById('loadingProgressFill');

// Configuration elements
const chunkSize = document.getElementById('chunkSize');
const batchSize = document.getElementById('batchSize');
const chunkSizeValue = document.getElementById('chunkSizeValue');
const batchSizeValue = document.getElementById('batchSizeValue');

// Status and stats elements
const sqlCount = document.getElementById('sqlCount');
const dataCount = document.getElementById('dataCount');
const statisticsContent = document.getElementById('statisticsContent');
const userInfo = document.getElementById('userInfo');
const userDetails = document.getElementById('userDetails');
const statsGrid = document.getElementById('statsGrid');
const statusText = document.getElementById('statusText');

// Tab elements
const tabs = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');

// Toast elements
const toast = document.getElementById('toast');
const toastMessage = document.getElementById('toastMessage');

// State
let currentFile = null;
let extractedResult = null;
let isProcessing = false;
let startTime = null;
let lastUpdateTime = null;
let processedCount = 0;
let speedHistory = [];

// Initialize
function init() {
    // Check if TikTokExtractor exists
    if (typeof TikTokExtractor === 'undefined') {
        console.error('TikTokExtractor is not loaded');
        showToast('Error: TikTokExtractor not loaded', 'error');
        return;
    }
    
    // Set up progress tracking
    TikTokExtractor.onProgress(handleProgress);
    
    // Load saved settings
    loadSettings();
    
    // Set initial configuration
    updateConfigDisplay();
    
    // Set up event listeners
    setupEventListeners();
    
    // Update status
    updateStatus('Ready to process TikTok JSON files');
    
    // Switch to first tab by default
    switchTab('upload');
}

// Set up event listeners
function setupEventListeners() {
    // File handling
    if (browseBtn) {
        browseBtn.addEventListener('click', () => fileInput.click());
    }
    
    if (fileInput) {
        fileInput.addEventListener('change', handleFileSelect);
    }
    
    if (newFileBtn) {
        newFileBtn.addEventListener('click', resetAll);
    }
    
    // Drag and drop
    if (uploadArea) {
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('dragover');
        });
        
        uploadArea.addEventListener('dragleave', () => {
            uploadArea.classList.remove('dragover');
        });
        
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('dragover');
            
            if (e.dataTransfer.files.length) {
                handleFile(e.dataTransfer.files[0]);
            }
        });
    }
    
    // Buttons - add listeners only if elements exist
    if (extractBtn) extractBtn.addEventListener('click', startExtraction);
    if (abortBtn) abortBtn.addEventListener('click', abortExtraction);
    if (resetBtn) resetBtn.addEventListener('click', resetAll);
    if (downloadSqlBtn) downloadSqlBtn.addEventListener('click', downloadSQL);
    if (copySqlBtn) copySqlBtn.addEventListener('click', copySQL);
    if (downloadJsonBtn) downloadJsonBtn.addEventListener('click', downloadJSON);
    if (previewSqlBtn) previewSqlBtn.addEventListener('click', previewSQL);
    if (saveSettingsBtn) saveSettingsBtn.addEventListener('click', saveSettings);
    if (cancelBtn) cancelBtn.addEventListener('click', abortExtraction);
    
    // Select changes
    if (sqlViewSelect) sqlViewSelect.addEventListener('change', updateSQLView);
    if (dataTableSelect) dataTableSelect.addEventListener('change', updateDataView);
    
    // Configuration sliders
    if (chunkSize) chunkSize.addEventListener('input', updateConfigDisplay);
    if (batchSize) batchSize.addEventListener('input', updateConfigDisplay);
    
    // Tab switching
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.dataset.tab;
            switchTab(tabName);
        });
    });
}

// Switch tabs
function switchTab(tabName) {
    // Update active tab button
    tabs.forEach(btn => {
        btn.classList.remove('active');
    });
    
    const activeTab = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
    if (activeTab) {
        activeTab.classList.add('active');
    }
    
    // Show corresponding tab content
    tabContents.forEach(content => {
        content.classList.remove('active');
    });
    
    const tabContent = document.getElementById(`${tabName}Tab`);
    if (tabContent) {
        tabContent.classList.add('active');
    }
}

// File handling
function handleFileSelect(e) {
    const file = e.target.files[0];
    handleFile(file);
}

async function handleFile(file) {
    if (!file) return;
    
    if (!file.name.toLowerCase().endsWith('.json')) {
        showToast('Please select a JSON file (.json extension required)', 'error');
        return;
    }
    
    currentFile = file;
    
    // Show file info
    if (fileInfo) {
        fileInfo.style.display = 'flex';
    }
    if (fileName) fileName.textContent = file.name;
    if (fileSize) fileSize.textContent = formatFileSize(file.size);
    
    // Validate file structure (quick check)
    try {
        const reader = new FileReader();
        reader.onload = function(e) {
            try {
                const firstPart = e.target.result.substring(0, 1000);
                const trimmed = firstPart.trim();
                
                if (trimmed.startsWith('{')) {
                    if (fileStructure) {
                        fileStructure.textContent = '✓ Valid JSON';
                        fileStructure.style.color = '#10b981';
                    }
                    if (extractBtn) extractBtn.disabled = false;
                    updateStatus(`Ready: ${file.name}`);
                    showToast('File loaded successfully', 'success');
                    
                    // Switch to extract tab
                    setTimeout(() => switchTab('extract'), 300);
                } else {
                    if (fileStructure) {
                        fileStructure.textContent = '⚠ Not a valid JSON object';
                        fileStructure.style.color = '#ef4444';
                    }
                    if (extractBtn) extractBtn.disabled = true;
                    showToast('File must be a JSON object (starting with {)', 'error');
                }
            } catch (error) {
                if (fileStructure) {
                    fileStructure.textContent = '⚠ Cannot validate structure';
                    fileStructure.style.color = '#f59e0b';
                }
                if (extractBtn) extractBtn.disabled = false;
            }
        };
        
        // Only read first 1KB for validation
        const blob = file.slice(0, 1024);
        reader.readAsText(blob);
        
    } catch (error) {
        console.error('Error validating file:', error);
        if (extractBtn) extractBtn.disabled = false;
    }
}

// Start extraction
async function startExtraction() {
    if (!currentFile || isProcessing) return;
    
    isProcessing = true;
    startTime = Date.now();
    lastUpdateTime = startTime;
    processedCount = 0;
    speedHistory = [];
    
    // Reset UI
    if (progressContainer) progressContainer.style.display = 'block';
    if (abortBtn) abortBtn.style.display = 'inline-flex';
    if (extractBtn) extractBtn.disabled = true;
    if (downloadSqlBtn) downloadSqlBtn.disabled = true;
    if (downloadJsonBtn) downloadJsonBtn.disabled = true;
    if (previewSqlBtn) previewSqlBtn.disabled = true;
    if (copySqlBtn) copySqlBtn.disabled = true;
    
    // Show loading overlay for large files
    if (loadingOverlay && currentFile.size > 5 * 1024 * 1024) {
        loadingOverlay.style.display = 'flex';
        if (loadingTitle) loadingTitle.textContent = 'Processing Large File';
        if (loadingMessage) loadingMessage.textContent = `Reading ${formatFileSize(currentFile.size)} JSON file...`;
    }
    
    try {
        // Update config from UI
        const config = {};
        if (chunkSize) config.chunkSize = parseInt(chunkSize.value);
        if (batchSize) config.batchSize = parseInt(batchSize.value);
        
        TikTokExtractor.setConfig(config);
        
        // Start extraction
        updateStatus('Starting extraction...');
        if (loadingDetails) loadingDetails.textContent = 'Parsing JSON structure...';
        
        extractedResult = await TikTokExtractor.extractFromFile(currentFile);
        
        // Hide loading overlay
        if (loadingOverlay) loadingOverlay.style.display = 'none';
        
        // Display results
        displayResults(extractedResult);
        
        // Calculate elapsed time
        const elapsedTime = Date.now() - startTime;
        const timeText = formatTime(elapsedTime);
        
        showToast(`Data extracted in ${timeText}!`, 'success');
        updateStatus(`Extraction complete (${timeText})`);
        
        // Enable download buttons
        if (downloadSqlBtn) downloadSqlBtn.disabled = false;
        if (downloadJsonBtn) downloadJsonBtn.disabled = false;
        if (previewSqlBtn) previewSqlBtn.disabled = false;
        if (copySqlBtn) copySqlBtn.disabled = false;
        
        // Update SQL view
        updateSQLView();
        
        // Switch to results tab
        setTimeout(() => switchTab('results'), 300);
        
    } catch (error) {
        if (loadingOverlay) loadingOverlay.style.display = 'none';
        
        if (error.name === 'AbortError') {
            showToast('Extraction cancelled', 'info');
            updateStatus('Extraction cancelled');
        } else if (error instanceof SyntaxError) {
            showToast('Invalid JSON format', 'error');
            updateStatus('Invalid JSON file');
        } else {
            showToast('Error extracting data: ' + error.message, 'error');
            updateStatus('Extraction failed');
        }
        console.error('Extraction error:', error);
    } finally {
        isProcessing = false;
        if (abortBtn) abortBtn.style.display = 'none';
        if (extractBtn) extractBtn.disabled = false;
        if (progressContainer) progressContainer.style.display = 'none';
    }
}

// Abort extraction
function abortExtraction() {
    if (TikTokExtractor.abort()) {
        showToast('Extraction cancelled', 'info');
        updateStatus('Extraction cancelled');
        if (loadingOverlay) loadingOverlay.style.display = 'none';
        if (abortBtn) abortBtn.style.display = 'none';
        if (extractBtn) extractBtn.disabled = false;
        isProcessing = false;
    }
}

// Handle progress updates
function handleProgress(progress) {
    const now = Date.now();
    
    // Update progress bar
    const percent = progress.percentage;
    if (progressFill) progressFill.style.width = percent + '%';
    if (progressPercent) progressPercent.textContent = Math.round(percent) + '%';
    
    // Update progress text
    let statusText = 'Starting...';
    if (percent < 20) {
        statusText = 'Reading file...';
    } else if (percent < 40) {
        statusText = 'Extracting user data...';
    } else if (percent < 60) {
        statusText = 'Processing posts/comments...';
    } else if (percent < 80) {
        statusText = 'Extracting activity data...';
    } else if (percent < 100) {
        statusText = 'Finishing up...';
    } else {
        statusText = 'Complete!';
    }
    
    if (progressStatus) progressStatus.textContent = statusText;
    
    // Update details
    if (progress.total > 0) {
        const processed = formatNumber(progress.processed);
        const total = formatNumber(progress.total);
        if (progressDetails) progressDetails.textContent = `Processed: ${processed} / ${total}`;
        
        // Update current table
        if (progressTable && progress.currentTable) {
            progressTable.textContent = `Table: ${progress.currentTable}`;
        }
        
        // Calculate speed
        if (lastUpdateTime && progress.processed > processedCount) {
            const timeDiff = now - lastUpdateTime;
            const itemsDiff = progress.processed - processedCount;
            const itemsPerSecond = timeDiff > 0 ? (itemsDiff / (timeDiff / 1000)) : 0;
            
            speedHistory.push(itemsPerSecond);
            if (speedHistory.length > 10) speedHistory.shift();
            
            const avgSpeed = speedHistory.reduce((a, b) => a + b, 0) / speedHistory.length;
            
            if (avgSpeed > 0 && progressSpeed) {
                const remaining = progress.total - progress.processed;
                const remainingTime = remaining / avgSpeed;
                progressSpeed.textContent = `Speed: ${Math.round(avgSpeed)}/s | ETA: ${formatTime(remainingTime * 1000)}`;
            }
        }
        
        lastUpdateTime = now;
        processedCount = progress.processed;
    }
    
    // Update loading overlay
    if (loadingOverlay && loadingOverlay.style.display === 'flex') {
        if (loadingProgressFill) loadingProgressFill.style.width = percent + '%';
        if (loadingDetails) loadingDetails.textContent = `Processing: ${Math.round(percent)}%`;
        
        if (loadingMessage && progress.currentTable) {
            loadingMessage.textContent = `Extracting ${progress.currentTable}...`;
        }
    }
    
    // Update status bar
    updateStatus(`Extracting: ${Math.round(percent)}%`);
}

// Display results
function displayResults(result) {
    // Update data table select
    if (result.data.tables) {
        updateDataTableSelect(result.data.tables);
    }
    
    // Display user info
    if (result.data.user) {
        if (userInfo) userInfo.style.display = 'block';
        if (userDetails) displayUserInfo(result.data.user);
    }
    
    // Display statistics
    if (result.stats) {
        displayStatistics(result.stats);
    }
    
    // Update SQL count
    if (sqlCount && result.sql) {
        sqlCount.textContent = `${formatNumber(result.sql.length)} SQL statements`;
    }
    

}

function displayUserInfos(user) {
    if (!userDetails) return;
    
    const fields = [
        { label: 'Username', value: user.username || 'Not set', icon: '👤' },
        { label: 'Display Name', value: user.display_name || 'Not set', icon: '📛' },
        { label: 'Email', value: user.email || 'Not set', icon: '📧' },
        { label: 'Followers', value: formatNumber(user.follower_count || 0), icon: '👥' },
        { label: 'Following', value: formatNumber(user.following_count || 0), icon: '✅' }
    ];
    
    userDetails.innerHTML = fields.map(field => `
        <div class="user-field">
            <span class="field-label">${field.icon} ${field.label}</span>
            <span class="field-value">${field.value}</span>
        </div>
    `).join('');
}

function displayUserInfo(user) {
    const fields = [
        {
            label: 'Username (from TikTok)',
            value: user.username || 'Not found in JSON',
            icon: '🔑',
            important: true,
            description: 'Extracted from ProfileMap.userName field'
        },
        { label: 'Display Name', value: user.display_name, icon: '📛' },
        { label: 'Email', value: user.email || 'Not provided', icon: '📧' },
        { label: 'Bio', value: user.bio_description || 'No bio', icon: '📝' },
        { label: 'Birth Date', value: user.birth_date || 'Not set', icon: '🎂' },
        { label: 'Region', value: user.account_region || 'Unknown', icon: '🌍' },
        { label: 'Followers', value: formatNumber(user.follower_count || 0), icon: '👥' },
        { label: 'Following', value: formatNumber(user.following_count || 0), icon: '✅' }
    ];
    
    userDetails.innerHTML = fields.map(field => `
        <div class="user-field" style="${field.important ? 'border: 2px solid #667eea; padding: 15px; border-radius: 8px; background: #f0f4ff; margin-bottom: 10px;' : 'padding: 5px 0;'}">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: ${field.important ? '5px' : '2px'}">
                <span style="font-size: ${field.important ? '1.2rem' : '1rem'}">${field.icon}</span>
                <span class="field-label" style="font-weight: ${field.important ? '700' : '600'}; color: ${field.important ? '#667eea' : '#666'}">${field.label}</span>
            </div>
            <div class="field-value" style="font-size: ${field.important ? '1.3rem' : '1rem'}; font-weight: ${field.important ? '700' : '600'}; color: ${field.important ? '#333' : '#444'}; margin-top: ${field.important ? '8px' : '2px'}">
                ${field.value}
            </div>
            ${field.description ? `<div style="font-size: 0.8rem; color: #666; margin-top: 3px;">${field.description}</div>` : ''}
        </div>
    `).join('');
    
    // Update status to show this user's data
    if (user.username) {
        updateStatus(`All extracted data belongs to user: ${user.username}`);
    }
}

function displayStatistics(stats) {
    if (!statsGrid) return;
    
    const totalRecords = Object.values(stats).reduce((sum, count) => sum + count, 0);
    const totalTables = Object.keys(stats).length;
    
    // Update stats grid
    const statCards = [
        { label: 'Total Records', value: formatNumber(totalRecords), color: '#4f46e5', icon: 'fas fa-database' },
        { label: 'Tables Created', value: totalTables, color: '#10b981', icon: 'fas fa-table' },
        { label: 'SQL Statements', value: formatNumber(extractedResult ? extractedResult.sql.length : 0), color: '#ef4444', icon: 'fas fa-code' },
        { label: 'File Size', value: formatFileSize(currentFile ? currentFile.size : 0), color: '#f59e0b', icon: 'fas fa-file' }
    ];
    
    statsGrid.innerHTML = statCards.map(card => `
        <div class="stat-card">
            <div class="stat-value" style="color: ${card.color}">${card.value}</div>
            <div class="stat-label">
                <i class="${card.icon}" style="color: ${card.color}"></i> ${card.label}
            </div>
        </div>
    `).join('');
    
    // Update statistics tab
    if (extractedResult && statisticsContent) {
        updateStatisticsTab();
    }
}

function updateStatisticsTab() {
    if (!statisticsContent || !extractedResult || !extractedResult.stats) return;
    
    const stats = extractedResult.stats;
    let html = '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 8px;">';
    
    for (const [table, count] of Object.entries(stats)) {
        html += `
            <div style="background: #f8fafc; padding: 8px; border-radius: 4px; border-left: 3px solid #4f46e5;">
                <div style="font-weight: 600; color: #333; font-size: 0.8rem;">${table.replace(/_/g, ' ')}</div>
                <div style="font-size: 1rem; font-weight: 700; color: #4f46e5; margin-top: 3px;">${formatNumber(count)}</div>
                <div style="font-size: 0.7rem; color: #666;">records</div>
            </div>
        `;
    }
    
    html += '</div>';
    statisticsContent.innerHTML = html;
}

function updateDataTableSelect(tables) {
    if (!dataTableSelect) return;
    
    dataTableSelect.innerHTML = '<option value="">Select table...</option>';
    
    for (const tableName in tables) {
        const option = document.createElement('option');
        option.value = tableName;
        option.textContent = `${tableName} (${formatNumber(tables[tableName].length)})`;
        dataTableSelect.appendChild(option);
    }
    
    // Select first table by default
    if (dataTableSelect.options.length > 1) {
        dataTableSelect.selectedIndex = 1;
        updateDataView();
    }
}

function updateDataView() {
    if (!dataTableSelect || !dataOutput || !dataCount) return;
    
    const tableName = dataTableSelect.value;
    if (!tableName || !extractedResult || !extractedResult.data.tables[tableName]) {
        dataOutput.innerHTML = '';
        dataCount.textContent = 'No data selected';
        return;
    }
    
    const rows = extractedResult.data.tables[tableName];
    const sample = rows.slice(0, 30); // Show first 30 rows
    
    // Format JSON for display
    const formatted = JSON.stringify(sample, null, 2);
    const highlighted = highlightJSON(formatted);
    
    // Create a wrapper for the highlighted JSON
    dataOutput.innerHTML = `<pre style="margin:0; padding:0;">${highlighted}</pre>`;
    dataCount.textContent = `Showing 30 of ${formatNumber(rows.length)} rows from ${tableName}`;
}

function highlightJSON(json) {
    return json
        .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?)/g, (match) => {
            if (match.endsWith(':')) {
                return `<span class="json-key">${match}</span>`;
            }
            return `<span class="json-string">${match}</span>`;
        })
        .replace(/\b(true|false|null)\b/g, '<span class="json-boolean">$&</span>')
        .replace(/\b-?\d+(\.\d+)?([eE][+-]?\d+)?\b/g, '<span class="json-number">$&</span>');
}

function updateSQLView() {
    if (!sqlOutput || !sqlCount || !extractedResult || !extractedResult.sql) {
        if (sqlOutput) sqlOutput.innerHTML = '';
        return;
    }
    
    const view = sqlViewSelect ? sqlViewSelect.value : 'full';
    let sqlToShow = [];
    
    if (view === 'schema') {
        // Only CREATE TABLE statements
        sqlToShow = extractedResult.sql.filter(line => 
            line.includes('CREATE TABLE') || line.includes('CREATE INDEX') || line.includes('CREATE VIEW')
        );
    } else if (view === 'data') {
        // Only INSERT statements
        sqlToShow = extractedResult.sql.filter(line => line.includes('INSERT INTO'));
    } else {
        // Full output
        sqlToShow = extractedResult.sql;
    }
    
    // Limit preview to 500 lines
    const previewCount = 500;
    const isTruncated = sqlToShow.length > previewCount;
    const displaySQL = isTruncated ? sqlToShow.slice(0, previewCount) : sqlToShow;
    
    // Join and syntax highlight
    let sqlText = displaySQL.join('\n\n');
    if (isTruncated) {
        sqlText += `\n\n-- ... and ${formatNumber(sqlToShow.length - previewCount)} more statements --`;
    }
    
    // Basic SQL highlighting
    sqlText = sqlText
        .replace(/CREATE TABLE|CREATE INDEX|CREATE VIEW|INSERT INTO|SELECT|FROM|WHERE|GROUP BY|ORDER BY|JOIN|INNER|LEFT|RIGHT|OUTER|ON|AND|OR|NOT|NULL|PRIMARY KEY|FOREIGN KEY|REFERENCES|UNIQUE|INDEX|DEFAULT|AUTOINCREMENT/gi, 
            '<span class="sql-keyword">$&</span>')
        .replace(/(\b\d+\b)/g, '<span class="sql-number">$&</span>')
        .replace(/(`[^`]+`|'[^']*')/g, '<span class="sql-string">$&</span>');
    
    sqlOutput.innerHTML = `<pre style="margin:0; padding:0;">${sqlText}</pre>`;
    sqlCount.textContent = `${formatNumber(sqlToShow.length)} SQL statements (${view})`;
}

// Helper functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + sizes[i];
}

function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
    return num.toString();
}

function formatTime(ms) {
    if (ms < 1000) return Math.round(ms) + 'ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
    const minutes = Math.floor(ms / 60000);
    const seconds = Math.floor((ms % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
}

// UI helpers
function showToast(message, type = 'success') {
    if (!toast || !toastMessage) return;
    
    const icon = toast.querySelector('i');
    
    toastMessage.textContent = message;
    toast.className = `toast ${type}`;
    if (icon) {
        icon.className = type === 'success' ? 'fas fa-check-circle' : 
                        type === 'error' ? 'fas fa-exclamation-circle' : 
                        'fas fa-info-circle';
    }
    
    toast.classList.add('show');
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 2000);
}

function updateStatus(text) {
    if (statusText) {
        statusText.textContent = text;
    }
}

// Actions
function downloadSQL() {
    try {
        TikTokExtractor.downloadSQL(`tiktok_${Date.now()}.sql`);
        showToast('SQL file downloaded!', 'success');
    } catch (error) {
        showToast('Error downloading SQL: ' + error.message, 'error');
    }
}

function downloadJSON() {
    try {
        TikTokExtractor.downloadJSON(`tiktok_data_${Date.now()}.json`);
        showToast('JSON file downloaded!', 'success');
    } catch (error) {
        showToast('Error downloading JSON: ' + error.message, 'error');
    }
}

function copySQL() {
    if (extractedResult && extractedResult.sql) {
        const sqlText = extractedResult.sql.join('\n\n');
        navigator.clipboard.writeText(sqlText).then(() => {
            showToast('SQL copied to clipboard!', 'success');
        }).catch(err => {
            showToast('Failed to copy: ' + err.message, 'error');
        });
    }
}

function previewSQL() {
    if (sqlViewSelect) sqlViewSelect.value = 'full';
    updateSQLView();
    switchTab('sql');
}

// Settings
function updateConfigDisplay() {
    if (chunkSizeValue && chunkSize) {
        chunkSizeValue.textContent = chunkSize.value;
    }
    if (batchSizeValue && batchSize) {
        batchSizeValue.textContent = batchSize.value;
    }
}

function loadSettings() {
    try {
        const saved = localStorage.getItem('tiktokExtractorSettings');
        if (saved) {
            const settings = JSON.parse(saved);
            
            if (settings.chunkSize && chunkSize) {
                chunkSize.value = settings.chunkSize;
            }
            
            if (settings.batchSize && batchSize) {
                batchSize.value = settings.batchSize;
            }
        }
    } catch (error) {
        console.error('Error loading settings:', error);
    }
    
    updateConfigDisplay();
}

function saveSettings() {
    try {
        const settings = {};
        if (chunkSize) settings.chunkSize = parseInt(chunkSize.value);
        if (batchSize) settings.batchSize = parseInt(batchSize.value);
        
        localStorage.setItem('tiktokExtractorSettings', JSON.stringify(settings));
        showToast('Settings saved!', 'success');
    } catch (error) {
        showToast('Error saving settings: ' + error.message, 'error');
    }
}

function resetAll() {
    if (isProcessing) {
        if (!confirm('Extraction is in progress. Reset anyway?')) {
            return;
        }
        abortExtraction();
    }
    
    currentFile = null;
    extractedResult = null;
    isProcessing = false;
    
    // Reset UI
    if (fileInput) fileInput.value = '';
    if (sqlOutput) sqlOutput.innerHTML = '';
    if (dataOutput) dataOutput.innerHTML = '';
    if (userInfo) userInfo.style.display = 'none';
    if (fileInfo) fileInfo.style.display = 'none';
    if (progressContainer) progressContainer.style.display = 'none';
    if (abortBtn) abortBtn.style.display = 'none';
    if (statsGrid) statsGrid.innerHTML = '';
    if (statisticsContent) statisticsContent.innerHTML = '<p style="font-size:0.8rem; color:#666; padding:10px;">Upload and extract data to see statistics</p>';
    if (dataTableSelect) dataTableSelect.innerHTML = '<option value="">Select table...</option>';
    if (sqlViewSelect) sqlViewSelect.value = 'full';
    
    // Reset buttons
    if (extractBtn) extractBtn.disabled = true;
    if (downloadSqlBtn) downloadSqlBtn.disabled = true;
    if (copySqlBtn) copySqlBtn.disabled = true;
    if (downloadJsonBtn) downloadJsonBtn.disabled = true;
    if (previewSqlBtn) previewSqlBtn.disabled = true;
    
    // Reset status
    updateStatus('Ready to process TikTok JSON');
    
    // Reset extractor
    TikTokExtractor.reset();
    
    // Switch to upload tab
    setTimeout(() => switchTab('upload'), 100);
    

}


// Add these functions at the BOTTOM of your existing tiktok-to-sql.js
// These are standalone functions that don't interfere with your extractor

function downloadSimpleSQL() {
    try {
        if (!window.TikTokExtractor || typeof window.TikTokExtractor.getSQL !== 'function') {
            showToast('No data available for export', 'error');
            return;
        }
        
        const sql = window.TikTokExtractor.getSQL();
        if (!Array.isArray(sql) || sql.length === 0) {
            showToast('No SQL data to export', 'error');
            return;
        }
        
        const username = window.TikTokExtractor.getCurrentUsername ?
            window.TikTokExtractor.getCurrentUsername() : 'tiktok_user';
        const date = new Date().toISOString().split('T')[0];
        const filename = `tiktok_schema_${username}_${date}.sql`;
        const content = sql.join('\n\n');
        
        // Simple download
        const blob = new Blob([content], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 100);
        
        showToast('SQL schema downloaded!', 'success');
        
    } catch (error) {
        console.error('Export error:', error);
        showToast('Export failed: ' + error.message, 'error');
    }
}

function downloadSimpleJSON() {
    try {
        if (!window.TikTokExtractor || typeof window.TikTokExtractor.getData !== 'function') {
            showToast('No data available for export', 'error');
            return;
        }
        
        const data = window.TikTokExtractor.getData();
        if (!data || !data.tables) {
            showToast('No data to export', 'error');
            return;
        }
        
        const username = window.TikTokExtractor.getCurrentUsername ?
            window.TikTokExtractor.getCurrentUsername() : 'tiktok_user';
        const date = new Date().toISOString().split('T')[0];
        const filename = `tiktok_data_${username}_${date}.json`;
        const content = JSON.stringify(data, null, 2);
        
        // Simple download
        const blob = new Blob([content], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 100);
        
        showToast('JSON data downloaded!', 'success');
        
    } catch (error) {
        console.error('Export error:', error);
        showToast('Export failed: ' + error.message, 'error');
    }
}

function downloadSimpleBoth() {
    downloadSimpleSQL();
    setTimeout(downloadSimpleJSON, 500);
}

// Initialize on load
window.addEventListener('load', init);