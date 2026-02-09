// JSON Structure Analyzer v2.5
class JSONAnalyzer {
    constructor() {
        this.jsonData = null;
        this.stats = {};
        this.maxArrayItems = 3;
        this.maxObjectProperties = 10;
        this.maxDepth = 5;
        this.maxStringLength = 50;
        this.chunkSize = 1024 * 1024;
        this.sampledJSON = null;
        this.summaryText = '';
        this.fileName = '';
        
        // Predefined AI messages
this.aiMessages = [
    {
        title: "Simple & Direct",
        content: `Here are three files from JSON Structure Analyzer for a JSON file structure:
1. structure.txt - shows the JSON hierarchy and relationships
2. sample.json - shows actual data patterns and types  
3. summary.txt - contains this request

IMPORTANT: Ignore any statistics about file size or counts - they vary by user/data volume.
Focus on the STRUCTURE in structure.txt and DATA PATTERNS in sample.json.

Please provide:
1. SQLite schema based on the structure (CREATE TABLE, INDEXES, VIEWS)
2. json-to-sql-mapper.js that maps JSON paths to database columns
3. json-data-extractor.js that works for any data volume with this structure
4. README explaining usage

All data belongs to one user (look for 'userName' field).`
    },
    {
        title: "Structure-Focused Request",
        content: `I have a JSON structure that I need to store in SQLite. All data in these files belongs to one user (identified by 'userName').

Attached are three files:
- structure.txt: Complete JSON hierarchy showing relationships and nesting
- sample.json: Sample data showing actual values and types
- summary.txt: This request (ignore statistical numbers - focus on structure)

CRITICAL: The statistics (file size, object counts, etc.) are for THIS SPECIFIC FILE only. Another user's data with the SAME STRUCTURE will have different statistics.

Please analyze the STRUCTURE (from structure.txt) and DATA PATTERNS (from sample.json) to create:
1. SQLite-compatible database schema that captures the structure
2. json-to-sql-mapper.js mapping each JSON path to its database column
3. json-data-extractor.js that converts JSON to SQL INSERTs (handles any data volume)
4. Brief README

Schema should be based on STRUCTURAL PATTERNS, not data volume.`
    },
    {
        title: "Most Complete (Recommended)",
        content: `I have analyzed a JSON file structure that needs to be stored in SQLite. All data belongs to a single user (identified by 'userName' somewhere in the JSON).

Three files are provided:
1. structure.txt - Complete JSON hierarchy with all relationships and nesting levels
2. sample.json - Clean JSON data samples showing actual values and data types
3. summary.txt - This request (NOTE: Ignore any statistical counts - they're instance-specific)

IMPORTANT CLARIFICATION:
- The structure.txt shows the SCHEMA/TEMPLATE that all users' data will follow
- The sample.json shows EXAMPLE DATA within that structure
- Statistics (file size, object counts) are for THIS EXAMPLE ONLY and vary by user
- Build tools for the STRUCTURE, not for this specific data volume

Required deliverables:
1. SQLite schema (CREATE TABLE with proper types, INDEXES for queries, VIEWS for common access)
2. json-to-sql-mapper.js - Maps every JSON path to its corresponding database table/column
3. json-data-extractor.js - Converts any JSON following this structure to SQL INSERT statements
4. README explaining how to use all components

Focus on the STRUCTURAL PATTERNS in structure.txt, not statistical metrics.`
    }
];
        
        this.initialize();
    }
    
    initialize() {
        this.setupEventListeners();
        this.updateUI();
        this.displayAIMessages();
    }
    
    setupEventListeners() {
        // File upload
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');
        
        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
        uploadArea.addEventListener('dragleave', this.handleDragLeave.bind(this));
        uploadArea.addEventListener('drop', this.handleFileDrop.bind(this));
        fileInput.addEventListener('change', this.handleFileSelect.bind(this));
        
        // Control buttons
        document.getElementById('copyStructureBtn').addEventListener('click', this.copyStructureToClipboard.bind(this));
        document.getElementById('copySampledBtn').addEventListener('click', this.copySampledToClipboard.bind(this));
        document.getElementById('copySummaryBtn').addEventListener('click', this.copySummaryToClipboard.bind(this));
        document.getElementById('copyAllMessagesBtn').addEventListener('click', this.copyAllMessagesToClipboard.bind(this));
        document.getElementById('newFileBtn').addEventListener('click', this.resetAnalyzer.bind(this));
        document.getElementById('newFileBtn2').addEventListener('click', this.resetAnalyzer.bind(this));
        document.getElementById('expandAllBtn').addEventListener('click', () => this.toggleAll(false));
        document.getElementById('collapseAllBtn').addEventListener('click', () => this.toggleAll(true));
        
        // Tab switching
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => this.switchTab(e.target.dataset.tab));
        });
    }
    
    displayAIMessages() {
        const messagesContainer = document.getElementById('messagesContainer');
        messagesContainer.innerHTML = '';
        
        this.aiMessages.forEach((message, index) => {
            const messageBox = document.createElement('div');
            messageBox.className = 'message-box';
            messageBox.innerHTML = `
                <div class="message-header">
                    <div class="message-title">${message.title}</div>
                    <button class="copy-message-btn success" data-index="${index}" style="padding: 4px 8px; font-size: 0.7rem;">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
                <div class="message-content">${this.escapeHtml(message.content)}</div>
            `;
            messagesContainer.appendChild(messageBox);
        });
        
        // Add event listeners to message copy buttons
        document.querySelectorAll('.copy-message-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const index = parseInt(e.target.closest('.copy-message-btn').dataset.index);
                this.copyMessageToClipboard(index);
            });
        });
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    switchTab(tabName) {
        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`.tab-btn[data-tab="${tabName}"]`).classList.add('active');
        
        // Show corresponding tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(`${tabName}Tab`).classList.add('active');
        
        // Show/hide corresponding copy buttons
        document.getElementById('copyStructureBtn').style.display = 'none';
        document.getElementById('copySampledBtn').style.display = 'none';
        document.getElementById('copySummaryBtn').style.display = 'none';
        document.getElementById('copyAllMessagesBtn').style.display = 'none';
        
        if (tabName === 'structure') {
            document.getElementById('copyStructureBtn').style.display = 'flex';
        } else if (tabName === 'sampled') {
            document.getElementById('copySampledBtn').style.display = 'flex';
        } else if (tabName === 'summary') {
            document.getElementById('copySummaryBtn').style.display = 'flex';
        } else if (tabName === 'messages') {
            document.getElementById('copyAllMessagesBtn').style.display = 'flex';
        }
    }
    
    handleDragOver(e) {
        e.preventDefault();
        document.getElementById('uploadArea').classList.add('dragover');
    }
    
    handleDragLeave() {
        document.getElementById('uploadArea').classList.remove('dragover');
    }
    
    handleFileDrop(e) {
        e.preventDefault();
        document.getElementById('uploadArea').classList.remove('dragover');
        
        if (e.dataTransfer.files.length) {
            document.getElementById('fileInput').files = e.dataTransfer.files;
            this.handleFileSelect();
        }
    }
    
    handleFileSelect() {
        const file = document.getElementById('fileInput').files[0];
        if (!file) return;
        
        if (!file.name.toLowerCase().endsWith('.json')) {
            this.showToast('Please select a JSON file', 'error');
            return;
        }
        
        // Store file name
        this.fileName = file.name;
        
        // Show file info
        document.getElementById('fileName').textContent = file.name;
        document.getElementById('fileSize').textContent = this.formatFileSize(file.size);
        document.getElementById('fileInfo').style.display = 'flex';
        
        // Show progress
        const progressContainer = document.getElementById('progressContainer');
        const progressFill = document.getElementById('progressFill');
        const progressPercent = document.getElementById('progressPercent');
        const progressStatus = document.getElementById('progressStatus');
        
        progressContainer.style.display = 'block';
        progressStatus.textContent = 'Reading file...';
        
        // Read file with progress
        const reader = new FileReader();
        let offset = 0;
        let content = '';
        
        reader.onload = (e) => {
            content += e.target.result;
            offset += this.chunkSize;
            
            // Update progress
            const progress = Math.min(100, Math.round((offset / file.size) * 100));
            progressFill.style.width = `${progress}%`;
            progressPercent.textContent = `${progress}%`;
            
            if (offset < file.size) {
                // Read next chunk
                this.readChunk(file, reader, offset);
            } else {
                // File completely loaded
                progressStatus.textContent = 'Parsing JSON...';
                setTimeout(() => this.processJSON(content), 300);
            }
        };
        
        reader.onerror = () => {
            this.showToast('Error reading file', 'error');
            progressContainer.style.display = 'none';
        };
        
        this.readChunk(file, reader, 0);
    }
    
    readChunk(file, reader, start) {
        const end = Math.min(file.size, start + this.chunkSize);
        const slice = file.slice(start, end);
        reader.readAsText(slice);
    }
    
    processJSON(content) {
        try {
            if (!content || content.trim().length === 0) {
                throw new Error('File is empty');
            }
            
            this.jsonData = JSON.parse(content);
            
            // Hide progress, show analysis
            document.getElementById('progressContainer').style.display = 'none';
            document.getElementById('analysisSection').style.display = 'block';
            
            // Calculate statistics
            this.calculateStatistics(this.jsonData);
            
            // Display structure
            this.displayStructure();
            
            // Generate and display sampled JSON
            this.generateSampledJSON();
            
            // Generate and display AI request & summary
            this.generateAIRequestAndSummary();
            
            // Show success message
            this.showToast('JSON analyzed successfully!', 'success');
            
        } catch (error) {
            const errorMsg = error.message || 'Unknown parsing error';
            this.showToast(`Error parsing JSON: ${errorMsg}`, 'error');
            console.error('JSON parsing error:', error);
            document.getElementById('progressContainer').style.display = 'none';
        }
    }
    
    calculateStatistics(data) {
        this.stats = {
            totalSize: JSON.stringify(data).length,
            depth: 0,
            objectCount: 0,
            arrayCount: 0,
            stringCount: 0,
            numberCount: 0,
            booleanCount: 0,
            nullCount: 0,
            totalKeys: 0
        };
        
        this.traverseForStats(data, 0);
        this.updateStatsGrid();
    }
    
    traverseForStats(data, depth) {
        if (depth > this.stats.depth) {
            this.stats.depth = depth;
        }
        
        if (Array.isArray(data)) {
            this.stats.arrayCount++;
            this.stats.totalKeys += data.length;
            
            const limit = Math.min(data.length, 100);
            for (let i = 0; i < limit; i++) {
                if (data[i] !== undefined && data[i] !== null) {
                    if (typeof data[i] === 'object') {
                        this.traverseForStats(data[i], depth + 1);
                    } else {
                        this.countPrimitive(data[i]);
                    }
                } else {
                    this.stats.nullCount++;
                }
            }
        } 
        else if (data !== null && typeof data === 'object') {
            this.stats.objectCount++;
            const keys = Object.keys(data);
            this.stats.totalKeys += keys.length;
            
            const limit = Math.min(keys.length, 100);
            for (let i = 0; i < limit; i++) {
                const key = keys[i];
                const value = data[key];
                
                if (value !== undefined && value !== null) {
                    if (typeof value === 'object') {
                        this.traverseForStats(value, depth + 1);
                    } else {
                        this.countPrimitive(value);
                    }
                } else {
                    this.stats.nullCount++;
                }
            }
        } 
        else {
            this.countPrimitive(data);
        }
    }
    
    countPrimitive(value) {
        switch (typeof value) {
            case 'string':
                this.stats.stringCount++;
                break;
            case 'number':
                this.stats.numberCount++;
                break;
            case 'boolean':
                this.stats.booleanCount++;
                break;
        }
    }
    
    updateStatsGrid() {
        const statsGrid = document.getElementById('statsGrid');
        statsGrid.innerHTML = '';
        
        const statsCards = [
            { value: this.formatFileSize(this.stats.totalSize), label: 'JSON Size', icon: 'fas fa-weight-hanging' },
            { value: this.stats.depth, label: 'Max Depth', icon: 'fas fa-layer-group' },
            { value: this.stats.objectCount, label: 'Objects', icon: 'fas fa-cube' },
            { value: this.stats.arrayCount, label: 'Arrays', icon: 'fas fa-list' },
            { value: this.stats.totalKeys, label: 'Total Keys', icon: 'fas fa-key' },
            { value: this.stats.stringCount, label: 'Strings', icon: 'fas fa-quote-right' },
            { value: this.stats.numberCount, label: 'Numbers', icon: 'fas fa-hashtag' },
            { value: this.stats.booleanCount, label: 'Booleans', icon: 'fas fa-toggle-on' }
        ];
        
        statsCards.forEach(stat => {
            const card = document.createElement('div');
            card.className = 'stat-card';
            card.innerHTML = `
                <div class="stat-value">${stat.value}</div>
                <div class="stat-label">
                    <i class="${stat.icon}"></i> ${stat.label}
                </div>
            `;
            statsGrid.appendChild(card);
        });
    }
    
    displayStructure() {
        const outputArea = document.getElementById('outputArea');
        const structureHTML = this.generateStructureHTML(this.jsonData, 0, 'root');
        outputArea.innerHTML = `<pre>${structureHTML}</pre>`;
        
        // Add click handlers for toggling
        setTimeout(() => {
            document.querySelectorAll('.toggle').forEach(toggle => {
                toggle.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const parent = e.target.parentElement;
                    parent.classList.toggle('collapsed');
                });
            });
        }, 100);
    }
    
    generateStructureHTML(data, depth = 0, key = null, isLast = true) {
        if (depth > this.maxDepth) {
            return `<span class="json-null">[Max depth reached]</span>`;
        }
        
        const indent = '  '.repeat(depth);
        let html = '';
        
        if (key) {
            html += `<span class="json-key">"${key}"</span>: `;
        }
        
        if (Array.isArray(data)) {
            const length = data.length;
            const sampleSize = Math.min(length, this.maxArrayItems);
            const isLarge = length > this.maxArrayItems;
            
            html += `<span class="toggle">[</span>`;
            html += `<span class="json-array">Array(${length})</span> `;
            
            if (length === 0) {
                html += `[]`;
            } else {
                html += `<div class="json-children">`;
                
                for (let i = 0; i < sampleSize; i++) {
                    const isLastItem = i === sampleSize - 1 && !isLarge;
                    html += `${indent}  ${this.generateStructureHTML(data[i], depth + 1, null, isLastItem)}`;
                    if (!isLastItem) html += ',';
                    html += '\n';
                }
                
                if (isLarge) {
                    html += `${indent}  <span class="json-null">... ${length - sampleSize} more items</span>\n`;
                }
                
                html += `${indent}</div>`;
                html += `${indent}]`;
            }
        } 
        else if (data !== null && typeof data === 'object') {
            const keys = Object.keys(data);
            const sampleSize = Math.min(keys.length, this.maxObjectProperties);
            const isLarge = keys.length > this.maxObjectProperties;
            
            html += `<span class="toggle">{</span>`;
            html += `<span class="json-object">Object(${keys.length} properties)</span> `;
            
            if (keys.length === 0) {
                html += `{}`;
            } else {
                html += `<div class="json-children">\n`;
                
                for (let i = 0; i < sampleSize; i++) {
                    const key = keys[i];
                    const value = data[key];
                    const isLastItem = i === sampleSize - 1 && !isLarge;
                    
                    html += `${indent}  <span class="json-key">"${key}"</span>: `;
                    
                    if (typeof value === 'object' && value !== null) {
                        html += `${this.generateStructureHTML(value, depth + 1, null, isLastItem)}`;
                    } else {
                        html += this.formatPrimitive(value);
                    }
                    
                    if (!isLastItem) html += ',';
                    html += '\n';
                }
                
                if (isLarge) {
                    html += `${indent}  <span class="json-null">... ${keys.length - sampleSize} more properties</span>\n`;
                }
                
                html += `${indent}</div>`;
                html += `${indent}}`;
            }
        } 
        else {
            html += this.formatPrimitive(data);
        }
        
        return html;
    }
    
    formatPrimitive(value) {
        if (value === null) {
            return `<span class="json-null">null</span>`;
        }
        
        switch (typeof value) {
            case 'string':
                let displayStr = value;
                if (displayStr.length > this.maxStringLength) {
                    displayStr = displayStr.substring(0, this.maxStringLength) + '...';
                }
                displayStr = displayStr.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
                return `<span class="json-string">"${displayStr}"</span>`;
                
            case 'number':
                return `<span class="json-number">${value}</span>`;
                
            case 'boolean':
                return `<span class="json-boolean">${value}</span>`;
                
            default:
                return `<span class="json-null">${String(value)}</span>`;
        }
    }
    
    generateSampledJSON() {
        this.sampledJSON = this.createSampledObject(this.jsonData, 0);
        
        // Format and display the sampled JSON
        const sampledOutputArea = document.getElementById('sampledOutputArea');
        const formattedJSON = JSON.stringify(this.sampledJSON, null, 2);
        
        // Apply syntax highlighting to the JSON
        const highlightedJSON = this.highlightJSON(formattedJSON);
        sampledOutputArea.innerHTML = `<pre>${highlightedJSON}</pre>`;
    }
    
    createSampledObject(data, depth = 0) {
        if (depth > this.maxDepth) {
            return "[Max depth reached]";
        }
        
        if (Array.isArray(data)) {
            const length = data.length;
            const sampleSize = Math.min(length, this.maxArrayItems);
            const result = [];
            
            for (let i = 0; i < sampleSize; i++) {
                if (data[i] !== undefined && data[i] !== null) {
                    if (typeof data[i] === 'object') {
                        result.push(this.createSampledObject(data[i], depth + 1));
                    } else {
                        result.push(this.samplePrimitive(data[i]));
                    }
                } else {
                    result.push(null);
                }
            }
            
            return result;
        } 
        else if (data !== null && typeof data === 'object') {
            const keys = Object.keys(data);
            const sampleSize = Math.min(keys.length, this.maxObjectProperties);
            const result = {};
            
            for (let i = 0; i < sampleSize; i++) {
                const key = keys[i];
                const value = data[key];
                
                if (value !== undefined && value !== null) {
                    if (typeof value === 'object') {
                        result[key] = this.createSampledObject(value, depth + 1);
                    } else {
                        result[key] = this.samplePrimitive(value);
                    }
                } else {
                    result[key] = null;
                }
            }
            
            return result;
        } 
        else {
            return this.samplePrimitive(data);
        }
    }
    
    samplePrimitive(value) {
        if (value === null) return null;
        
        if (typeof value === 'string') {
            if (value.length > this.maxStringLength) {
                return value.substring(0, this.maxStringLength) + '...';
            }
            return value;
        }
        
        return value;
    }
    
    generateAIRequestAndSummary() {
    this.summaryText = this.createAIRequestAndSummaryText();
    const summaryArea = document.getElementById('summaryArea');
    
    // Create HTML for summary - STRUCTURE ONLY, NO STATISTICS
    const summaryHTML = `<span style="color:#4f46e5;font-weight:bold">AI REQUEST & STRUCTURE SUMMARY</span>

<span style="color:#7c3aed">IMPORTANT NOTE:</span>
  // Statistical metrics (file size, object counts, etc.) are specific to THIS FILE only
  // Another JSON file with the SAME STRUCTURE may have different statistics
  // AI should build helper/extractor based on STRUCTURE, not statistics

<span style="color:#7c3aed">FILE_INFO:</span>
  original_file_name="${this.fileName}"
  // Note: File size varies by user data volume

<span style="color:#7c3aed">STRUCTURE_CHARACTERISTICS:</span>
  // See structure.txt for complete hierarchical structure
  // See sample.json for actual data patterns and types
  // Look for 'userName' field to associate data with specific user

<span style="color:#7c3aed">KEY_REQUIREMENTS:</span>
  // 1. SQLite schema based on STRUCTURE patterns, not data volume
  // 2. Helper script maps JSON paths to database columns
  // 3. Extractor script works for ANY data volume with same structure
  // 4. All data belongs to one user (find 'userName' field)

<span style="color:#6b7280;font-style:italic">// Use structure.txt (hierarchy) + sample.json (data patterns) for analysis
// Ignore statistical counts - they vary by user/data volume
// See AI Messages tab for ready-to-use prompts</span>`;
    
    summaryArea.innerHTML = summaryHTML;
}

createAIRequestAndSummaryText() {
    return `AI REQUEST & STRUCTURE SUMMARY
================================

IMPORTANT NOTE:
  Statistical metrics (file size, object counts, etc.) are specific to THIS FILE only
  Another JSON file with the SAME STRUCTURE may have different statistics
  AI should build helper/extractor based on STRUCTURE, not statistics

FILE_INFO:
  original_file_name="${this.fileName}"
  Note: File size varies by user data volume

STRUCTURE_CHARACTERISTICS:
  See structure.txt for complete hierarchical structure
  See sample.json for actual data patterns and types
  Look for 'userName' field to associate data with specific user

KEY_REQUIREMENTS:
  1. SQLite schema based on STRUCTURE patterns, not data volume
  2. Helper script maps JSON paths to database columns
  3. Extractor script works for ANY data volume with same structure
  4. All data belongs to one user (find 'userName' field)

// Use structure.txt (hierarchy) + sample.json (data patterns) for analysis
// Ignore statistical counts - they vary by user/data volume
// See AI Messages tab for ready-to-use prompts`;
}
    
   
    highlightJSON(json) {
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
    
    toggleAll(collapse) {
        const toggles = document.querySelectorAll('#structureTab .toggle');
        toggles.forEach(toggle => {
            const parent = toggle.parentElement;
            if (collapse) {
                parent.classList.add('collapsed');
            } else {
                parent.classList.remove('collapsed');
            }
        });
    }
    
    copyStructureToClipboard() {
        const outputText = this.generateTextStructure(this.jsonData, 0, 'root');
        navigator.clipboard.writeText(outputText).then(() => {
            this.showToast('Structure copied! Save as structure.txt', 'success');
        }).catch(err => {
            console.error('Failed to copy: ', err);
            this.showToast('Failed to copy', 'error');
        });
    }
    
    copySampledToClipboard() {
        if (!this.sampledJSON) return;
        const outputText = JSON.stringify(this.sampledJSON, null, 2);
        navigator.clipboard.writeText(outputText).then(() => {
            this.showToast('Sampled JSON copied! Save as sample.json', 'success');
        }).catch(err => {
            console.error('Failed to copy: ', err);
            this.showToast('Failed to copy', 'error');
        });
    }
    
    copySummaryToClipboard() {
        if (!this.summaryText) return;
        navigator.clipboard.writeText(this.summaryText).then(() => {
            this.showToast('Summary copied! Save as summary.txt', 'success');
        }).catch(err => {
            console.error('Failed to copy: ', err);
            this.showToast('Failed to copy', 'error');
        });
    }
    
    copyMessageToClipboard(index) {
        if (index >= 0 && index < this.aiMessages.length) {
            const message = this.aiMessages[index].content;
            navigator.clipboard.writeText(message).then(() => {
                this.showToast(`"${this.aiMessages[index].title}" copied!`, 'success');
            }).catch(err => {
                console.error('Failed to copy: ', err);
                this.showToast('Failed to copy', 'error');
            });
        }
    }
    
    copyAllMessagesToClipboard() {
        const allMessages = this.aiMessages.map(msg => 
            `=== ${msg.title} ===\n\n${msg.content}\n\n`
        ).join('\n');
        
        navigator.clipboard.writeText(allMessages).then(() => {
            this.showToast('All messages copied!', 'success');
        }).catch(err => {
            console.error('Failed to copy: ', err);
            this.showToast('Failed to copy', 'error');
        });
    }
    
    generateTextStructure(data, depth = 0, key = null, isLast = true) {
        if (depth > this.maxDepth) {
            return '[Max depth reached]';
        }
        
        const indent = '  '.repeat(depth);
        let text = '';
        
        if (key) {
            text += `${indent}"${key}": `;
        } else if (depth > 0) {
            text += indent;
        }
        
        if (Array.isArray(data)) {
            const length = data.length;
            const sampleSize = Math.min(length, this.maxArrayItems);
            const isLarge = length > this.maxArrayItems;
            
            text += `[Array(${length})]\n`;
            
            if (length > 0) {
                for (let i = 0; i < sampleSize; i++) {
                    const isLastItem = i === sampleSize - 1 && !isLarge;
                    text += this.generateTextStructure(data[i], depth + 1, null, isLastItem);
                    if (!isLastItem) text += ',\n';
                }
                
                if (isLarge) {
                    text += `${indent}  ... ${length - sampleSize} more items\n`;
                }
                
                text += `${indent}]`;
            } else {
                text += `${indent}[]`;
            }
        } 
        else if (data !== null && typeof data === 'object') {
            const keys = Object.keys(data);
            const sampleSize = Math.min(keys.length, this.maxObjectProperties);
            const isLarge = keys.length > this.maxObjectProperties;
            
            text += `{Object(${keys.length} properties)}\n`;
            
            if (keys.length > 0) {
                for (let i = 0; i < sampleSize; i++) {
                    const key = keys[i];
                    const value = data[key];
                    const isLastItem = i === sampleSize - 1 && !isLarge;
                    
                    text += `${indent}  "${key}": `;
                    
                    if (typeof value === 'object' && value !== null) {
                        text += this.generateTextStructure(value, depth + 2, null, isLastItem);
                    } else {
                        text += this.formatPrimitiveText(value);
                    }
                    
                    if (!isLastItem) text += ',\n';
                }
                
                if (isLarge) {
                    text += `${indent}  ... ${keys.length - sampleSize} more properties\n`;
                }
                
                text += `${indent}}`;
            } else {
                text += `${indent}{}`;
            }
        } 
        else {
            text += this.formatPrimitiveText(data);
        }
        
        return text;
    }
    
    formatPrimitiveText(value) {
        if (value === null) return 'null';
        
        switch (typeof value) {
            case 'string':
                let displayStr = value;
                if (displayStr.length > this.maxStringLength) {
                    displayStr = displayStr.substring(0, this.maxStringLength) + '...';
                }
                return `"${displayStr.replace(/"/g, '\\"')}"`;
                
            case 'number':
                return value.toString();
                
            case 'boolean':
                return value.toString();
                
            default:
                return String(value);
        }
    }
    
    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    resetAnalyzer() {
        this.jsonData = null;
        this.sampledJSON = null;
        this.summaryText = '';
        this.fileName = '';
        document.getElementById('fileInput').value = '';
        document.getElementById('fileInfo').style.display = 'none';
        document.getElementById('analysisSection').style.display = 'none';
        document.getElementById('uploadArea').classList.remove('dragover');
        this.switchTab('structure');
        this.showToast('Ready for new file', 'success');
    }
    
    showToast(message, type = 'success') {
        const toast = document.getElementById('toast');
        const toastMessage = document.getElementById('toastMessage');
        
        toastMessage.textContent = message;
        toast.className = `toast ${type}`;
        
        const icon = toast.querySelector('i');
        icon.className = type === 'success' ? 'fas fa-check-circle' : 'fas fa-exclamation-circle';
        
        toast.classList.add('show');
        
        setTimeout(() => {
            toast.classList.remove('show');
        }, 2000);
    }
    
    updateUI() {
        document.getElementById('analysisSection').style.display = 'none';
        document.getElementById('progressContainer').style.display = 'none';
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    new JSONAnalyzer();
});