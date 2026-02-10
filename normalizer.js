// normalizerCore.js - Standalone Core Data Processing Functions
class DataNormalizerCore {
    constructor() {
        this.processors = this.registerProcessors();
    }
    
    // Register all available data processors
    registerProcessors() {
        return {
            // Main processors - NO _processed suffix in field names
            'tables.group_chats': {
                inputField: 'message_content',
                processor: this.processMessage.bind(this),
                processTikTok: true,
                processMedia: true,
                shorten: true,
                type: 'message'
            },
            'tables.direct_messages': {
                inputField: 'message_content',
                processor: this.processMessage.bind(this),
                processTikTok: true,
                processMedia: true,
                shorten: true,
                type: 'message'
            },
            'tables.comments': {
                inputField: 'comment_text',
                processor: this.processMessage.bind(this),
                processTikTok: false,
                processMedia: true,
                shorten: true,
                type: 'comment'
            },
            'tables.reposts': {
                inputField: 'video_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'link'
            },
            'tables.share_history': {
                inputField: 'shared_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'link'
            },
            'tables.posts': {
                inputField: 'video_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'storage_url'
            },
            'tables.liked_videos': {
                inputField: 'video_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'link'
            },
            'tables.deleted_posts': {
                inputField: 'video_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'link'
            },
            'tables.favorite_videos': {
                inputField: 'video_link',
                processor: this.processLink.bind(this),
                processTikTok: true,
                processMedia: false,
                shorten: false,
                type: 'link'
            },
            'tables.favorite_sounds': {
                inputField: 'sound_link',
                processor: this.processSoundLink.bind(this),
                processTikTok: false,
                processMedia: true,
                shorten: false,
                type: 'sound_link'
            },
            // NEW: Processor for favorite_effects table
            'tables.favorite_effects': {
                inputField: 'effect_link',
                processor: this.processEffectLink.bind(this),
                processTikTok: false,
                processMedia: true,
                shorten: false,
                type: 'effect_link'
            },
            // Legacy support
            'group_chats': {
                inputField: 'message_content',
                processor: this.processMessage.bind(this),
                processTikTok: true,
                processMedia: true,
                shorten: true,
                type: 'message'
            }
        };
    }
    
    // Add a new processor dynamically
    addProcessor(path, config) {
        this.processors[path] = config;
    }
    
    // Safe string conversion
    safeToString(value) {
        if (value == null) return '';
        if (typeof value === 'string') return value;
        if (typeof value === 'number' || typeof value === 'boolean') return value.toString();
        if (typeof value === 'object') {
            try {
                return JSON.stringify(value);
            } catch {
                return String(value);
            }
        }
        return String(value);
    }
    
    // Generate short hash
    generateShortHash(str, length = 8) {
        if (!str) return '';
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        const base36 = Math.abs(hash).toString(36);
        return base36.substring(0, length).toUpperCase();
    }
    
    // Check if text contains URLs
    hasUrls(text) {
        if (!text || typeof text !== 'string') return false;
        const urlRegex = /https?:\/\/[^\s\]]+/g;
        return urlRegex.test(text);
    }
    
    // Extract video hash from URL (handles both types) - for LINK fields only
    extractVideoHash(url) {
        if (!url || typeof url !== 'string') return url || '';
        
        // 1. Regular TikTok video URLs
        const tiktokRegex = /https:\/\/www\.tiktokv\.com\/share\/video\/(\d+)(?:\/|$)/g;
        const match = tiktokRegex.exec(url);
        if (match) return match[1];
        
        // 2. Storage URLs (like posts.video_link)
        const storageRegex = /https:\/\/video-(?:[^\/]+)\.tiktokv\.com\/storage\/v1\/[^\/]+\/([^\/?\&]+)/i;
        const storageMatch = url.match(storageRegex);
        
        if (storageMatch && storageMatch[1]) {
            // Generate hash from the unique object ID
            return this.generateShortHash(storageMatch[1], 10);
        }
        
        // 3. For other URLs, try to extract filename
        const filenameRegex = /\/([^\/?\&]+)(?:\?|$)/i;
        const filenameMatch = url.match(filenameRegex);
        
        if (filenameMatch && filenameMatch[1]) {
            const filename = filenameMatch[1];
            if (filename.length > 20) {
                return this.generateShortHash(url, 10);
            }
            return filename;
        }
        
        // 4. Fallback: hash the entire URL
        return this.generateShortHash(url, 12);
    }
    
    // Extract TikTok video ID from text (only from URLs in messages)
    extractVideoIdFromText(text) {
        if (!text || typeof text !== 'string') return text || '';
        
        // Find all TikTok video URLs and replace them with just the video ID
        const tiktokRegex = /https:\/\/www\.tiktokv\.com\/share\/video\/(\d+)/gi;
        
        return text.replace(tiktokRegex, (match, videoId) => {
            return videoId; // Replace URL with just the video ID
        });
    }
    
    // Extract media filenames from text (only from URLs in messages)
    extractMediaFilenamesFromText(text) {
        if (!text || typeof text !== 'string') return text || '';
        
        let result = text;
        
        // 1. Replace bracketed URLs with filenames
        const bracketedUrlRegex = /\[https?:\/\/[^\]]+\/([^\/?\]]+)(?:\?[^\]]*)?\]/g;
        result = result.replace(bracketedUrlRegex, '$1');
        
        // 2. Handle non-bracketed URLs that are media files
        const urlRegex = /(https?:\/\/[^\s]+\/([^\/?\s]+)(?:\?[^\s]*)?)/g;
        result = result.replace(urlRegex, (fullUrl, url, filename) => {
            // Only replace if it looks like a media file URL
            if (url.includes('.mp4') || url.includes('.mp3') || url.includes('.jpg') || 
                url.includes('.png') || url.includes('.gif') || url.includes('.webp') ||
                url.includes('tiktok.com') || url.includes('tiktokv.com')) {
                return filename;
            }
            return fullUrl; // Keep original if not a media URL
        });
        
        return result;
    }
    
    // Extract filename from sound link (non-bracketed URLs)
    extractSoundFilename(text) {
        if (!text || typeof text !== 'string') return text || '';
        
        // Handle both bracketed and regular URLs
        const bracketedMatch = text.match(/\[https?:\/\/[^\]]+\/([^\/?\]]+)(?:\?[^\]]*)?\]/);
        if (bracketedMatch) return bracketedMatch[1];
        
        // Try regular URL format
        const urlMatch = text.match(/https?:\/\/[^\s]+\/([^\/?\s]+)(?:\?[^\s]*)?/);
        if (urlMatch) return urlMatch[1];
        
        // If no URL found, return original
        return text;
    }
    
    // Extract filename from effect link (similar to sound links)
    extractEffectFilename(text) {
        if (!text || typeof text !== 'string') return text || '';
        
        // Handle both bracketed and regular URLs
        const bracketedMatch = text.match(/\[https?:\/\/[^\]]+\/([^\/?\]]+)(?:\?[^\]]*)?\]/);
        if (bracketedMatch) return bracketedMatch[1];
        
        // Try regular URL format
        const urlMatch = text.match(/https?:\/\/[^\s]+\/([^\/?\s]+)(?:\?[^\s]*)?/);
        if (urlMatch) return urlMatch[1];
        
        // Try TikTok effect-specific URLs
        const tiktokEffectRegex = /https?:\/\/(?:www\.)?tiktok\.com\/[^\s]*\/(?:effect|sticker)\/([^\/?\s]+)/i;
        const tiktokMatch = text.match(tiktokEffectRegex);
        if (tiktokMatch) return tiktokMatch[1];
        
        // If no URL found, return original
        return text;
    }
    
    // Shorten message with optional URL preservation
    shortenMessage(text, maxLength = 100, preserveUrls = true, ellipsis = '...') {
        if (!text || typeof text !== 'string') return text || '';
        if (text.length <= maxLength) return text;
        
        let messageToShorten = text;
        const urlMap = new Map();
        
        if (preserveUrls) {
            const urlRegex = /https?:\/\/[^\s\]]+/g;
            const matches = [];
            let match;
            
            while ((match = urlRegex.exec(text)) !== null) {
                matches.push({ url: match[0], index: match.index });
            }
            
            for (let i = matches.length - 1; i >= 0; i--) {
                const m = matches[i];
                const placeholder = `__URL_${i}__`;
                urlMap.set(placeholder, m.url);
                messageToShorten = messageToShorten.substring(0, m.index) + 
                                  placeholder + 
                                  messageToShorten.substring(m.index + m.url.length);
            }
        }
        
        if (messageToShorten.length > maxLength) {
            let cutIndex = maxLength - ellipsis.length;
            const lastSpace = messageToShorten.lastIndexOf(' ', cutIndex);
            if (lastSpace > maxLength * 0.5) cutIndex = lastSpace;
            
            for (const [placeholder] of urlMap) {
                const idx = messageToShorten.indexOf(placeholder);
                if (idx !== -1 && idx < cutIndex && idx + placeholder.length > cutIndex) {
                    cutIndex = idx + placeholder.length;
                    break;
                }
            }
            
            messageToShorten = messageToShorten.substring(0, cutIndex).trim() + ellipsis;
        }
        
        if (preserveUrls) {
            for (const [placeholder, url] of urlMap) {
                messageToShorten = messageToShorten.replace(placeholder, url);
            }
        }
        
        return messageToShorten;
    }
    
    // Smart truncation preserving word boundaries
    smartTruncate(text, maxLength = 100, ellipsis = '...') {
        if (!text || typeof text !== 'string') return text || '';
        if (text.length <= maxLength) return text;
        
        const cutLength = maxLength - ellipsis.length;
        let truncated = text.substring(0, cutLength);
        
        const lastSpace = truncated.lastIndexOf(' ');
        const lastPunctuation = Math.max(
            truncated.lastIndexOf('.'),
            truncated.lastIndexOf(','),
            truncated.lastIndexOf('!'),
            truncated.lastIndexOf('?'),
            truncated.lastIndexOf(';'),
            truncated.lastIndexOf(':')
        );
        
        const breakPoint = Math.max(lastPunctuation, lastSpace);
        if (breakPoint > cutLength * 0.5) {
            truncated = truncated.substring(0, breakPoint);
        }
        
        return truncated.trim() + ellipsis;
    }
    
    // Process a single message with options - FIXED VERSION
    processMessage(text, options = {}) {
        if (text == null) return '';
        
        let processed = this.safeToString(text);
        
        // SAFE option extraction with defaults
        const maxLength = options.maxLength || 100;
        const extractVideoId = options.extractVideoId !== undefined ? options.extractVideoId : true;
        const extractMediaFilename = options.extractMediaFilename !== undefined ? options.extractMediaFilename : true;
        const preserveUrls = options.preserveUrls !== undefined ? options.preserveUrls : true;
        const smartTruncate = options.smartTruncate !== undefined ? options.smartTruncate : false;
        const ellipsis = options.ellipsis || '...';
        
        // Only process URL extraction if text contains URLs
        const containsUrls = this.hasUrls(processed);
        
        if (containsUrls) {
            // Extract TikTok IDs from URLs
            if (extractVideoId) {
                processed = this.extractVideoIdFromText(processed);
            }
            
            // Extract media filenames from URLs
            if (extractMediaFilename) {
                processed = this.extractMediaFilenamesFromText(processed);
            }
        }
        
        // Shorten if needed (preserve word boundaries)
        if (maxLength > 0 && processed.length > maxLength) {
            if (smartTruncate) {
                processed = this.smartTruncate(processed, maxLength, ellipsis);
            } else {
                processed = this.shortenMessage(processed, maxLength, preserveUrls, ellipsis);
            }
        }
        
        return processed;
    }
    
    // Process links (for video_link, shared_link fields) - MORE AGGRESSIVE
    processLink(text, options = {}) {
        if (text == null) return '';
        
        let processed = this.safeToString(text);
        const extractVideoId = options.extractVideoId !== undefined ? options.extractVideoId : true;
        
        if (extractVideoId) {
            // For link fields, always try to extract video ID/hash
            processed = this.extractVideoHash(processed);
        }
        
        return processed;
    }
    
    // Process sound links - extract filename
    processSoundLink(text, options = {}) {
        if (text == null) return '';
        
        let processed = this.safeToString(text);
        const extractMediaFilename = options.extractMediaFilename !== undefined ? options.extractMediaFilename : true;
        
        if (extractMediaFilename) {
            processed = this.extractSoundFilename(processed);
        }
        
        return processed;
    }
    
    // Process effect links - extract filename
    processEffectLink(text, options = {}) {
        if (text == null) return '';
        
        let processed = this.safeToString(text);
        const extractMediaFilename = options.extractMediaFilename !== undefined ? options.extractMediaFilename : true;
        
        if (extractMediaFilename) {
            processed = this.extractEffectFilename(processed);
        }
        
        return processed;
    }
    
    // Process an array of items
    processArray(items, processorConfig, options = {}) {
        if (!Array.isArray(items)) return items;
        
        return items.map(item => {
            if (item && item[processorConfig.inputField] !== undefined) {
                const originalValue = this.safeToString(item[processorConfig.inputField]);
                
                // Prepare options for the processor
                const processorOptions = {
                    maxLength: processorConfig.shorten ? (options.maxLength || 100) : 0,
                    extractVideoId: processorConfig.processTikTok ? 
                        (options.extractVideoId !== undefined ? options.extractVideoId : true) : false,
                    extractMediaFilename: processorConfig.processMedia ? 
                        (options.extractMediaFilename !== undefined ? options.extractMediaFilename : true) : false,
                    preserveUrls: options.preserveUrls !== undefined ? options.preserveUrls : true,
                    smartTruncate: options.smartTruncate !== undefined ? options.smartTruncate : false,
                    ellipsis: options.ellipsis || '...'
                };
                
                let processed;
                
                // Route to appropriate processor
                if (processorConfig.processor === this.processSoundLink.bind(this)) {
                    processed = this.processSoundLink(originalValue, processorOptions);
                } else if (processorConfig.processor === this.processEffectLink.bind(this)) {
                    processed = this.processEffectLink(originalValue, processorOptions);
                } else if (processorConfig.processor === this.processLink.bind(this)) {
                    processed = this.processLink(originalValue, processorOptions);
                } else {
                    processed = processorConfig.processor(originalValue, processorOptions);
                }
                
                // REPLACE the original value in-place
                return {
                    ...item,
                    [processorConfig.inputField]: processed
                };
            }
            return item;
        });
    }
    
    // MAIN PUBLIC METHOD: Normalize all data with progress callback
    async normalizeAllData(jsonData, options = {}, chunkSize = 1000, progressCallback = null) {
        // If jsonData is a string, parse it
        if (typeof jsonData === 'string') {
            try {
                jsonData = JSON.parse(jsonData);
            } catch (error) {
                throw new Error('Invalid JSON string provided');
            }
        }
        
        // If jsonData is not an object, throw error
        if (typeof jsonData !== 'object' || jsonData === null) {
            throw new Error('Input must be a JSON object or valid JSON string');
        }
        
        const data = jsonData;
        let totalItems = 0;
        let processedItems = 0;
        const activeProcessors = [];
        
        // Identify which processors to use based on data structure
        for (const [path, config] of Object.entries(this.processors)) {
            const parts = path.split('.');
            let current = data;
            
            for (const part of parts) {
                if (current && current[part] && Array.isArray(current[part])) {
                    current = current[part];
                    activeProcessors.push({ path, items: current, config });
                    totalItems += current.length;
                    break;
                }
                if (current && current[part]) {
                    current = current[part];
                } else {
                    break;
                }
            }
        }
        
        if (activeProcessors.length === 0) {
            throw new Error('No processable data found in the JSON');
        }
        
        // Process each active processor
        for (const processor of activeProcessors) {
            const items = processor.items;
            const total = items.length;
            const chunks = Math.ceil(total / chunkSize);
            
            for (let i = 0; i < chunks; i++) {
                const start = i * chunkSize;
                const end = Math.min(start + chunkSize, total);
                const chunk = items.slice(start, end);
                
                // Process chunk
                const processedChunk = this.processArray(chunk, processor.config, options);
                
                // Replace the chunk in the original array
                for (let j = 0; j < processedChunk.length; j++) {
                    items[start + j] = processedChunk[j];
                }
                
                processedItems += end - start;
                
                // Call progress callback if provided
                if (progressCallback && typeof progressCallback === 'function') {
                    progressCallback(processedItems, totalItems);
                }
                
                // Yield to prevent blocking (non-blocking async)
                if (i % 10 === 0) {
                    await new Promise(resolve => setTimeout(resolve, 0));
                }
            }
        }
        
        return data;
    }
    
    // QUICK PROCESS: Your preferred defaults (preserveUrls: false, smartTruncate: true)
    async quickProcess(jsonData) {
        const quickOptions = {
            maxLength: 100,
            extractVideoId: true,
            extractMediaFilename: true,
            preserveUrls: false,      // Your preference
            smartTruncate: true,      // Your preference
            ellipsis: '...',
            chunkSize: 1000
        };
        
        return await this.normalizeAllData(
            jsonData,
            quickOptions,
            quickOptions.chunkSize
        );
    }
    
    // SIMPLE API: Process data with minimal options
    async processData(jsonData, options = {}) {
        const defaultOptions = {
            maxLength: 100,
            extractVideoId: true,
            extractMediaFilename: true,
            preserveUrls: true,
            smartTruncate: false,
            ellipsis: '...',
            chunkSize: 1000
        };
        
        const mergedOptions = { ...defaultOptions, ...options };
        
        return await this.normalizeAllData(
            jsonData,
            mergedOptions,
            mergedOptions.chunkSize
        );
    }
    
    // FILTERED PROCESS: Process only specific tables
    async processFiltered(jsonData, filterOptions = {}, processingOptions = {}) {
        // Create a deep copy to avoid modifying original
        const data = JSON.parse(JSON.stringify(jsonData));
        
        // Default filter: process all tables
        const defaultFilter = {
            group_chats: true,
            direct_messages: true,
            comments: true,
            reposts: true,
            share_history: true,
            posts: true,
            liked_videos: true,
            deleted_posts: true,
            favorite_videos: true,
            favorite_sounds: true,
            favorite_effects: true  // NEW: Include favorite_effects
        };
        
        const filter = { ...defaultFilter, ...filterOptions };
        
        // Process each table based on filter
        for (const [tableName, shouldProcess] of Object.entries(filter)) {
            if (!shouldProcess) continue;
            
            const processorPath = `tables.${tableName}`;
            const config = this.processors[processorPath];
            
            if (config && data.tables && data.tables[tableName]) {
                const items = data.tables[tableName];
                if (Array.isArray(items)) {
                    data.tables[tableName] = this.processArray(items, config, processingOptions);
                }
            }
        }
        
        return data;
    }
    
    // EXTREME COMPRESSION: Maximum reduction settings
    async compressMax(jsonData) {
        const maxCompressionOptions = {
            maxLength: 50,           // Very short
            extractVideoId: true,
            extractMediaFilename: true,
            preserveUrls: false,     // Don't preserve URLs
            smartTruncate: true,     // Smart truncation
            ellipsis: '...',
            chunkSize: 1000
        };
        
        return await this.normalizeAllData(jsonData, maxCompressionOptions);
    }
    
    // PRESERVE ALL: Keep URLs and don't truncate much
    async preserveAll(jsonData) {
        const preserveOptions = {
            maxLength: 200,          // Longer limit
            extractVideoId: true,
            extractMediaFilename: true,
            preserveUrls: true,      // Keep URLs
            smartTruncate: false,    // Simple truncation
            ellipsis: '...',
            chunkSize: 1000
        };
        
        return await this.normalizeAllData(jsonData, preserveOptions);
    }
}

// Export for different environments
if (typeof window !== 'undefined') {
    window.DataNormalizerCore = DataNormalizerCore;
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = DataNormalizerCore;
}

if (typeof exports !== 'undefined') {
    exports.DataNormalizerCore = DataNormalizerCore;
}