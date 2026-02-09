// tiktok-mapper.js - Complete version with date handling and username extraction
// FIXED: Numeric fields are no longer incorrectly parsed as dates
var TikTokMapper = (function() {
    'use strict';
    
    // Date field patterns - case-insensitive matching
    // FIXED: Removed patterns that could match numeric fields
    const DATE_FIELD_PATTERNS = [
        /_date$/i,
        /_time$/i,
        /date$/i,
        /time$/i,
        /_at$/i,
        /Date$/,
        /Time$/,
        /Start$/i,
        /End$/i,
        /Deleted$/i,
        /birth/i,
        /created/i,
        /updated/i,
        /watch_time/i,
        /login_date/i,
        /purchase_date/i,
        /send_date/i,
        /share_date/i,
        /repost_date/i,
        /search_date/i,
        /follow_date/i,
        /block_date/i,
        /favorite_date/i,
        /like_date/i,
        /comment_date/i,
        /post_date/i,
        /live_/i,  // Only match live_start_time, live_end_time
        /browsing_date/i
    ];
    
    // Fields that should NEVER be treated as dates (even if they match patterns)
    const NON_DATE_FIELDS = [
        'followerCount', 'followingCount', 'likesCount', 'totalLikes', 'totalViews',
        'follower_count', 'following_count', 'likes_count', 'total_likes', 'total_views',
        'count', 'amount', 'number', 'total', 'quantity', 'score', 'rating', 'duration',
        'coinAmount', 'coin_amount', 'giftAmount', 'gift_amount', 'price', 'cost',
        'followerCount', 'followingCount', 'likesCount', 'coinAmount', 'giftAmount'
    ];
    
    // Special date fields that need specific handling
    const SPECIAL_DATE_FIELDS = {
        'birthDate': 'birth_date',  // Needs special parsing
        'RawTime': 'raw_time'       // Unix timestamp, store as INTEGER
    };
    
    // Numeric field patterns - fields that should be numeric
    const NUMERIC_FIELD_PATTERNS = [
        /count$/i,
        /amount$/i,
        /number$/i,
        /total$/i,
        /quantity$/i,
        /score$/i,
        /rating$/i,
        /duration$/i,
        /price$/i,
        /cost$/i,
        /value$/i,
        /size$/i,
        /length$/i,
        /width$/i,
        /height$/i,
        /weight$/i,
        /age$/i,
        /percent$/i,
        /percentage$/i,
        /ratio$/i,
        /frequency$/i,
        /rate$/i
    ];
    
    // Path mapping configuration with date field annotations
    const JSON_PATH_MAPPING = {
        // User Profile - includes userName field
        'Profile And Settings.Profile Info.ProfileMap': {
            table: 'users',
            columns: {
                'userName': 'username',           // Unique TikTok username
                'displayName': 'display_name',
                'emailAddress': 'email',
                'bioDescription': 'bio_description',
                'birthDate': 'birth_date',        // Special date format
                'accountRegion': 'account_region',
                'followerCount': 'follower_count',  // NUMERIC FIELD
                'followingCount': 'following_count' // NUMERIC FIELD
            },
            dateFields: ['birth_date'],
            numericFields: ['follower_count', 'following_count']
        },

        // Comments
        'Comment.Comments.CommentsList': {
            table: 'comments',
            isArray: true,
            columns: {
                'date': 'comment_date',           // DATE FIELD
                'comment': 'comment_text',
                'photo': 'photo_url',
                'url': 'video_url'
            },
            dateFields: ['comment_date']
        },

        // Direct Messages
        'Direct Message.Direct Messages.ChatHistory': {
            table: 'direct_messages',
            isDynamic: true,
            dynamicKeyColumn: 'chat_identifier',
            columns: {
                'Date': 'message_date',           // DATE FIELD
                'From': 'sender_username',
                'Content': 'message_content'
            },
            dateFields: ['message_date']
        },

        // Group Chats
        'Direct Message.Group Chat.GroupChat': {
            table: 'group_chats',
            isDynamic: true,
            dynamicKeyColumn: 'group_chat_identifier',
            columns: {
                'Date': 'message_date',           // DATE FIELD
                'From': 'sender_username',
                'Content': 'message_content'
            },
            dateFields: ['message_date']
        },

        // Coin Purchases
        'Income+ Wallet.Coin Purchase History.CoinPurchaseHistoryList': {
            table: 'coin_purchases',
            isArray: true,
            columns: {
                'Date': 'purchase_date',          // DATE FIELD
                'Type': 'purchase_type',
                'CoinAmount': 'coin_amount'       // NUMERIC FIELD
            },
            dateFields: ['purchase_date'],
            numericFields: ['coin_amount']
        },

        // Favorites
        'Likes and Favorites.Favorite Collection.FavoriteCollectionList': {
            table: 'favorite_collections',
            isArray: true,
            columns: {
                'Date': 'favorite_date',          // DATE FIELD
                'FavoriteCollection': 'collection_name'
            },
            dateFields: ['favorite_date']
        },

        'Likes and Favorites.Favorite Comment.FavoriteCommentList': {
            table: 'favorite_comments',
            isArray: true,
            columns: {
                'FavoriteComment': 'comment_text'
            }
            // No date field
        },

        'Likes and Favorites.Favorite Effects.FavoriteEffectsList': {
            table: 'favorite_effects',
            isArray: true,
            columns: {
                'Date': 'effect_date',            // DATE FIELD
                'EffectLink': 'effect_link'
            },
            dateFields: ['effect_date']
        },

        'Likes and Favorites.Favorite Hashtags.FavoriteHashtagList': {
            table: 'favorite_hashtags',
            isArray: true,
            columns: {
                'Date': 'favorite_date',          // DATE FIELD
                'Link': 'hashtag_link'
            },
            dateFields: ['favorite_date']
        },

        'Likes and Favorites.Favorite Sounds.FavoriteSoundList': {
            table: 'favorite_sounds',
            isArray: true,
            columns: {
                'Date': 'favorite_date',          // DATE FIELD
                'Link': 'sound_link'
            },
            dateFields: ['favorite_date']
        },

        'Likes and Favorites.Favorite Videos.FavoriteVideoList': {
            table: 'favorite_videos',
            isArray: true,
            columns: {
                'Date': 'favorite_date',          // DATE FIELD
                'Link': 'video_link'
            },
            dateFields: ['favorite_date']
        },

        'Likes and Favorites.Like List.ItemFavoriteList': {
            table: 'liked_videos',
            isArray: true,
            columns: {
                'date': 'like_date',              // DATE FIELD (lowercase)
                'link': 'video_link'
            },
            dateFields: ['like_date']
        },

        // Posts
        'Post.Posts.VideoList': {
            table: 'posts',
            isArray: true,
            columns: {
                'Date': 'post_date',              // DATE FIELD
                'Link': 'video_link',
                'Likes': 'likes_count',           // NUMERIC FIELD
                'WhoCanView': 'who_can_view',
                'AllowComments': 'allow_comments',
                'AllowStitches': 'allow_stitches',
                'AllowDuets': 'allow_duets',
                'AllowStickers': 'allow_stickers',
                'AllowSharingToStory': 'allow_sharing_to_story',
                'ContentDisclosure': 'content_disclosure'
            },
            dateFields: ['post_date'],
            numericFields: ['likes_count']
        },

        'Post.Recently Deleted Posts.PostList': {
            table: 'deleted_posts',
            isArray: true,
            columns: {
                'Date': 'post_date',              // DATE FIELD
                'DateDeleted': 'delete_date',     // DATE FIELD
                'Link': 'video_link',
                'Likes': 'likes_count',           // NUMERIC FIELD
                'ContentDisclosure': 'content_disclosure',
                'AIGeneratedContent': 'ai_generated',
                'Sound': 'sound_used',
                'Location': 'location',
                'Title': 'title',
                'AddYoursText': 'add_yours_text'
            },
            dateFields: ['post_date', 'delete_date'],
            numericFields: ['likes_count']
        },

        // Social Connections
        'Profile And Settings.Block List.BlockList': {
            table: 'blocked_users',
            isArray: true,
            columns: {
                'Date': 'block_date',             // DATE FIELD
                'UserName': 'blocked_username'
            },
            dateFields: ['block_date']
        },

        'Profile And Settings.Follower.FansList': {
            table: 'followers',
            isArray: true,
            columns: {
                'Date': 'follow_date',            // DATE FIELD
                'UserName': 'follower_username'
            },
            dateFields: ['follow_date']
        },

        'Profile And Settings.Following.Following': {
            table: 'following',
            isArray: true,
            columns: {
                'Date': 'follow_date',            // DATE FIELD
                'UserName': 'following_username'
            },
            dateFields: ['follow_date']
        },

        // TikTok Live
        'TikTok Live.Go Live History.GoLiveList': {
            table: 'live_sessions',
            isArray: true,
            columns: {
                'LiveStartTime': 'live_start_time',  // DATE FIELD
                'RoomId': 'room_id',
                'CoverUri': 'cover_uri',
                'ReplayUrl': 'replay_url',
                'TotalEarning': 'total_earning',
                'LiveEndTime': 'live_end_time',      // DATE FIELD
                'TotalLike': 'total_likes',          // NUMERIC FIELD
                'TotalView': 'total_views',          // NUMERIC FIELD
                'QualitySetting': 'quality_setting',
                'RoomTitle': 'room_title'
            },
            dateFields: ['live_start_time', 'live_end_time'],
            numericFields: ['total_likes', 'total_views']
        },

        'TikTok Live.Watch Live History.WatchLiveMap': {
            table: 'watched_lives',
            isDynamic: true,
            dynamicKeyColumn: 'room_id',
            columns: {
                'WatchTime': 'watch_time',        // DATE FIELD
                'Link': 'live_link'
            },
            dateFields: ['watch_time']
        },

        'TikTok Live.Watch Live History.WatchLiveMap.*.Comments': {
            table: 'live_comments',
            isNestedArray: true,
            parentKey: 'room_id',
            columns: {
                'CommentTime': 'comment_time',    // DATE FIELD
                'CommentContent': 'comment_content',
                'RawTime': 'raw_time'             // Unix timestamp (INTEGER)
            },
            dateFields: ['comment_time'],
            integerFields: ['raw_time']
        },

        // Activity
        'Your Activity.Searches.SearchList': {
            table: 'searches',
            isArray: true,
            columns: {
                'Date': 'search_date',            // DATE FIELD
                'SearchTerm': 'search_term'
            },
            dateFields: ['search_date']
        },

        'Your Activity.Login History.LoginHistoryList': {
            table: 'login_history',
            isArray: true,
            columns: {
                'Date': 'login_date',             // DATE FIELD
                'IP': 'ip_address',
                'DeviceModel': 'device_model',
                'DeviceSystem': 'device_system',
                'NetworkType': 'network_type',
                'Carrier': 'carrier'
            },
            dateFields: ['login_date']
        },

        'Your Activity.Hashtag.HashtagList': {
            table: 'user_hashtags',
            isArray: true,
            columns: {
                'HashtagName': 'hashtag_name',
                'HashtagLink': 'hashtag_link'
            }
            // No date field
        },

        'Your Activity.Reposts.RepostList': {
            table: 'reposts',
            isArray: true,
            columns: {
                'Date': 'repost_date',            // DATE FIELD
                'Link': 'video_link'
            },
            dateFields: ['repost_date']
        },

        'Your Activity.Share History.ShareHistoryList': {
            table: 'share_history',
            isArray: true,
            columns: {
                'Date': 'share_date',             // DATE FIELD
                'SharedContent': 'shared_content',
                'Link': 'shared_link',
                'Method': 'share_method'
            },
            dateFields: ['share_date']
        },

        'Your Activity.Purchases.SendGifts.SendGifts': {
            table: 'sent_gifts',
            isArray: true,
            columns: {
                'Date': 'send_date',              // DATE FIELD
                'GiftAmount': 'gift_amount',      // NUMERIC FIELD
                'UserName': 'recipient_username'
            },
            dateFields: ['send_date'],
            numericFields: ['gift_amount']
        },

        'Your Activity.Purchases.BuyGifts.BuyGifts': {
            table: 'purchased_gifts',
            isArray: true,
            columns: {
                'Date': 'purchase_date',          // DATE FIELD
                'Price': 'price'                  // NUMERIC FIELD
            },
            dateFields: ['purchase_date'],
            numericFields: ['price']
        },

        // TikTok Shop
        'TikTok Shop.Product Browsing History.ProductBrowsingHistories': {
            table: 'product_browsing',
            isArray: true,
            columns: {
                'browsing_date': 'browsing_date',  // DATE FIELD
                'shop_name': 'shop_name',
                'product_name': 'product_name'
            },
            dateFields: ['browsing_date']
        }
    };

    // Helper function to get nested value from object using path
    function getValueByPath(obj, path) {
        if (!obj || !path) return null;
        
        const parts = path.split('.');
        let current = obj;
        
        for (let i = 0; i < parts.length; i++) {
            const part = parts[i];
            if (current === null || current === undefined) {
                return null;
            }
            
            // Handle wildcard
            if (part === '*') {
                return current; // Return the object to handle at higher level
            }
            
            // Handle special characters in keys
            if (part.includes('+') || part.includes(' ')) {
                // Try to find the exact key
                const exactKey = Object.keys(current).find(key => 
                    key === part || 
                    key.replace(/[^a-zA-Z0-9]/g, '') === part.replace(/[^a-zA-Z0-9]/g, '')
                );
                
                if (exactKey) {
                    current = current[exactKey];
                } else {
                    return null;
                }
            } else if (current[part] !== undefined) {
                current = current[part];
            } else {
                return null;
            }
        }
        
        return current;
    }

    // Check if a field name indicates a date/time field
    function isDateField(fieldName) {
        if (!fieldName) return false;
        
        const lowerField = fieldName.toLowerCase();
        
        // First check if it's explicitly a non-date field
        if (NON_DATE_FIELDS.includes(fieldName) || NON_DATE_FIELDS.some(nonDate => 
            lowerField.includes(nonDate.toLowerCase()))) {
            return false;
        }
        
        // Check special cases first
        if (SPECIAL_DATE_FIELDS[fieldName]) {
            return fieldName !== 'RawTime'; // RawTime is integer, not date string
        }
        
        // Check if it's a numeric field (should not be date)
        for (const pattern of NUMERIC_FIELD_PATTERNS) {
            if (pattern.test(fieldName)) {
                return false;
            }
        }
        
        // Check date patterns
        for (const pattern of DATE_FIELD_PATTERNS) {
            if (pattern.test(fieldName)) {
                return true;
            }
        }
        
        return false;
    }

    // Check if a field should be numeric
    function isNumericField(fieldName, mappingConfig = {}) {
        if (!fieldName) return false;
        
        const lowerField = fieldName.toLowerCase();
        
        // Check mapping config numericFields first
        if (mappingConfig.numericFields && mappingConfig.numericFields.includes(fieldName)) {
            return true;
        }
        
        // Check patterns
        for (const pattern of NUMERIC_FIELD_PATTERNS) {
            if (pattern.test(fieldName)) {
                return true;
            }
        }
        
        // Common numeric field names
        const numericFieldNames = ['count', 'amount', 'number', 'total', 'quantity', 
                                   'price', 'cost', 'value', 'size', 'age', 'percent',
                                   'percentage', 'ratio', 'frequency', 'rate', 'duration',
                                   'likes', 'views', 'followers', 'following', 'coins'];
        
        return numericFieldNames.some(name => lowerField.includes(name));
    }

    // Parse date string to SQLite format
    function parseDate(value) {
        if (!value || value === '' || value === 'N/A' || value === 'null' || value === 'NULL') {
            return null;
        }
        
        // Handle birthDate format "01-Feb-1982" or "Feb-01-1982"
        if (typeof value === 'string') {
            // Try pattern "01-Feb-1982"
            const match1 = value.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
            if (match1) {
                try {
                    const day = match1[1].padStart(2, '0');
                    const month = match1[2];
                    const year = match1[3];
                    
                    // Convert month name to number
                    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                    const monthIndex = monthNames.findIndex(m => 
                        m.toLowerCase() === month.toLowerCase().substring(0, 3));
                    
                    if (monthIndex !== -1) {
                        const monthNum = (monthIndex + 1).toString().padStart(2, '0');
                        return `${year}-${monthNum}-${day}`;
                    }
                } catch (e) {
                    console.warn('Failed to parse birth date:', value, e);
                    return value;
                }
            }
            
            // Try pattern "Feb-01-1982"
            const match2 = value.match(/^([A-Za-z]{3})-(\d{1,2})-(\d{4})$/);
            if (match2) {
                try {
                    const month = match2[1];
                    const day = match2[2].padStart(2, '0');
                    const year = match2[3];
                    
                    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                    const monthIndex = monthNames.findIndex(m => 
                        m.toLowerCase() === month.toLowerCase().substring(0, 3));
                    
                    if (monthIndex !== -1) {
                        const monthNum = (monthIndex + 1).toString().padStart(2, '0');
                        return `${year}-${monthNum}-${day}`;
                    }
                } catch (e) {
                    console.warn('Failed to parse birth date:', value, e);
                    return value;
                }
            }
        }
        
        // Handle standard "YYYY-MM-DD HH:MM:SS" format
        if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
            return value; // Already in SQLite-friendly format
        }
        
        // Handle "YYYY-MM-DD" format
        if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}$/)) {
            return value;
        }
        
        // Handle ISO format "YYYY-MM-DDTHH:MM:SS"
        if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
            // Convert to SQLite format
            return value.replace('T', ' ').split('.')[0];
        }
        
        // Try to parse as Date object
        try {
            const date = new Date(value);
            if (!isNaN(date.getTime())) {
                // Format as YYYY-MM-DD HH:MM:SS
                const year = date.getFullYear();
                const month = (date.getMonth() + 1).toString().padStart(2, '0');
                const day = date.getDate().toString().padStart(2, '0');
                const hours = date.getHours().toString().padStart(2, '0');
                const minutes = date.getMinutes().toString().padStart(2, '0');
                const seconds = date.getSeconds().toString().padStart(2, '0');
                
                return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
            }
        } catch (e) {
            // Not a parseable date
        }
        
        // Return as-is for non-date values
        return value;
    }

    // Clean dynamic keys (remove prefixes)
    function cleanKey(key) {
        return key
            .replace(/Chat History with /g, '')
            .replace(/Group Chat with /g, '')
            .replace(/:/g, '')
            .trim();
    }

    // Extract a single row of data with proper type handling
    function extractRow(source, columnMapping, mappingConfig = {}) {
        const row = {};
        const dateFields = mappingConfig.dateFields || [];
        const integerFields = mappingConfig.integerFields || [];
        const numericFields = mappingConfig.numericFields || [];
        
        for (const sourceKey in columnMapping) {
            if (columnMapping.hasOwnProperty(sourceKey)) {
                const targetColumn = columnMapping[sourceKey];
                let value = source[sourceKey];
                
                if (value !== undefined && value !== null && value !== '') {
                    // Convert "N/A" to empty string
                    if (value === 'N/A' || value === 'null' || value === 'NULL') {
                        value = '';
                    }
                    
                    // Handle special fields
                    if (sourceKey === 'RawTime') {
                        // Parse Unix timestamp to integer
                        const num = parseInt(value);
                        if (!isNaN(num)) {
                            value = num;
                        }
                    } 
                    // Check if it's a numeric field
                    else if (numericFields.includes(targetColumn) || isNumericField(sourceKey, mappingConfig)) {
                        // Parse numeric fields
                        const num = parseFloat(value);
                        if (!isNaN(num) && !isNaN(parseFloat(value))) {
                            value = num;
                        }
                    }
                    // Check if it's a date field
                    else if (dateFields.includes(targetColumn) || isDateField(sourceKey)) {
                        // Parse date fields
                        value = parseDate(value);
                    }
                    // Handle strings that might be numbers
                    else if (typeof value === 'string') {
                        // Only parse as number if it looks like a number and isn't explicitly text
                        if (value.match(/^-?\d+(\.\d+)?$/) && 
                            !value.includes(' ') && 
                            !isDateField(sourceKey)) {
                            const num = parseFloat(value);
                            if (!isNaN(num)) {
                                value = num;
                            }
                        }
                    }
                    
                    row[targetColumn] = value;
                }
            }
        }
        
        return row;
    }

    // Extract data using mapping with date handling
    function extractDataByMapping(data, mapping, path, options = {}) {
        const extracted = [];
        
        if (!mapping || !data) return extracted;
        
        try {
            if (mapping.isArray) {
                const arrayData = getValueByPath(data, path);
                if (Array.isArray(arrayData)) {
                    const chunkSize = options.chunkSize || 1000;
                    const total = arrayData.length;
                    
                    for (let start = 0; start < total; start += chunkSize) {
                        const end = Math.min(start + chunkSize, total);
                        for (let i = start; i < end; i++) {
                            const row = extractRow(arrayData[i], mapping.columns, {
                                dateFields: mapping.dateFields,
                                integerFields: mapping.integerFields,
                                numericFields: mapping.numericFields
                            });
                            if (row && Object.keys(row).length > 0) {
                                extracted.push(row);
                            }
                        }
                    }
                }
            } else if (mapping.isDynamic) {
                const dynamicData = getValueByPath(data, path);
                if (dynamicData && typeof dynamicData === 'object') {
                    for (const key in dynamicData) {
                        if (Array.isArray(dynamicData[key])) {
                            for (let i = 0; i < dynamicData[key].length; i++) {
                                const row = extractRow(dynamicData[key][i], mapping.columns, {
                                    dateFields: mapping.dateFields,
                                    integerFields: mapping.integerFields,
                                    numericFields: mapping.numericFields
                                });
                                if (row && Object.keys(row).length > 0) {
                                    row[mapping.dynamicKeyColumn] = cleanKey(key);
                                    extracted.push(row);
                                }
                            }
                        } else if (mapping.isNestedArray && dynamicData[key]) {
                            // Handle nested arrays like Comments
                            const comments = dynamicData[key].Comments;
                            if (Array.isArray(comments)) {
                                for (let i = 0; i < comments.length; i++) {
                                    const row = extractRow(comments[i], mapping.columns, {
                                        dateFields: mapping.dateFields,
                                        integerFields: mapping.integerFields,
                                        numericFields: mapping.numericFields
                                    });
                                    if (row && Object.keys(row).length > 0) {
                                        row[mapping.parentKey] = key;
                                        extracted.push(row);
                                    }
                                }
                            }
                        }
                    }
                }
            } else if (mapping.columns) {
                const objectData = getValueByPath(data, path);
                if (objectData && typeof objectData === 'object') {
                    const row = extractRow(objectData, mapping.columns, {
                        dateFields: mapping.dateFields,
                        integerFields: mapping.integerFields,
                        numericFields: mapping.numericFields
                    });
                    if (row && Object.keys(row).length > 0) {
                        extracted.push(row);
                    }
                }
            }
        } catch (error) {
            console.warn('Error extracting data for path', path, error);
        }
        
        return extracted;
    }

    // Get SQL column type for a value
    function getColumnType(columnName, value, mappingConfig = {}) {
        // Check if it's in dateFields array
        const dateFields = mappingConfig.dateFields || [];
        const integerFields = mappingConfig.integerFields || [];
        const numericFields = mappingConfig.numericFields || [];
        
        if (dateFields.includes(columnName) || isDateField(columnName)) {
            return 'TIMESTAMP';
        }
        
        if (integerFields.includes(columnName)) {
            return 'INTEGER';
        }
        
        if (numericFields.includes(columnName) || isNumericField(columnName, mappingConfig)) {
            return 'INTEGER';
        }
        
        if (value === null || value === undefined) {
            return 'TEXT'; // Default
        }
        
        if (typeof value === 'number') {
            if (Number.isInteger(value)) {
                return 'INTEGER';
            } else {
                return 'REAL';
            }
        }
        
        if (typeof value === 'boolean') {
            return 'INTEGER';
        }
        
        return 'TEXT';
    }

    // Get all mappings
    function getAllMappings() {
        return JSON_PATH_MAPPING;
    }

    // Get mapping for a specific path
    function getMapping(path) {
        return JSON_PATH_MAPPING[path];
    }

    // Estimate total items for progress tracking
    function estimateTotalItems(jsonData) {
        let total = 0;
        
        for (const path in JSON_PATH_MAPPING) {
            if (JSON_PATH_MAPPING.hasOwnProperty(path)) {
                const mapping = JSON_PATH_MAPPING[path];
                const data = getValueByPath(jsonData, path);
                
                if (data) {
                    if (mapping.isArray && Array.isArray(data)) {
                        total += data.length;
                    } else if (mapping.isDynamic && typeof data === 'object') {
                        for (const key in data) {
                            if (Array.isArray(data[key])) {
                                total += data[key].length;
                            } else if (mapping.isNestedArray && data[key] && data[key].Comments) {
                                const comments = data[key].Comments;
                                if (Array.isArray(comments)) {
                                    total += comments.length;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return total;
    }

    // Get username specifically from JSON
    function extractUsername(jsonData) {
        const profilePath = 'Profile And Settings.Profile Info.ProfileMap';
        const profileData = getValueByPath(jsonData, profilePath);
        
        if (profileData && profileData.userName) {
            return profileData.userName;
        }
        
        // Try displayName as fallback
        if (profileData && profileData.displayName) {
            return profileData.displayName;
        }
        
        return null;
    }

    // Get all date fields for a specific table
    function getDateFieldsForTable(tableName) {
        for (const path in JSON_PATH_MAPPING) {
            const mapping = JSON_PATH_MAPPING[path];
            if (mapping.table === tableName && mapping.dateFields) {
                return mapping.dateFields;
            }
        }
        return [];
    }

    // Get all numeric fields for a specific table
    function getNumericFieldsForTable(tableName) {
        for (const path in JSON_PATH_MAPPING) {
            const mapping = JSON_PATH_MAPPING[path];
            if (mapping.table === tableName && mapping.numericFields) {
                return mapping.numericFields;
            }
        }
        return [];
    }

    // Public API
    return {
        getMapping: getMapping,
        getAllMappings: getAllMappings,
        getValueByPath: getValueByPath,
        extractDataByMapping: extractDataByMapping,
        extractRow: extractRow,
        getColumnType: getColumnType,
        isDateField: isDateField,
        isNumericField: isNumericField,
        parseDate: parseDate,
        estimateTotalItems: estimateTotalItems,
        extractUsername: extractUsername,
        getDateFieldsForTable: getDateFieldsForTable,
        getNumericFieldsForTable: getNumericFieldsForTable,
        cleanKey: cleanKey
    };
})();