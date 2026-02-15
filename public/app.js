// Data-source-aware freshness thresholds
// WESENSE sensors report every 5 minutes, Meshtastic sensors report every 30-60 minutes
const FRESHNESS_THRESHOLDS = {
    'WESENSE': 10 * 60 * 1000,              // 10 minutes
    'MESHTASTIC_PUBLIC': 61 * 60 * 1000,    // 61 minutes (catches 60-min reporters)
    'MESHTASTIC_COMMUNITY': 61 * 60 * 1000, // 61 minutes
    'default': 10 * 60 * 1000               // Conservative default
};

// Helper to get freshness threshold for a given data source
function getFreshnessThreshold(dataSource) {
    return FRESHNESS_THRESHOLDS[dataSource] || FRESHNESS_THRESHOLDS.default;
}

// Extract a clean display name from a sensor/room name
// Removes common prefixes and cleans up formatting
function extractRoomDisplayName(name) {
    if (!name) return 'Unknown Room';

    // Common prefixes to strip (case-insensitive)
    const prefixPatterns = [
        /^getalife[_\-\s]*/i,
        /^wesense[_\-\s]*/i,
        /^sensor[_\-\s]*/i,
        /^room[_\-\s]*/i,
        /^my[_\-\s]*/i,
        /^home[_\-\s]*/i,
    ];

    let cleanName = name;
    for (const pattern of prefixPatterns) {
        cleanName = cleanName.replace(pattern, '');
    }

    // If we stripped everything, use original
    if (!cleanName.trim()) {
        cleanName = name;
    }

    // Replace underscores and hyphens with spaces
    cleanName = cleanName.replace(/[_\-]+/g, ' ');

    // Capitalize first letter of each word
    cleanName = cleanName.trim().split(/\s+/).map(word =>
        word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    ).join(' ');

    // Handle common abbreviations that should stay uppercase
    cleanName = cleanName
        .replace(/\bWc\b/g, 'WC')
        .replace(/\bTv\b/g, 'TV')
        .replace(/\bHvac\b/g, 'HVAC');

    return cleanName || 'Unknown Room';
}

// Detect room type from sensor/room name for icon display
// Returns a room type key matching roomTypeIcons
function detectRoomType(name) {
    if (!name) return 'unknown';
    const lowerName = name.toLowerCase();

    // Patterns in priority order (more specific first)
    const patterns = [
        // Specific bedroom variants first
        { type: 'master-bedroom', regex: /master\s*(bed|bedroom)|principal/ },
        { type: 'guest-bedroom', regex: /guest\s*(bed|bedroom|room)|spare\s*(bed|room)|hu[eé]sped/ },
        { type: 'nursery', regex: /nursery|baby\s*room|beb[eé]/ },
        // Generic bedroom after specific variants
        { type: 'bedroom', regex: /bed|dormitorio|habitaci[oó]n|rec[aá]mara|schlafzimmer/ },

        // Living spaces
        { type: 'living-room', regex: /living|lounge|sala|family\s*room|den|wohnzimmer/ },
        { type: 'dining-room', regex: /dining|comedor|esszimmer/ },

        // Kitchen
        { type: 'kitchen', regex: /kitchen|cocina|k[uü]che/ },

        // Bathroom variants
        { type: 'bathroom', regex: /bath|shower|loo|wc|ba[nñ]o|powder|toilette|badezimmer/ },

        // Work spaces
        { type: 'office', regex: /office|study|despacho|librar|escritorio|arbeitszimmer|b[uü]ro/ },
        { type: 'server-room', regex: /server|rack|network|data\s*center/ },

        // Utility spaces
        { type: 'laundry', regex: /laundry|utility|mudroom|lavander[ií]a|waschk[uü]che/ },
        { type: 'garage', regex: /garage|carport|garaje/ },
        { type: 'basement', regex: /basement|cellar|s[oó]tano|keller/ },
        { type: 'attic', regex: /attic|loft|[aá]tico|dachboden/ },
        { type: 'shed', regex: /shed|workshop|taller|werkstatt/ },

        // Transitional spaces
        { type: 'hallway', regex: /hall|foyer|entry|landing|corridor|pasillo|vestibulo|flur/ },

        // Semi-outdoor
        { type: 'patio', regex: /patio|balcon|deck|sunroom|conservator|terraza|veranda/ },

        // Activity rooms
        { type: 'gym', regex: /gym|exercise|workout|gimnasio|fitness/ }
    ];

    for (const { type, regex } of patterns) {
        if (regex.test(lowerName)) return type;
    }

    return 'unknown';
}

// Detect outdoor area type from sensor/area name for icon display
// Returns an area type key matching areaTypeIcons
function detectAreaType(name) {
    if (!name) return 'unknown';
    const lowerName = name.toLowerCase();

    // Patterns in priority order (more specific first)
    const patterns = [
        // Specific outdoor areas
        { type: 'balcony', regex: /balcon|balcón|balkon/ },
        { type: 'rooftop', regex: /roof|rooftop|azotea|techo|dach/ },
        { type: 'driveway', regex: /driveway|drive\s*way|entrada|auffahrt/ },
        { type: 'paddock', regex: /paddock|field|pasture|meadow|potrero|campo|feld|wiese/ },
        { type: 'pool', regex: /pool|piscina|schwimmbad|swimming/ },
        { type: 'greenhouse', regex: /greenhouse|green\s*house|invernadero|gew[äa]chshaus/ },
        { type: 'carport', regex: /carport|car\s*port/ },
        { type: 'pergola', regex: /pergola|p[ée]rgola/ },
        { type: 'courtyard', regex: /courtyard|court\s*yard|patio\s*interior|innenhof/ },
        { type: 'terrace', regex: /terrace|terraza|terrasse/ },
        { type: 'veranda', regex: /veranda|verandah/ },
        { type: 'porch', regex: /porch|porche|veranda|entrada/ },
        { type: 'deck', regex: /deck|decking/ },
        // Generic outdoor
        { type: 'yard', regex: /yard|garden|jard[ií]n|garten|backyard|front\s*yard|back\s*yard/ },
    ];

    for (const { type, regex } of patterns) {
        if (regex.test(lowerName)) return type;
    }

    return 'unknown';
}

// FogEffect - Ambient fog/mist particle system for weather hero
class FogEffect {
    constructor(canvasId) {
        this.canvas = document.getElementById(canvasId);
        if (!this.canvas) return;

        this.ctx = this.canvas.getContext('2d');
        this.particles = [];
        this.humidity = 50;
        this.isRunning = false;
        this.animationId = null;

        // Check for reduced motion preference
        this.prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

        // Resize handler
        this.resizeCanvas();
        window.addEventListener('resize', () => this.resizeCanvas());

        // Visibility handler - pause when tab not visible
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.stop();
            } else if (this.humidity >= 70) {
                this.start();
            }
        });
    }

    resizeCanvas() {
        if (!this.canvas) return;
        const rect = this.canvas.parentElement.getBoundingClientRect();
        this.canvas.width = rect.width;
        this.canvas.height = rect.height;
    }

    setHumidity(humidity) {
        this.humidity = humidity || 50;
        const heroEl = document.getElementById('weatherHero');

        if (humidity >= 85) {
            heroEl?.setAttribute('data-humidity-level', 'very-high');
            if (!this.isRunning && !this.prefersReducedMotion) this.start();
        } else if (humidity >= 70) {
            heroEl?.setAttribute('data-humidity-level', 'high');
            if (!this.isRunning && !this.prefersReducedMotion) this.start();
        } else {
            heroEl?.removeAttribute('data-humidity-level');
            this.stop();
        }

        // Adjust particle count based on humidity
        this.updateParticleCount();
    }

    updateParticleCount() {
        if (this.humidity < 70) {
            this.particles = [];
            return;
        }

        // More particles at higher humidity
        const targetCount = Math.floor((this.humidity - 60) / 3);
        const maxParticles = Math.min(targetCount, 25);

        while (this.particles.length < maxParticles) {
            this.particles.push(this.createParticle());
        }
        while (this.particles.length > maxParticles) {
            this.particles.pop();
        }
    }

    createParticle() {
        return {
            x: Math.random() * this.canvas.width,
            y: Math.random() * this.canvas.height,
            radius: Math.random() * 60 + 40,
            vx: (Math.random() - 0.5) * 0.3,
            vy: (Math.random() - 0.5) * 0.1,
            opacity: Math.random() * 0.08 + 0.02
        };
    }

    start() {
        if (this.isRunning || this.prefersReducedMotion || !this.canvas) return;
        this.isRunning = true;
        this.animate();
    }

    stop() {
        this.isRunning = false;
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
            this.animationId = null;
        }
        // Clear canvas
        if (this.ctx) {
            this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        }
    }

    animate() {
        if (!this.isRunning) return;

        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

        // Draw fog particles
        for (const particle of this.particles) {
            // Move particle
            particle.x += particle.vx;
            particle.y += particle.vy;

            // Wrap around edges
            if (particle.x < -particle.radius) particle.x = this.canvas.width + particle.radius;
            if (particle.x > this.canvas.width + particle.radius) particle.x = -particle.radius;
            if (particle.y < -particle.radius) particle.y = this.canvas.height + particle.radius;
            if (particle.y > this.canvas.height + particle.radius) particle.y = -particle.radius;

            // Draw soft circular gradient
            const gradient = this.ctx.createRadialGradient(
                particle.x, particle.y, 0,
                particle.x, particle.y, particle.radius
            );
            gradient.addColorStop(0, `rgba(255, 255, 255, ${particle.opacity})`);
            gradient.addColorStop(1, 'rgba(255, 255, 255, 0)');

            this.ctx.fillStyle = gradient;
            this.ctx.beginPath();
            this.ctx.arc(particle.x, particle.y, particle.radius, 0, Math.PI * 2);
            this.ctx.fill();
        }

        // Throttle to ~30fps for performance
        this.animationId = setTimeout(() => {
            requestAnimationFrame(() => this.animate());
        }, 33);
    }
}

// RainEffect - Animated rain drops for weather hero when rain is likely
class RainEffect {
    constructor(canvasId) {
        this.canvas = document.getElementById(canvasId);
        if (!this.canvas) return;

        this.ctx = this.canvas.getContext('2d');
        this.drops = [];
        this.isRaining = false;
        this.intensity = 0; // 0-1 scale
        this.isRunning = false;
        this.animationId = null;

        this.prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

        // Visibility handler
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.stop();
            } else if (this.isRaining) {
                this.start();
            }
        });
    }

    setRainConditions(pressureTrend, humidity) {
        // Determine if rain is likely based on conditions
        const isFalling = pressureTrend && (
            pressureTrend.includes('Falling') ||
            pressureTrend.includes('Storm')
        );
        const isRapid = pressureTrend && (
            pressureTrend.includes('Rapidly') ||
            pressureTrend.includes('Storm')
        );
        const isHumid = humidity >= 75;

        // Set rain intensity
        if (isRapid && isHumid) {
            this.intensity = 0.8;
            this.isRaining = true;
        } else if (isFalling && isHumid) {
            this.intensity = 0.4;
            this.isRaining = true;
        } else if (isFalling && humidity >= 65) {
            this.intensity = 0.2;
            this.isRaining = true;
        } else {
            this.isRaining = false;
            this.intensity = 0;
        }

        // Update hero data attribute
        const heroEl = document.getElementById('weatherHero');
        if (heroEl) {
            if (this.isRaining) {
                heroEl.setAttribute('data-weather', 'rain');
            } else {
                heroEl.removeAttribute('data-weather');
            }
        }

        if (this.isRaining && !this.prefersReducedMotion) {
            this.updateDropCount();
            this.start();
        } else {
            this.stop();
        }
    }

    updateDropCount() {
        const targetCount = Math.floor(this.intensity * 60) + 10;

        while (this.drops.length < targetCount) {
            this.drops.push(this.createDrop());
        }
        while (this.drops.length > targetCount) {
            this.drops.pop();
        }
    }

    createDrop() {
        return {
            x: Math.random() * this.canvas.width,
            y: Math.random() * this.canvas.height - this.canvas.height,
            length: Math.random() * 15 + 10,
            speed: Math.random() * 4 + 8,
            opacity: Math.random() * 0.3 + 0.2
        };
    }

    start() {
        if (this.isRunning || this.prefersReducedMotion || !this.canvas) return;
        this.isRunning = true;
        this.animate();
    }

    stop() {
        this.isRunning = false;
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
            this.animationId = null;
        }
        this.drops = [];
        if (this.ctx) {
            this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        }
    }

    animate() {
        if (!this.isRunning) return;

        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

        // Draw rain drops
        for (const drop of this.drops) {
            // Move drop
            drop.y += drop.speed;

            // Reset if off screen
            if (drop.y > this.canvas.height) {
                drop.y = -drop.length;
                drop.x = Math.random() * this.canvas.width;
            }

            // Draw drop as a line
            this.ctx.beginPath();
            this.ctx.moveTo(drop.x, drop.y);
            this.ctx.lineTo(drop.x - 1, drop.y + drop.length);
            this.ctx.strokeStyle = `rgba(150, 200, 255, ${drop.opacity})`;
            this.ctx.lineWidth = 1;
            this.ctx.stroke();
        }

        this.animationId = requestAnimationFrame(() => this.animate());
    }
}

// InsightEngine - Generates actionable insights from sensor data
class InsightEngine {
    constructor() {
        // Look for the left container first, fall back to the strip itself
        this.container = document.querySelector('#insightStrip .insight-strip-left')
                      || document.getElementById('insightStrip');
        this.currentInsights = new Map(); // Track active insights by ID
        this.conditions = {}; // Store current conditions
    }

    updateConditions(conditions) {
        this.conditions = { ...this.conditions, ...conditions };
        this.generateInsights();
    }

    generateInsights() {
        const newInsights = [];
        const { temperature, humidity, pressure, pressureTrend, co2, pm25, aqi, iaqi } = this.conditions;

        // Frost warning
        if (temperature !== null && temperature !== undefined && temperature <= 2) {
            newInsights.push({
                id: 'frost',
                level: temperature <= 0 ? 'critical' : 'warning',
                icon: '*', // Snowflake
                text: temperature <= 0 ? 'Freezing conditions' : 'Frost possible tonight'
            });
        }

        // Heat warning
        if (temperature !== null && temperature !== undefined && temperature >= 35) {
            newInsights.push({
                id: 'heat',
                level: temperature >= 40 ? 'critical' : 'warning',
                icon: '!',
                text: temperature >= 40 ? 'Extreme heat warning' : 'High temperature alert'
            });
        }

        // Rain likely
        if (pressureTrend && humidity) {
            const isFalling = pressureTrend.includes('Falling') || pressureTrend.includes('Storm');
            if (isFalling && humidity >= 75) {
                newInsights.push({
                    id: 'rain',
                    level: pressureTrend.includes('Rapidly') ? 'warning' : 'info',
                    icon: '~',
                    text: pressureTrend.includes('Rapidly') ? 'Rain likely soon' : 'Rain possible'
                });
            }
        }

        // Storm warning
        if (pressureTrend && pressureTrend.includes('Storm')) {
            newInsights.push({
                id: 'storm',
                level: 'critical',
                icon: '!',
                text: 'Storm warning - rapid pressure drop'
            });
        }

        // High humidity
        if (humidity !== null && humidity !== undefined && humidity >= 85) {
            newInsights.push({
                id: 'humidity',
                level: 'info',
                icon: '%',
                text: 'Very high humidity'
            });
        }

        // CO2 ventilation
        if (co2 !== null && co2 !== undefined) {
            if (co2 >= 2000) {
                newInsights.push({
                    id: 'co2',
                    level: 'critical',
                    icon: '!',
                    text: 'Ventilate immediately - CO2 very high'
                });
            } else if (co2 >= 1000) {
                newInsights.push({
                    id: 'co2',
                    level: 'warning',
                    icon: '>',
                    text: 'Consider ventilating - CO2 elevated'
                });
            }
        }

        // Poor outdoor air quality
        if (aqi !== null && aqi !== undefined) {
            if (aqi >= 150) {
                newInsights.push({
                    id: 'aqi',
                    level: 'critical',
                    icon: '!',
                    text: 'Unhealthy air quality - limit outdoor activity'
                });
            } else if (aqi >= 100) {
                newInsights.push({
                    id: 'aqi',
                    level: 'warning',
                    icon: '!',
                    text: 'Moderate air quality - sensitive groups take care'
                });
            }
        }

        // Poor indoor air quality
        if (iaqi !== null && iaqi !== undefined && iaqi >= 100) {
            newInsights.push({
                id: 'iaqi',
                level: 'warning',
                icon: '>',
                text: 'Indoor air quality degraded'
            });
        }

        // Swarm insights
        const swarmInsight = this.getSwarmInsight();
        if (swarmInsight) {
            newInsights.push(swarmInsight);
        }

        // Photography tips - only show one at a time to avoid clutter
        const photoTip = this.getPhotographyTip();
        if (photoTip) {
            newInsights.push(photoTip);
        }

        // Update display
        this.updateDisplay(newInsights);
    }

    getSwarmInsight() {
        const { swarm, swarmStatus, outlierCount, temperature } = this.conditions;

        if (!swarmStatus) return null;

        const swarmIcon = '\u25C9'; // Circle with dot (broadcast symbol)

        // Priority 1: Outlier warning - sensor might need attention
        if (outlierCount > 0) {
            return {
                id: 'swarm-outlier',
                level: 'warning',
                icon: '!',
                text: `${outlierCount} sensor${outlierCount > 1 ? 's' : ''} reading outside swarm range - check placement`
            };
        }

        // Priority 2: Significant deviation from swarm (if verified)
        if (swarm?.temperature?.available && swarm.temperature.median != null && swarm.temperature.myValue != null) {
            const deviation = Math.abs(swarm.temperature.myValue - swarm.temperature.median);
            if (deviation >= 2) {
                const direction = swarm.temperature.myValue > swarm.temperature.median ? 'warmer' : 'cooler';
                return {
                    id: 'swarm-deviation',
                    level: 'info',
                    icon: swarmIcon,
                    text: `Your sensors read ${deviation.toFixed(1)}° ${direction} than swarm average`
                };
            }
        }

        // Priority 3: Super swarm celebration
        if (swarmStatus.totalSize >= 7 && swarmStatus.allVerified) {
            return {
                id: 'swarm-super',
                level: 'success',
                icon: swarmIcon,
                text: `Super Swarm active - ${swarmStatus.totalSize} sensors providing high-confidence readings`
            };
        }

        // Priority 4: Peer verified status
        if (swarmStatus.hasSwarm && swarmStatus.totalSize >= 5) {
            return {
                id: 'swarm-verified',
                level: 'info',
                icon: swarmIcon,
                text: `Peer Verified - ${swarmStatus.mySensorCount} of yours + ${swarmStatus.swarmPeerCount} community sensors`
            };
        }

        // Priority 5: Almost there - encourage growth
        if (swarmStatus.mySensorCount > 0 && swarmStatus.totalSize >= 3 && swarmStatus.totalSize < 5) {
            const needed = 5 - swarmStatus.totalSize;
            return {
                id: 'swarm-grow',
                level: 'info',
                icon: swarmIcon,
                text: `${needed} more sensor${needed > 1 ? 's' : ''} needed for Peer Verified status`
            };
        }

        return null;
    }

    getPhotographyTip() {
        const { humidity, pressureTrend, sunrise, sunset } = this.conditions;
        const now = new Date();
        const hour = now.getHours();
        const minutes = now.getMinutes();
        const currentTime = hour + minutes / 60;

        // Parse sunrise/sunset times (format: "HH:MM")
        let sunriseHour = 6, sunsetHour = 18;
        if (sunrise && sunrise !== '--:--') {
            const parts = sunrise.split(':');
            sunriseHour = parseInt(parts[0]) + parseInt(parts[1]) / 60;
        }
        if (sunset && sunset !== '--:--') {
            const parts = sunset.split(':');
            sunsetHour = parseInt(parts[0]) + parseInt(parts[1]) / 60;
        }

        // Time-based conditions (golden/blue hour times are shown in astronomy bar)
        const isNight = currentTime < sunriseHour - 1 || currentTime > sunsetHour + 1;
        const isMidDay = currentTime >= 11 && currentTime <= 14;

        // Weather-based conditions
        const isStableOrRising = !pressureTrend ||
            pressureTrend.includes('Stable') ||
            pressureTrend.includes('Rising') ||
            pressureTrend.includes('High');
        const isClear = humidity !== null && humidity < 70 && isStableOrRising;
        const isOvercast = humidity !== null && humidity >= 75;
        const isFoggy = humidity !== null && humidity >= 85;

        // Priority order for photography condition tips
        // Note: Golden/blue hour TIMES are now shown persistently in the astronomy bar
        // These tips focus on current CONDITIONS that affect photography
        const photoIcon = '\u25CE'; // Bullseye/aperture symbol

        if (isNight && isClear && humidity !== null && humidity < 60) {
            return {
                id: 'photo',
                level: 'photo',
                icon: photoIcon,
                text: 'Clear skies - astrophotography conditions'
            };
        }

        if (isFoggy && !isNight) {
            return {
                id: 'photo',
                level: 'photo',
                icon: photoIcon,
                text: 'Misty conditions - atmospheric landscapes'
            };
        }

        if (isOvercast && !isFoggy && !isNight) {
            return {
                id: 'photo',
                level: 'photo',
                icon: photoIcon,
                text: 'Overcast - soft light for portraits'
            };
        }

        if (isMidDay && isClear) {
            return {
                id: 'photo',
                level: 'photo',
                icon: photoIcon,
                text: 'Harsh midday light - seek shade'
            };
        }

        return null;
    }

    updateDisplay(newInsights) {
        if (!this.container) return;

        const newIds = new Set(newInsights.map(i => i.id));
        const currentIds = new Set(this.currentInsights.keys());

        // Remove insights that are no longer active
        for (const id of currentIds) {
            if (!newIds.has(id)) {
                const element = this.currentInsights.get(id);
                if (element) {
                    element.classList.add('dismissing');
                    setTimeout(() => element.remove(), 300);
                }
                this.currentInsights.delete(id);
            }
        }

        // Add new insights
        for (const insight of newInsights) {
            if (!this.currentInsights.has(insight.id)) {
                const chip = this.createChip(insight);
                this.container.appendChild(chip);
                this.currentInsights.set(insight.id, chip);
            } else {
                // Update existing chip if level changed
                const existing = this.currentInsights.get(insight.id);
                if (existing && !existing.classList.contains(insight.level)) {
                    existing.className = `insight-chip ${insight.level}`;
                    existing.querySelector('.insight-text').textContent = insight.text;
                }
            }
        }
    }

    createChip(insight) {
        const chip = document.createElement('div');
        chip.className = `insight-chip ${insight.level}`;
        chip.innerHTML = `
            <span class="insight-icon">${insight.icon}</span>
            <span class="insight-text">${insight.text}</span>
        `;
        return chip;
    }
}

// DetailsSidebar - Handles metric detail view with historical comparison
class DetailsSidebar {
    constructor() {
        this.sidebar = document.getElementById('detailsSidebar');
        this.overlay = document.getElementById('detailsSidebarOverlay');
        this.isOpen = false;
        this.currentMetric = null;
        this.currentTimeRange = '24h';
        this.chart = null;
        this.sensorSparklines = [];
        this.comparisonChart = null;

        this.metricConfig = {
            temperature: { title: 'TEMPERATURE', unit: '°C', color: '#f97316', hasFeelsLike: true },
            humidity: { title: 'HUMIDITY', unit: '%', color: '#06b6d4', hasFeelsLike: false },
            pressure: { title: 'PRESSURE', unit: ' hPa', color: '#8b5cf6', hasFeelsLike: false },
            co2: { title: 'CO₂', unit: ' ppm', color: '#22c55e', hasFeelsLike: false },
            pm2_5: { title: 'PM2.5', unit: ' µg/m³', color: '#ef4444', hasFeelsLike: false },
            pm10: { title: 'PM10', unit: ' µg/m³', color: '#f97316', hasFeelsLike: false },
            pm1: { title: 'PM1', unit: ' µg/m³', color: '#8b5cf6', hasFeelsLike: false },
            voc_index: { title: 'VOC', unit: '', color: '#06b6d4', hasFeelsLike: false },
            nox_index: { title: 'NOx', unit: '', color: '#64748b', hasFeelsLike: false }
        };

        // Air quality metrics that can be switched between
        this.aqMetrics = ['pm2_5', 'pm10', 'pm1', 'co2', 'voc_index', 'nox_index'];
        this.isAqMode = false;
        this.aqSourceType = null; // 'indoor' or 'outdoor'

        this.setupEventListeners();

        // Handle window resize - debounced
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => this.handleResize(), 150);
        });
    }

    handleResize() {
        if (this.chart && typeof this.chart.resize === 'function') {
            this.chart.resize();
        }
        if (this.comparisonChart && typeof this.comparisonChart.resize === 'function') {
            this.comparisonChart.resize();
        }
        this.sensorSparklines.forEach(chart => {
            if (chart && typeof chart.resize === 'function') {
                chart.resize();
            }
        });
    }

    setupEventListeners() {
        // Back button closes sidebar
        document.getElementById('sidebarBackBtn')?.addEventListener('click', () => this.close());

        // Overlay click closes sidebar
        this.overlay?.addEventListener('click', () => this.close());

        // Time pills in sidebar
        document.querySelectorAll('.details-time-pills .time-pill').forEach(pill => {
            pill.addEventListener('click', (e) => {
                document.querySelectorAll('.details-time-pills .time-pill').forEach(p => p.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeRange = e.target.dataset.range;
                this.loadChartData();
            });
        });

        // Escape key closes sidebar
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });

    }

    // Switch between air quality metrics
    switchAqMetric(newMetric) {
        if (!this.aqMetrics.includes(newMetric)) return;

        this.currentMetric = newMetric;
        const config = this.metricConfig[newMetric];

        // Update header
        document.getElementById('sidebarTitle').textContent = config.title;

        // Update current value from aggregates - strict source type (no fallback)
        const aggregates = newDashboardLayout?.currentAggregates;
        const aggregateKey = newMetric === 'pm2_5' ? 'pm25' :
                            newMetric === 'pm10' ? 'pm10' :
                            newMetric === 'pm1' ? 'pm1' :
                            newMetric === 'voc_index' ? 'voc' :
                            newMetric === 'nox_index' ? 'nox' : newMetric;

        // Only use data from the correct source type - no fallback to avoid mixing indoor/outdoor
        const source = (this.aqSourceType === 'outdoor') ? aggregates?.outdoor : aggregates?.indoor;
        const avg = source?.[aggregateKey]?.avg;

        let currentValue = '--';
        if (avg != null) {
            currentValue = (newMetric === 'co2') ? Math.round(avg).toString() : avg.toFixed(1);
        }

        document.getElementById('sidebarCurrentValue').textContent = currentValue;
        document.getElementById('sidebarCurrentUnit').textContent = config.unit;

        // Update sensor data for the new metric
        const aqMetricToKey = { 'pm2_5': 'pm25', 'pm10': 'pm10', 'pm1': 'pm1', 'co2': 'co2', 'voc_index': 'voc', 'nox_index': 'nox' };
        const metricKey = aqMetricToKey[newMetric] || newMetric;

        if (this.aqSourceType === 'outdoor') {
            const outdoorSensors = aggregates?.outdoorSensors || {};
            this.sensorData = Object.values(outdoorSensors)
                .filter(s => s[metricKey] != null)
                .map(s => ({
                    id: s.deviceId,
                    name: s.name,
                    value: s[metricKey],
                    isIndoor: false,
                    boardModel: s.boardModel,
                    sensorModel: s.sensorModels?.[metricKey]
                }));
        } else {
            const rooms = aggregates?.rooms || {};
            this.sensorData = [];
            Object.values(rooms).forEach(room => {
                const sensors = Object.values(room.sensors || {});
                sensors.forEach(s => {
                    if (s[metricKey] != null) {
                        this.sensorData.push({
                            id: s.deviceId,
                            name: s.name,
                            value: s[metricKey],
                            isIndoor: true,
                            boardModel: s.boardModel,
                            sensorModel: s.sensorModels?.[metricKey]
                        });
                    }
                });
            });
        }
        this.avgValue = avg;

        // Clear comparison stats (will reload)
        this.clearComparisonStats();

        // Re-render contributing sensors for new metric
        this.renderContributingSensors();

        // Reload chart and comparison data
        this.fetchComparisonData();
        this.loadChartData();
    }

    open(metric, currentValue, data = {}) {
        this.currentMetric = metric;
        this.deviceIds = data.deviceIds || [];
        this.sensorData = data.sensors || [];
        this.avgValue = data.avg;

        // Collect swarm sensors from app.sensors (other sensors in same H3 cell)
        this.swarmSensors = [];
        if (this.sensorData.length > 0) {
            // Get the first favorited sensor's swarm_sensors list
            for (const s of this.sensorData) {
                const fullSensor = window.app?.sensors?.find(fs => fs.deviceId === (s.id || s.deviceId));
                if (fullSensor && fullSensor.swarm_sensors && fullSensor.swarm_sensors.length > 0) {
                    // Filter to only include sensors not already in sensorData
                    const sensorIds = this.sensorData.map(sd => sd.id || sd.deviceId);
                    // For temp/humidity, only include outdoor sensors in swarm peers
                    // For pressure, include all sensors (pressure is uniform regardless of building envelope)
                    const requiresOutdoor = metric === 'temperature' || metric === 'humidity';

                    this.swarmSensors = fullSensor.swarm_sensors
                        .filter(ss => !sensorIds.includes(ss.deviceId))
                        .filter(ss => !requiresOutdoor || ss.is_outdoor === true)
                        .map(ss => ({
                            ...ss,
                            isSwarmPeer: true,
                            value: ss.readings?.[metric]
                        }))
                        .filter(ss => ss.value != null);
                    break;
                }
            }
        }

        const config = this.metricConfig[metric];

        if (!config) return;

        // Update header
        document.getElementById('sidebarTitle').textContent = config.title;

        // Show initial value (will be updated with fresh data)
        document.getElementById('sidebarCurrentValue').textContent = currentValue ?? '--';
        document.getElementById('sidebarCurrentUnit').textContent = config.unit;

        // Show/hide feels like based on metric
        const feelsLikeEl = document.getElementById('sidebarFeelsLike');
        if (config.hasFeelsLike && data.feelsLike !== undefined) {
            feelsLikeEl.textContent = `Feels like ${data.feelsLike}${config.unit}`;
            feelsLikeEl.style.display = 'block';
        } else {
            feelsLikeEl.style.display = 'none';
        }

        // Hide sensor info section for now (shows aggregated data)
        const sensorInfo = document.querySelector('.details-sensor-info');
        if (sensorInfo) sensorInfo.style.display = 'none';

        // Handle air quality metric selector
        const aqSelect = document.getElementById('aqMetricSelect');
        this.isAqMode = this.aqMetrics.includes(metric);

        if (this.isAqMode && aqSelect) {
            // Determine source type based on initial metric
            this.aqSourceType = (metric === 'pm2_5' || metric === 'pm10' || metric === 'pm1') ? 'outdoor' : 'indoor';
            if (data.sourceType) this.aqSourceType = data.sourceType; // Allow override

            // Show selector and populate available options
            aqSelect.style.display = 'block';
            aqSelect.value = metric;

            // Attach change handler (remove old one first to avoid duplicates)
            aqSelect.onchange = (e) => {
                this.switchAqMetric(e.target.value);
            };

        } else if (aqSelect) {
            aqSelect.style.display = 'none';
            this.aqSourceType = null;
        }

        // Clear comparison stats initially
        this.clearComparisonStats();

        // Render contributing sensors section
        this.renderContributingSensors();

        // Show sidebar with animation
        this.sidebar.classList.add('open');
        this.overlay.classList.add('visible');
        this.isOpen = true;

        // Fetch fresh current value from API
        this.fetchCurrentValue();

        // Fetch comparison data from API
        this.fetchComparisonData();

        // Load chart data
        this.loadChartData();
    }

    async fetchCurrentValue() {
        // Only use the pre-calculated average from the dashboard
        // The aggregate history API uses time buckets which can give incorrect
        // averages when sensors report at different times within a bucket period
        if (this.avgValue == null) {
            // No accurate data available - show placeholder
            document.getElementById('sidebarCurrentValue').textContent = '--';
            return;
        }

        const config = this.metricConfig[this.currentMetric];
        const decimals = this.currentMetric === 'pressure' ? 2 : 1;

        document.getElementById('sidebarCurrentValue').textContent = this.avgValue.toFixed(decimals);

        // Update feels like for temperature
        if (this.currentMetric === 'temperature' && config.hasFeelsLike) {
            await this.updateFeelsLike();
        }
    }

    async updateFeelsLike() {
        // Use avgValue for temperature, fetch humidity to calculate feels like
        if (this.avgValue == null) return;

        const sensorIds = (this.sensorData && this.sensorData.length > 0)
            ? this.sensorData.map(s => s.id)
            : this.deviceIds;

        if (!sensorIds || sensorIds.length === 0) return;

        try {
            const humidityUrl = `/api/history/aggregate?devices=${sensorIds.join(',')}&type=humidity&range=1h`;
            const humidityResponse = await fetch(humidityUrl);
            if (humidityResponse.ok) {
                const humidityResult = await humidityResponse.json();
                const humidityData = humidityResult.data || [];
                if (humidityData.length > 0) {
                    const humidity = humidityData[humidityData.length - 1].value;
                    const feelsLike = this.calculateFeelsLike(this.avgValue, humidity);
                    const feelsLikeEl = document.getElementById('sidebarFeelsLike');
                    if (feelsLikeEl) {
                        feelsLikeEl.textContent = `Feels like ${feelsLike.toFixed(0)}°C`;
                        feelsLikeEl.style.display = 'block';
                    }
                }
            }
        } catch (error) {
            console.error('Error fetching humidity for feels like:', error);
        }
    }

    calculateFeelsLike(temp, humidity) {
        // Heat index calculation (simplified)
        if (temp >= 27 && humidity >= 40) {
            return -8.785 + 1.611 * temp + 2.339 * humidity - 0.146 * temp * humidity;
        }
        // Wind chill would need wind speed, just return temp for now
        return temp;
    }

    renderContributingSensors() {
        const container = document.getElementById('detailsContributingSensors');
        const list = document.getElementById('contributingSensorsList');
        const countEl = document.getElementById('contributingSensorsCount');
        const comparisonSection = document.getElementById('contributingSensorsComparison');
        const config = this.metricConfig[this.currentMetric];

        if (!container || !list) return;

        // Destroy existing sensor charts
        if (this.sensorSparklines) {
            this.sensorSparklines.forEach(chart => chart.destroy());
        }
        this.sensorSparklines = [];

        // Destroy comparison chart
        if (this.comparisonChart) {
            this.comparisonChart.destroy();
            this.comparisonChart = null;
        }

        // Show section only if we have sensor data
        if (!this.sensorData || this.sensorData.length === 0) {
            container.style.display = 'none';
            if (comparisonSection) comparisonSection.style.display = 'none';
            return;
        }

        container.style.display = 'block';
        list.innerHTML = '';
        countEl.textContent = this.sensorData.length;

        const decimals = this.currentMetric === 'pressure' ? 2 : 1;
        const avg = this.avgValue || 0;

        // Sort by value (highest first)
        const sortedSensors = [...this.sensorData].sort((a, b) => b.value - a.value);

        // Generate colors for each sensor
        const sensorColors = this.generateSensorColors(sortedSensors.length);

        sortedSensors.forEach((sensor, index) => {
            const diff = sensor.value - avg;
            const isAnomaly = this.currentMetric === 'pressure' && Math.abs(diff) > 3;
            const sensorColor = sensorColors[index];

            const item = document.createElement('div');
            item.className = `contributing-sensor-item${isAnomaly ? ' anomaly' : ''}`;
            item.style.setProperty('--accent-color', sensorColor);
            item.style.borderLeftColor = sensorColor;

            const diffClass = diff >= 0 ? 'positive' : 'negative';
            const diffSign = diff >= 0 ? '+' : '';
            const canvasId = `sensorSparkline_${sensor.id.replace(/[^a-zA-Z0-9]/g, '_')}`;

            // Build hardware info line
            const hwParts = [];
            if (sensor.boardModel) hwParts.push(sensor.boardModel);
            if (sensor.sensorModel) hwParts.push(sensor.sensorModel);
            const hwInfo = hwParts.length > 0 ? hwParts.join(' / ') : null;

            item.innerHTML = `
                <div class="contributing-sensor-row">
                    <div class="contributing-sensor-info">
                        <span class="contributing-sensor-name">
                            <span class="favorited-sensor-icon">⭐</span>
                            ${this.escapeHtml(sensor.name)}
                        </span>
                        <span class="contributing-sensor-type">${sensor.isIndoor ? 'Indoor' : 'Outdoor'}</span>
                        ${hwInfo ? `<span class="contributing-sensor-hw">${this.escapeHtml(hwInfo)}</span>` : ''}
                    </div>
                    <div class="contributing-sensor-value">
                        <span class="contributing-sensor-reading" style="color: ${sensorColor}">${sensor.value.toFixed(decimals)}${config.unit}</span>
                        <span class="contributing-sensor-diff ${diffClass}">${diffSign}${diff.toFixed(decimals)} from avg</span>
                    </div>
                </div>
                <div class="contributing-sensor-chart">
                    <canvas id="${canvasId}"></canvas>
                </div>
            `;
            list.appendChild(item);

            // Store sensor info for chart rendering
            sensor.canvasId = canvasId;
            sensor.color = sensorColor;
        });

        // Add swarm sensors section if we have any
        if (this.swarmSensors && this.swarmSensors.length > 0) {
            // Separate fresh and stale peers
            const freshPeers = this.swarmSensors.filter(s => s.is_fresh !== false);
            const stalePeers = this.swarmSensors.filter(s => s.is_fresh === false);

            // Add separator and swarm section header
            const swarmHeader = document.createElement('div');
            swarmHeader.className = 'swarm-sensors-header';
            const headerCount = stalePeers.length > 0
                ? `${freshPeers.length} active, ${stalePeers.length} offline`
                : `${freshPeers.length} sensor${freshPeers.length !== 1 ? 's' : ''}`;
            swarmHeader.innerHTML = `
                <span class="swarm-sensors-icon">🐝</span>
                <span class="swarm-sensors-title">Swarm Peers</span>
                <span class="swarm-sensors-count">${headerCount}</span>
            `;
            list.appendChild(swarmHeader);

            // Sort swarm sensors: fresh first, then by value
            const sortedSwarmSensors = [...this.swarmSensors].sort((a, b) => {
                // Fresh sensors first
                if (a.is_fresh !== false && b.is_fresh === false) return -1;
                if (a.is_fresh === false && b.is_fresh !== false) return 1;
                // Then by value
                return (b.value || 0) - (a.value || 0);
            });
            const swarmColors = this.generateSensorColors(sortedSwarmSensors.length);

            sortedSwarmSensors.forEach((sensor, index) => {
                const diff = (sensor.value || 0) - avg;
                const sensorColor = swarmColors[index];
                const isOutlier = sensor.is_outlier;
                const isStale = sensor.is_fresh === false;

                const item = document.createElement('div');
                item.className = `contributing-sensor-item swarm-peer${isOutlier ? ' outlier' : ''}${isStale ? ' stale' : ''}`;
                item.style.setProperty('--accent-color', sensorColor);
                item.style.borderLeftColor = isStale ? '#64748b' : '#fbbf24'; // Gray for stale, amber for fresh

                const diffClass = diff >= 0 ? 'positive' : 'negative';
                const diffSign = diff >= 0 ? '+' : '';

                // Calculate time since last reading for stale sensors
                let staleInfo = '';
                if (isStale && sensor.timestamp) {
                    const timeSince = this.formatTimeSince(sensor.timestamp);
                    staleInfo = `<span class="contributing-sensor-type stale-badge">Offline ${timeSince}</span>`;
                }

                item.innerHTML = `
                    <div class="contributing-sensor-row">
                        <div class="contributing-sensor-info">
                            <span class="contributing-sensor-name${isStale ? ' stale-text' : ''}">
                                <span class="swarm-peer-icon">${isStale ? '💤' : '🐝'}</span>
                                ${this.escapeHtml(sensor.name || sensor.deviceId)}
                            </span>
                            ${isOutlier ? '<span class="contributing-sensor-type outlier-badge">⚠️ Outlier</span>' : ''}
                            ${staleInfo}
                        </div>
                        <div class="contributing-sensor-value">
                            <span class="contributing-sensor-reading${isStale ? ' stale-text' : ''}" style="color: ${isStale ? '#64748b' : sensorColor}">${sensor.value?.toFixed(decimals) ?? '--'}${config.unit}</span>
                            ${!isStale ? `<span class="contributing-sensor-diff ${diffClass}">${diffSign}${diff.toFixed(decimals)} from avg</span>` : ''}
                        </div>
                    </div>
                `;
                list.appendChild(item);
            });

            // Update total count to include only fresh swarm sensors
            countEl.textContent = `${sortedSensors.length} + ${freshPeers.length}`;
        }

        // Fetch historical data and render charts with consistent time bounds
        const timeRangeMs = this.getTimeRangeMs(this.currentTimeRange);
        const now = Date.now();
        const timeBounds = {
            min: now - timeRangeMs,
            max: now,
            range: this.currentTimeRange
        };
        this.fetchAndRenderSensorCharts(sortedSensors, config, timeBounds);
    }

    generateSensorColors(count) {
        // Generate distinct colors for sensors
        const baseColors = [
            '#8b5cf6', // Purple
            '#3b82f6', // Blue
            '#06b6d4', // Cyan
            '#10b981', // Emerald
            '#f59e0b', // Amber
            '#ef4444', // Red
            '#ec4899', // Pink
            '#6366f1', // Indigo
        ];

        const colors = [];
        for (let i = 0; i < count; i++) {
            colors.push(baseColors[i % baseColors.length]);
        }
        return colors;
    }

    async fetchAndRenderSensorCharts(sensors, config, timeBounds) {
        const comparisonSection = document.getElementById('contributingSensorsComparison');
        const legendContainer = document.getElementById('comparisonChartLegend');

        // Fetch historical data for each sensor
        const sensorHistories = [];
        const decimals = this.currentMetric === 'pressure' ? 2 : 1;

        for (const sensor of sensors) {
            try {
                const response = await fetch(`/api/history?device_id=${sensor.id}&reading_type=${this.currentMetric}&range=${this.currentTimeRange}`);
                if (response.ok) {
                    const result = await response.json();
                    const historyData = result.historyData || result.data || [];
                    if (historyData.length > 0) {
                        sensorHistories.push({
                            sensor,
                            data: historyData
                        });

                        // Get the most recent value from history
                        const latestValue = historyData[historyData.length - 1].value;

                        // Update the displayed value to show fresh data
                        const valueEl = document.querySelector(`#${sensor.canvasId}`)?.closest('.contributing-sensor-item')?.querySelector('.contributing-sensor-reading');
                        if (valueEl) {
                            valueEl.textContent = `${latestValue.toFixed(decimals)}${config.unit}`;
                        }

                        // Render individual sparkline with consistent time bounds
                        this.renderSensorSparkline(sensor.canvasId, historyData, sensor.color, timeBounds);
                    }
                }
            } catch (error) {
                console.error(`Failed to fetch history for sensor ${sensor.id}:`, error);
            }
        }

        // Render comparison chart if we have data from multiple sensors
        if (sensorHistories.length >= 1 && comparisonSection) {
            comparisonSection.style.display = 'block';
            this.renderComparisonChart(sensorHistories, config);

            // Build legend
            if (legendContainer) {
                legendContainer.innerHTML = sensorHistories.map(sh => `
                    <div class="comparison-legend-item">
                        <span class="comparison-legend-color" style="background: ${sh.sensor.color}"></span>
                        <span>${this.escapeHtml(sh.sensor.name)}</span>
                    </div>
                `).join('');
            }
        } else if (comparisonSection) {
            comparisonSection.style.display = 'none';
        }
    }

    renderSensorSparkline(canvasId, data, color, timeBounds) {
        const canvas = document.getElementById(canvasId);
        if (!canvas || !data || data.length === 0) return;

        const ctx = canvas.getContext('2d');
        const decimals = this.currentMetric === 'pressure' ? 2 : 1;
        const config = this.metricConfig[this.currentMetric];

        // Convert data to time-based points with gap detection
        const gapThreshold = this.getGapThresholdMinutes(this.currentTimeRange);
        const timeData = this.convertToTimeData(data, gapThreshold);

        // Extend line to current time if we have data
        if (timeData.length > 0 && timeBounds?.max) {
            const lastPoint = timeData[timeData.length - 1];
            if (lastPoint && lastPoint.y !== null) {
                // Only extend if last point is within gap threshold of now
                const timeSinceLastPoint = timeBounds.max - lastPoint.x;
                if (timeSinceLastPoint < gapThreshold * 60 * 1000 && timeSinceLastPoint > 0) {
                    timeData.push({ x: timeBounds.max, y: lastPoint.y });
                }
            }
        }

        // Determine appropriate time display format based on range
        const getTimeFormat = (range) => {
            if (['1h', '2h', '4h', '8h', '24h', '48h'].includes(range)) {
                return { unit: 'hour', displayFormats: { hour: 'HH:mm', minute: 'HH:mm' } };
            } else {
                return { unit: 'day', displayFormats: { day: 'd MMM', hour: 'HH:mm' } };
            }
        };
        const timeFormat = getTimeFormat(timeBounds?.range || '24h');

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    data: timeData,
                    borderColor: color,
                    backgroundColor: 'transparent',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    tension: 0.2,
                    fill: false,
                    spanGaps: false // Don't connect points across gaps
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        callbacks: {
                            title: (items) => {
                                if (items.length > 0) {
                                    const date = new Date(items[0].parsed.x);
                                    // Use 24-hour format for tooltip
                                    const hours = date.getHours().toString().padStart(2, '0');
                                    const mins = date.getMinutes().toString().padStart(2, '0');
                                    const day = date.getDate();
                                    const month = date.toLocaleString('en', { month: 'short' });
                                    return `${day} ${month} ${hours}:${mins}`;
                                }
                                return '';
                            },
                            label: (context) => {
                                return `${context.parsed.y.toFixed(decimals)}${config.unit}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        display: true,
                        // Use consistent time bounds across all sparklines
                        min: timeBounds?.min,
                        max: timeBounds?.max,
                        // Prevent padding at edges
                        offset: false,
                        ticks: {
                            color: 'rgba(255,255,255,0.4)',
                            font: { size: 9 },
                            maxRotation: 0,
                            source: 'auto',
                            autoSkip: false,
                            maxTicksLimit: 4
                        },
                        afterBuildTicks: function(axis) {
                            if (!timeBounds?.max || !timeBounds?.min) return;

                            // Generate evenly spaced ticks with current time at right edge
                            const range = timeBounds.max - timeBounds.min;
                            const tickCount = 3;
                            const interval = range / tickCount;

                            const newTicks = [];
                            for (let i = 0; i <= tickCount; i++) {
                                const tickValue = i === tickCount ? timeBounds.max : timeBounds.min + (interval * i);
                                newTicks.push({ value: tickValue });
                            }

                            axis.ticks = newTicks;
                        },
                        grid: { display: false },
                        time: {
                            displayFormats: {
                                hour: 'HH:mm',
                                minute: 'HH:mm',
                                second: 'HH:mm',
                                millisecond: 'HH:mm',
                                day: 'd MMM'
                            }
                        }
                    },
                    y: {
                        display: true,
                        position: 'right',
                        ticks: {
                            maxTicksLimit: 3,
                            color: 'rgba(255,255,255,0.4)',
                            font: { size: 9 },
                            callback: (value) => value.toFixed(decimals)
                        },
                        grid: { display: false }
                    }
                },
                elements: {
                    line: { borderCapStyle: 'round' }
                }
            }
        });

        this.sensorSparklines.push(chart);
    }

    // Convert data array to time-based points, inserting nulls for gaps
    convertToTimeData(data, maxGapMinutes = 30) {
        if (!data || data.length === 0) return [];

        const result = [];
        const maxGapMs = maxGapMinutes * 60 * 1000;

        for (let i = 0; i < data.length; i++) {
            const point = data[i];
            const timestamp = new Date(point.timestamp || point.bucket).getTime();

            // Check for gap from previous point
            if (i > 0) {
                const prevTimestamp = new Date(data[i-1].timestamp || data[i-1].bucket).getTime();
                const gap = timestamp - prevTimestamp;

                // If gap is too large, insert a null to break the line
                if (gap > maxGapMs) {
                    result.push({ x: prevTimestamp + 1, y: null });
                }
            }

            result.push({ x: timestamp, y: point.value });
        }

        return result;
    }

    // Get time range in milliseconds for chart bounds
    getTimeRangeMs(range) {
        const ranges = {
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000,
            '48h': 48 * 60 * 60 * 1000,
            '7d': 7 * 24 * 60 * 60 * 1000,
            '30d': 30 * 24 * 60 * 60 * 1000,
            '90d': 90 * 24 * 60 * 60 * 1000,
            '1y': 365 * 24 * 60 * 60 * 1000,
            'all': 10 * 365 * 24 * 60 * 60 * 1000 // 10 years for "all"
        };
        return ranges[range] || ranges['24h'];
    }

    // Get appropriate gap threshold in minutes based on time range
    // This should be ~1.5x the expected bucket size to allow for some variance
    getGapThresholdMinutes(range) {
        const thresholds = {
            '1h': 15,       // Raw data, 15 min gap threshold
            '2h': 15,       // Raw data
            '4h': 15,       // Raw data
            '8h': 20,       // Raw data
            '24h': 30,      // Raw data, 30 min gap threshold
            '48h': 90,      // 1-hour buckets, 90 min threshold
            '7d': 180,      // 2-hour buckets, 3 hour threshold
            '30d': 540,     // 6-hour buckets, 9 hour threshold
            '90d': 2160,    // 1-day buckets, 36 hour threshold
            '1y': 7200,     // 3-day buckets, 5 day threshold
            'all': 14400    // 1-week buckets, 10 day threshold
        };
        return thresholds[range] || 30;
    }

    renderComparisonChart(sensorHistories, config) {
        const canvas = document.getElementById('sensorComparisonChart');
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        const decimals = this.currentMetric === 'pressure' ? 2 : 1;

        // Calculate explicit time bounds so chart shows "now" correctly
        const now = Date.now();
        const timeRangeMs = this.getTimeRangeMs(this.currentTimeRange);
        const xMin = now - timeRangeMs;
        const xMax = now;
        const useExplicitBounds = this.currentTimeRange !== 'all';

        // Convert each sensor's data to time-based points with gap detection
        const gapThreshold = this.getGapThresholdMinutes(this.currentTimeRange);
        const datasets = sensorHistories.map(sh => {
            const timeData = this.convertToTimeData(sh.data, gapThreshold);

            // Extend line to current time if we have data
            if (timeData.length > 0) {
                const lastPoint = timeData[timeData.length - 1];
                if (lastPoint && lastPoint.y !== null) {
                    // Only extend if last point is within gap threshold of now
                    const timeSinceLastPoint = now - lastPoint.x;
                    if (timeSinceLastPoint < gapThreshold * 60 * 1000 && timeSinceLastPoint > 0) {
                        timeData.push({ x: now, y: lastPoint.y });
                    }
                }
            }

            return {
                label: sh.sensor.name,
                data: timeData,
                borderColor: sh.sensor.color,
                backgroundColor: 'transparent',
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.2,
                fill: false,
                spanGaps: false // Don't connect points across gaps
            };
        });

        this.comparisonChart = new Chart(ctx, {
            type: 'line',
            data: { datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'nearest',
                    intersect: false,
                    axis: 'x'
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        callbacks: {
                            title: (items) => {
                                if (items.length > 0) {
                                    const date = new Date(items[0].parsed.x);
                                    // Use 24-hour format for tooltip
                                    const hours = date.getHours().toString().padStart(2, '0');
                                    const mins = date.getMinutes().toString().padStart(2, '0');
                                    const day = date.getDate();
                                    const month = date.toLocaleString('en', { month: 'short' });
                                    return `${day} ${month} ${hours}:${mins}`;
                                }
                                return '';
                            },
                            label: (context) => {
                                if (context.parsed.y === null) return null;
                                return `${context.dataset.label}: ${context.parsed.y.toFixed(decimals)}${config.unit}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        min: useExplicitBounds ? xMin : undefined,
                        max: useExplicitBounds ? xMax : undefined,
                        display: true,
                        // Prevent padding at edges
                        offset: false,
                        ticks: {
                            color: 'rgba(255,255,255,0.5)',
                            font: { size: 10 },
                            maxRotation: 0,
                            source: 'auto',
                            autoSkip: false,
                            maxTicksLimit: 6
                        },
                        afterBuildTicks: function(axis) {
                            if (!useExplicitBounds) return;

                            // Generate evenly spaced ticks with current time at right edge
                            const range = xMax - xMin;
                            const tickCount = 5;
                            const interval = range / tickCount;

                            const newTicks = [];
                            for (let i = 0; i <= tickCount; i++) {
                                const tickValue = i === tickCount ? xMax : xMin + (interval * i);
                                newTicks.push({ value: tickValue });
                            }

                            axis.ticks = newTicks;
                        },
                        grid: { display: false },
                        time: {
                            displayFormats: {
                                hour: 'HH:mm',
                                minute: 'HH:mm',
                                second: 'HH:mm',
                                millisecond: 'HH:mm',
                                day: 'd MMM'
                            }
                        }
                    },
                    y: {
                        display: true,
                        ticks: {
                            maxTicksLimit: 5,
                            color: 'rgba(255,255,255,0.5)',
                            font: { size: 10 },
                            callback: (value) => value.toFixed(decimals)
                        },
                        grid: {
                            color: 'rgba(255,255,255,0.1)',
                            drawBorder: false
                        }
                    }
                }
            }
        });
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }

    formatTimeSince(timestamp) {
        if (!timestamp) return '';
        const now = Date.now();
        const then = new Date(timestamp).getTime();
        const diffMs = now - then;

        const minutes = Math.floor(diffMs / 60000);
        const hours = Math.floor(diffMs / 3600000);
        const days = Math.floor(diffMs / 86400000);

        if (days > 0) return `${days}d ago`;
        if (hours > 0) return `${hours}h ago`;
        if (minutes > 0) return `${minutes}m ago`;
        return 'just now';
    }

    clearComparisonStats() {
        document.getElementById('sidebar1hValue').textContent = '--';
        document.getElementById('sidebar1hDiff').textContent = '';
        document.getElementById('sidebarYesterdayValue').textContent = '--';
        document.getElementById('sidebarYesterdayDiff').textContent = '';
        document.getElementById('sidebarWeekValue').textContent = '--';
        document.getElementById('sidebarWeekDiff').textContent = '';
        document.getElementById('sidebarRangeLow').textContent = '--';
        document.getElementById('sidebarRangeHigh').textContent = '--';
    }

    async fetchComparisonData() {
        if (!this.deviceIds || this.deviceIds.length === 0) return;

        try {
            const response = await fetch(`/api/comparison?devices=${this.deviceIds.join(',')}`);
            if (!response.ok) return;

            const comparison = await response.json();

            // Map frontend metric names to database names
            const metricToDbKey = {
                'pm1': 'pm1_0',
                'nox_index': 'nox_index',
                'voc_index': 'voc_index'
            };
            const dbKey = metricToDbKey[this.currentMetric] || this.currentMetric;
            const data = comparison[dbKey] || comparison[this.currentMetric];

            if (data) {
                const config = this.metricConfig[this.currentMetric];
                const decimals = this.currentMetric === 'pressure' ? 2 : 1;

                this.updateComparisonStats({
                    oneHourAgo: data.hourAgo !== null ? data.hourAgo.toFixed(decimals) + config.unit : null,
                    oneHourDiff: data.hourAgoDiff,
                    yesterday: data.yesterday !== null ? data.yesterday.toFixed(decimals) + config.unit : null,
                    yesterdayDiff: data.yesterdayDiff,
                    weekAvg: data.weekAvg !== null ? data.weekAvg.toFixed(decimals) + config.unit : null,
                    weekDiff: data.weekDiff,
                    rangeLow: data.todayMin !== null ? data.todayMin.toFixed(decimals) : null,
                    rangeHigh: data.todayMax !== null ? data.todayMax.toFixed(decimals) : null
                });
            }
        } catch (error) {
            console.error('Failed to fetch sidebar comparison data:', error);
        }
    }

    close() {
        this.sidebar.classList.remove('open');
        this.overlay.classList.remove('visible');
        this.isOpen = false;

        // Destroy main chart to free resources
        if (this.chart) {
            this.chart.destroy();
            this.chart = null;
        }

        // Destroy sensor sparklines
        if (this.sensorSparklines) {
            this.sensorSparklines.forEach(chart => chart.destroy());
            this.sensorSparklines = [];
        }

        // Destroy comparison chart
        if (this.comparisonChart) {
            this.comparisonChart.destroy();
            this.comparisonChart = null;
        }
    }

    updateComparisonStats(data) {
        // Use 2 decimal places for pressure, 1 for other metrics
        const decimals = this.currentMetric === 'pressure' ? 2 : 1;

        // 1 hour ago
        if (data.oneHourAgo != null) {
            document.getElementById('sidebar1hValue').textContent = data.oneHourAgo;
            const diff1h = data.oneHourDiff;
            const diff1hEl = document.getElementById('sidebar1hDiff');
            if (diff1h != null) {
                const sign = diff1h > 0 ? '+' : '';
                diff1hEl.textContent = `${sign}${diff1h.toFixed(decimals)} ${diff1h >= 0 ? '↑' : '↓'}`;
                diff1hEl.className = 'stat-card-diff ' + (diff1h >= 0 ? 'positive' : 'negative');
            } else {
                diff1hEl.textContent = '';
            }
        }

        // Yesterday
        if (data.yesterday != null) {
            document.getElementById('sidebarYesterdayValue').textContent = data.yesterday;
            const diffY = data.yesterdayDiff;
            const diffYEl = document.getElementById('sidebarYesterdayDiff');
            if (diffY != null) {
                const sign = diffY > 0 ? '+' : '';
                diffYEl.textContent = `${sign}${diffY.toFixed(decimals)} ${diffY >= 0 ? '↑' : '↓'}`;
                diffYEl.className = 'stat-card-diff ' + (diffY >= 0 ? 'positive' : 'negative');
            } else {
                diffYEl.textContent = '';
            }
        }

        // Week average
        if (data.weekAvg != null) {
            document.getElementById('sidebarWeekValue').textContent = data.weekAvg;
            const diffW = data.weekDiff;
            const diffWEl = document.getElementById('sidebarWeekDiff');
            if (diffW != null) {
                const sign = diffW > 0 ? '+' : '';
                diffWEl.textContent = `${sign}${diffW.toFixed(decimals)} ${diffW >= 0 ? '↑' : '↓'}`;
                diffWEl.className = 'stat-card-diff ' + (diffW >= 0 ? 'positive' : 'negative');
            } else {
                diffWEl.textContent = '';
            }
        }

        // Today's range
        if (data.rangeLow != null && data.rangeHigh != null) {
            document.getElementById('sidebarRangeLow').textContent = data.rangeLow;
            document.getElementById('sidebarRangeHigh').textContent = data.rangeHigh;
        }
    }

    async loadChartData() {
        const config = this.metricConfig[this.currentMetric];
        if (!config) return;

        const canvas = document.getElementById('sidebarChart');
        if (!canvas) return;

        // Destroy existing chart
        if (this.chart) {
            this.chart.destroy();
        }

        // Update period range label
        const rangeLabels = {
            '1h': 'Last 1h', '2h': 'Last 2h', '4h': 'Last 4h', '8h': 'Last 8h',
            '24h': 'Last 24h', '48h': 'Last 48h', '7d': 'Last 7 days',
            '30d': 'Last 30 days', '90d': 'Last 90 days', '1y': 'Last year', 'all': 'All time'
        };
        const rangeLabel = document.getElementById('periodStatsRange');
        if (rangeLabel) {
            rangeLabel.textContent = rangeLabels[this.currentTimeRange] || this.currentTimeRange;
        }

        try {
            // Fetch historical data for the sidebar chart
            const data = await this.fetchHistoricalData();

            // Update period statistics (filter out nulls for stats)
            const validValues = data.values.filter(v => v !== null);
            this.updatePeriodStats(validValues, config);

            const decimals = this.currentMetric === 'pressure' ? 2 : 1;

            // Convert to time-based data with gap detection
            const gapThreshold = this.getGapThresholdMinutes(this.currentTimeRange);
            const timeData = this.convertToTimeData(data.rawData || [], gapThreshold);

            // Calculate explicit time bounds so chart shows "now" correctly
            // even if sensor data stopped earlier
            const now = Date.now();
            const timeRangeMs = this.getTimeRangeMs(this.currentTimeRange);
            const xMin = now - timeRangeMs;
            const xMax = now;

            // Extend line to current time if we have data
            // This prevents a gap between the last data point and the right edge
            if (timeData.length > 0) {
                const lastPoint = timeData[timeData.length - 1];
                if (lastPoint && lastPoint.y !== null) {
                    // Only extend if last point is within 30 mins of now (not stale)
                    const timeSinceLastPoint = now - lastPoint.x;
                    if (timeSinceLastPoint < 30 * 60 * 1000) {
                        timeData.push({ x: now, y: lastPoint.y });
                    }
                }
            }

            // For "all" range, let Chart.js auto-determine bounds from data
            const useExplicitBounds = this.currentTimeRange !== 'all';

            this.chart = new Chart(canvas, {
                type: 'line',
                data: {
                    datasets: [{
                        label: config.title,
                        data: timeData,
                        borderColor: config.color,
                        backgroundColor: config.color + '20',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHitRadius: 10,
                        spanGaps: false // Don't connect across gaps
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            mode: 'nearest',
                            intersect: false,
                            callbacks: {
                                title: (items) => {
                                    if (items.length > 0) {
                                        const date = new Date(items[0].parsed.x);
                                        const hours = date.getHours().toString().padStart(2, '0');
                                        const mins = date.getMinutes().toString().padStart(2, '0');
                                        const day = date.getDate();
                                        const month = date.toLocaleString('en', { month: 'short' });
                                        return `${day} ${month} ${hours}:${mins}`;
                                    }
                                    return '';
                                },
                                label: (context) => {
                                    if (context.parsed.y === null) return null;
                                    return `${context.parsed.y.toFixed(decimals)}${config.unit}`;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            type: 'time',
                            min: useExplicitBounds ? xMin : undefined,
                            max: useExplicitBounds ? xMax : undefined,
                            // Prevent padding at edges
                            offset: false,
                            grid: { color: '#334155' },
                            ticks: {
                                color: '#64748b',
                                maxRotation: 0,
                                source: 'auto',
                                autoSkip: false,
                                maxTicksLimit: 6
                            },
                            afterBuildTicks: function(axis) {
                                if (!useExplicitBounds) return;

                                // Generate evenly spaced ticks with current time at right edge
                                const range = xMax - xMin;
                                const tickCount = 5;
                                const interval = range / tickCount;

                                const newTicks = [];
                                for (let i = 0; i <= tickCount; i++) {
                                    const tickValue = i === tickCount ? xMax : xMin + (interval * i);
                                    newTicks.push({ value: tickValue });
                                }

                                axis.ticks = newTicks;
                            },
                            time: {
                                displayFormats: {
                                    hour: 'HH:mm',
                                    minute: 'HH:mm',
                                    second: 'HH:mm',
                                    millisecond: 'HH:mm',
                                    day: 'd MMM'
                                }
                            }
                        },
                        y: {
                            grid: { color: '#334155' },
                            ticks: {
                                color: '#64748b',
                                callback: (value) => value.toFixed(decimals)
                            }
                        }
                    },
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    }
                }
            });
        } catch (error) {
            console.error('Error loading sidebar chart:', error);
        }
    }

    async fetchHistoricalData() {
        // Use sensor IDs from sensorData (contributing sensors) if available,
        // otherwise fall back to deviceIds. This ensures the main chart only
        // shows data from sensors that are currently reporting.
        const sensorIds = (this.sensorData && this.sensorData.length > 0)
            ? this.sensorData.map(s => s.id)
            : this.deviceIds;

        if (!sensorIds || sensorIds.length === 0) {
            return { labels: [], values: [], rawData: [] };
        }

        // Some reading types have alternate names in the database
        const alternateTypes = {
            'voc_index': 'voc',
            'voc': 'voc_index',
            'pm1': 'pm1_0',
            'pm1_0': 'pm1',
            'nox_index': 'nox',
            'nox': 'nox_index'
        };

        try {
            let url = `/api/history/aggregate?devices=${sensorIds.join(',')}&type=${this.currentMetric}&range=${this.currentTimeRange}`;
            let response = await fetch(url);
            let result = response.ok ? await response.json() : { data: [] };
            let data = result.data || [];

            // If no data and there's an alternate type, try that
            if (data.length === 0 && alternateTypes[this.currentMetric]) {
                const altType = alternateTypes[this.currentMetric];
                url = `/api/history/aggregate?devices=${sensorIds.join(',')}&type=${altType}&range=${this.currentTimeRange}`;
                response = await fetch(url);
                if (response.ok) {
                    result = await response.json();
                    data = result.data || [];
                }
            }

            if (data.length === 0) {
                return { labels: [], values: [], rawData: [] };
            }

            const labels = data.map(d => this.formatTimeLabel(new Date(d.timestamp), this.currentTimeRange));
            const values = data.map(d => d.value);

            // Return raw data for time-based chart conversion
            return { labels, values, rawData: data };

        } catch (error) {
            console.error('Error fetching historical data:', error);
            return { labels: [], values: [], rawData: [] };
        }
    }

    formatTimeLabel(date, range) {
        // Format time in 24-hour format
        const hours = date.getHours().toString().padStart(2, '0');
        const mins = date.getMinutes().toString().padStart(2, '0');

        if (['1h', '2h', '4h', '8h', '24h', '48h'].includes(range)) {
            return `${hours}:${mins}`;
        } else {
            const day = date.getDate();
            const month = date.toLocaleString('en', { month: 'short' });
            return `${day} ${month}`;
        }
    }

    // ==================== Period Stats & Insights ====================

    updatePeriodStats(values, config) {
        if (!values || values.length === 0) {
            document.getElementById('periodStatMin').textContent = '--';
            document.getElementById('periodStatMax').textContent = '--';
            document.getElementById('periodStatAvg').textContent = '--';
            document.getElementById('periodStatRange').textContent = '--';
            return;
        }

        const min = Math.min(...values);
        const max = Math.max(...values);
        const avg = values.reduce((a, b) => a + b, 0) / values.length;
        const range = max - min;

        const decimals = this.currentMetric === 'pressure' ? 2 : 1;
        const unit = config.unit;

        document.getElementById('periodStatMin').textContent = min.toFixed(decimals) + unit;
        document.getElementById('periodStatMax').textContent = max.toFixed(decimals) + unit;
        document.getElementById('periodStatAvg').textContent = avg.toFixed(decimals) + unit;
        document.getElementById('periodStatRange').textContent = range.toFixed(decimals) + unit;

        // Generate insights based on current data
        this.generateInsights(values, min, max, avg);
    }

    generateInsights(values, min, max, avg) {
        const insightsList = document.getElementById('insightsList');
        if (!insightsList) return;

        const insights = [];
        const metric = this.currentMetric;
        const current = values[values.length - 1]; // Most recent value

        // Metric-specific insights
        switch (metric) {
            case 'temperature':
                if (current > 30) {
                    insights.push({
                        type: 'warning',
                        icon: '',
                        title: 'High Temperature',
                        desc: 'Consider cooling measures. Stay hydrated.'
                    });
                } else if (current < 10) {
                    insights.push({
                        type: 'info',
                        icon: '',
                        title: 'Cold Conditions',
                        desc: 'Heating may be needed for comfort.'
                    });
                }
                if (max - min > 10) {
                    insights.push({
                        type: 'info',
                        icon: '',
                        title: 'High Variability',
                        desc: `Temperature varied by ${(max - min).toFixed(1)}°C in this period.`
                    });
                }
                break;

            case 'humidity':
                if (current > 70) {
                    insights.push({
                        type: 'warning',
                        icon: '',
                        title: 'High Humidity',
                        desc: 'Consider ventilation to reduce moisture and prevent mold.'
                    });
                } else if (current < 30) {
                    insights.push({
                        type: 'info',
                        icon: '',
                        title: 'Low Humidity',
                        desc: 'Dry air may cause discomfort. Consider a humidifier.'
                    });
                } else if (current >= 40 && current <= 60) {
                    insights.push({
                        type: 'success',
                        icon: '',
                        title: 'Optimal Humidity',
                        desc: 'Humidity is in the ideal comfort range (40-60%).'
                    });
                }
                break;

            case 'pressure':
                // Check for rapid pressure changes (would need historical comparison)
                if (current < 1000) {
                    insights.push({
                        type: 'info',
                        icon: '',
                        title: 'Low Pressure',
                        desc: 'Unsettled weather possible. Watch for incoming rain.'
                    });
                } else if (current > 1020) {
                    insights.push({
                        type: 'success',
                        icon: '',
                        title: 'High Pressure',
                        desc: 'Fair weather expected. Good conditions likely.'
                    });
                }
                break;

            case 'co2':
                if (current > 1500) {
                    insights.push({
                        type: 'alert',
                        icon: '',
                        title: 'Poor Air Quality',
                        desc: 'CO₂ levels are high. Open windows immediately for fresh air.'
                    });
                } else if (current > 1000) {
                    insights.push({
                        type: 'warning',
                        icon: '',
                        title: 'Elevated CO₂',
                        desc: 'Consider opening a window. May cause drowsiness.'
                    });
                } else if (current < 600) {
                    insights.push({
                        type: 'success',
                        icon: '',
                        title: 'Excellent Air Quality',
                        desc: 'CO₂ levels are similar to fresh outdoor air.'
                    });
                }
                break;

            case 'pm2_5':
                if (current > 35) {
                    insights.push({
                        type: 'alert',
                        icon: '',
                        title: 'Unhealthy PM2.5',
                        desc: 'Air quality is poor. Limit exposure if possible.'
                    });
                } else if (current > 12) {
                    insights.push({
                        type: 'warning',
                        icon: '',
                        title: 'Moderate PM2.5',
                        desc: 'Sensitive individuals may want to limit prolonged exposure.'
                    });
                } else {
                    insights.push({
                        type: 'success',
                        icon: '',
                        title: 'Good Air Quality',
                        desc: 'PM2.5 levels are within healthy limits.'
                    });
                }
                break;
        }

        // Trend insight (if we have enough data)
        if (values.length >= 3) {
            const recentTrend = values[values.length - 1] - values[Math.floor(values.length / 2)];
            if (Math.abs(recentTrend) > (max - min) * 0.3) {
                const direction = recentTrend > 0 ? 'rising' : 'falling';
                insights.push({
                    type: 'info',
                    icon: recentTrend > 0 ? '📈' : '📉',
                    title: `${direction.charAt(0).toUpperCase() + direction.slice(1)} Trend`,
                    desc: `Values have been ${direction} during this period.`
                });
            }
        }

        // Render insights
        if (insights.length === 0) {
            insightsList.innerHTML = '<div class="insights-empty">No notable insights for current conditions.</div>';
        } else {
            insightsList.innerHTML = insights.map(insight => `
                <div class="insight-item ${insight.type}">
                    <span class="insight-icon">${insight.icon}</span>
                    <div class="insight-content">
                        <div class="insight-title">${insight.title}</div>
                        <div class="insight-desc">${insight.desc}</div>
                    </div>
                </div>
            `).join('');
        }
    }

}

// Global sidebar instance
let detailsSidebar = null;

// RoomDetailsSidebar - Handles indoor room detail view with charts
class RoomDetailsSidebar {
    constructor() {
        this.sidebar = document.getElementById('roomDetailsSidebar');
        this.overlay = document.getElementById('roomDetailsSidebarOverlay');
        this.isOpen = false;
        this.currentRoom = null;
        this.currentRoomData = null;
        this.currentMetric = 'temperature';
        this.currentTimeRange = '24h';
        this.chart = null;

        this.setupEventListeners();

        // Handle window resize - debounced
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => this.handleResize(), 150);
        });
    }

    handleResize() {
        if (this.chart && typeof this.chart.resize === 'function') {
            this.chart.resize();
        }
    }

    setupEventListeners() {
        // Back button closes sidebar
        document.getElementById('roomSidebarBackBtn')?.addEventListener('click', () => this.close());

        // Overlay click closes sidebar
        this.overlay?.addEventListener('click', () => this.close());

        // Time pills
        document.querySelectorAll('#roomTimePills .time-pill').forEach(pill => {
            pill.addEventListener('click', (e) => {
                document.querySelectorAll('#roomTimePills .time-pill').forEach(p => p.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeRange = e.target.dataset.range;
                this.loadChartData();
            });
        });

        // Metric selector
        document.getElementById('roomMetricSelect')?.addEventListener('change', (e) => {
            this.currentMetric = e.target.value;
            this.loadChartData();
        });

        // Escape key closes sidebar
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
    }

    open(roomName, roomData) {
        this.currentRoom = roomName;
        this.currentRoomData = roomData;

        // Update title with clean display name
        document.getElementById('roomSidebarTitle').textContent = extractRoomDisplayName(roomName);

        // Update insight banner
        const insight = this.generateInsight(roomData);
        const banner = document.getElementById('roomInsightBanner');
        banner.className = `room-insight-banner ${insight.class}`;
        document.getElementById('roomInsightIcon').textContent = insight.icon;
        document.getElementById('roomInsightText').textContent = insight.text;

        // Update current values
        this.updateCurrentValues(roomData);

        // Update metric selector options based on available data
        this.updateMetricSelector(roomData);

        // Show sidebar
        this.sidebar.classList.add('open');
        this.overlay.classList.add('visible');
        this.isOpen = true;

        // Load chart data
        this.loadChartData();

        // Show contributing sensors
        this.showDeviceInfo(roomData);
    }

    close() {
        this.sidebar.classList.remove('open');
        this.overlay.classList.remove('visible');
        this.isOpen = false;

        // Destroy chart to free memory
        if (this.chart) {
            this.chart.destroy();
            this.chart = null;
        }
    }

    generateInsight(room) {
        // CO2 checks
        if (room.co2 != null) {
            if (room.co2 > 2000) return { text: 'Ventilate now - CO2 very high', icon: '', class: 'critical' };
            if (room.co2 > 1500) return { text: 'Open a window - CO2 elevated', icon: '', class: 'warning' };
            if (room.co2 > 1000) return { text: 'Air getting stale', icon: '', class: 'moderate' };
        }

        // PM2.5 checks
        if (room.pm25 != null) {
            if (room.pm25 > 35) return { text: 'High particles - ventilate', icon: '', class: 'warning' };
            if (room.pm25 > 12) return { text: 'Particles slightly elevated', icon: '', class: 'moderate' };
        }

        // VOC checks
        if (room.voc != null) {
            if (room.voc > 300) return { text: 'VOCs high - ventilate', icon: '', class: 'warning' };
            if (room.voc > 200) return { text: 'VOCs moderate', icon: '', class: 'moderate' };
        }

        // Temperature comfort
        if (room.temperature != null) {
            if (room.temperature > 28) return { text: 'Room too warm', icon: '', class: 'moderate' };
            if (room.temperature < 16) return { text: 'Room too cold', icon: '', class: 'moderate' };
        }

        // Humidity
        if (room.humidity != null) {
            if (room.humidity > 70) return { text: 'Humidity high - ventilate', icon: '', class: 'moderate' };
            if (room.humidity < 30) return { text: 'Air too dry', icon: '', class: 'info' };
        }

        return { text: 'Air quality good', icon: '', class: 'good' };
    }

    updateCurrentValues(room) {
        // Temperature
        if (room.temperature != null) {
            document.getElementById('roomTempValue').textContent = room.temperature.toFixed(1);
            document.getElementById('roomTempCard').style.display = 'block';
        } else {
            document.getElementById('roomTempCard').style.display = 'none';
        }

        // Humidity
        if (room.humidity != null) {
            document.getElementById('roomHumidityValue').textContent = room.humidity.toFixed(1);
            document.getElementById('roomHumidityCard').style.display = 'block';
        } else {
            document.getElementById('roomHumidityCard').style.display = 'none';
        }

        // CO2
        if (room.co2 != null) {
            document.getElementById('roomCO2Value').textContent = Math.round(room.co2);
            document.getElementById('roomCO2Card').style.display = 'block';
            const status = document.getElementById('roomCO2Status');
            if (room.co2 > 1500) {
                status.textContent = 'Poor';
                status.className = 'metric-status poor';
            } else if (room.co2 > 1000) {
                status.textContent = 'Moderate';
                status.className = 'metric-status moderate';
            } else {
                status.textContent = 'Good';
                status.className = 'metric-status good';
            }
        } else {
            document.getElementById('roomCO2Card').style.display = 'none';
        }

        // PM2.5
        if (room.pm25 != null) {
            document.getElementById('roomPM25Value').textContent = room.pm25.toFixed(1);
            document.getElementById('roomPM25Card').style.display = 'block';
            const status = document.getElementById('roomPM25Status');
            if (room.pm25 > 35) {
                status.textContent = 'Poor';
                status.className = 'metric-status poor';
            } else if (room.pm25 > 12) {
                status.textContent = 'Moderate';
                status.className = 'metric-status moderate';
            } else {
                status.textContent = 'Good';
                status.className = 'metric-status good';
            }
        } else {
            document.getElementById('roomPM25Card').style.display = 'none';
        }

        // VOC
        if (room.voc != null) {
            document.getElementById('roomVOCValue').textContent = Math.round(room.voc);
            document.getElementById('roomVOCCard').style.display = 'block';
            const status = document.getElementById('roomVOCStatus');
            if (room.voc > 300) {
                status.textContent = 'Poor';
                status.className = 'metric-status poor';
            } else if (room.voc > 200) {
                status.textContent = 'Moderate';
                status.className = 'metric-status moderate';
            } else {
                status.textContent = 'Good';
                status.className = 'metric-status good';
            }
        } else {
            document.getElementById('roomVOCCard').style.display = 'none';
        }

        // Pressure
        if (room.pressure != null) {
            document.getElementById('roomPressureValue').textContent = Math.round(room.pressure);
            document.getElementById('roomPressureCard').style.display = 'block';
        } else {
            document.getElementById('roomPressureCard').style.display = 'none';
        }
    }

    updateMetricSelector(room) {
        const select = document.getElementById('roomMetricSelect');
        if (!select) return;

        // Enable/disable options based on available data
        const options = select.querySelectorAll('option');
        let firstAvailable = null;

        options.forEach(opt => {
            const metric = opt.value;
            let hasData = false;

            if (metric === 'temperature') hasData = room.temperature != null;
            else if (metric === 'humidity') hasData = room.humidity != null;
            else if (metric === 'co2') hasData = room.co2 != null;
            else if (metric === 'pm2_5') hasData = room.pm25 != null;
            else if (metric === 'voc_index') hasData = room.voc != null;

            opt.disabled = !hasData;
            if (hasData && !firstAvailable) firstAvailable = metric;
        });

        // Set to first available metric if current is not available
        const currentOption = select.querySelector(`option[value="${this.currentMetric}"]`);
        if (currentOption?.disabled && firstAvailable) {
            this.currentMetric = firstAvailable;
            select.value = firstAvailable;
        }
    }

    async loadChartData() {
        const sensors = this.currentRoomData?.sensors ? Object.values(this.currentRoomData.sensors) : [];

        // Fallback to single sensor mode if no sensors array
        if (sensors.length === 0 && this.currentRoomData?.sensorId) {
            sensors.push({ deviceId: this.currentRoomData.sensorId, name: 'Sensor' });
        }

        if (sensors.length === 0) return;

        // Some reading types have alternate names in the database
        const alternateTypes = {
            'voc_index': 'voc',
            'voc': 'voc_index',
            'pm1': 'pm1_0',
            'pm1_0': 'pm1',
            'nox_index': 'nox',
            'nox': 'nox_index'
        };

        try {
            // Fetch history for all sensors in parallel
            const sensorDataPromises = sensors.map(async (sensor) => {
                const deviceId = sensor.deviceId;
                let response = await fetch(`/api/history?device_id=${encodeURIComponent(deviceId)}&reading_type=${this.currentMetric}&range=${this.currentTimeRange}`);
                let data = response.ok ? await response.json() : { historyData: [] };

                // If no data and there's an alternate type, try that
                if ((!data.historyData || data.historyData.length === 0) && alternateTypes[this.currentMetric]) {
                    const altType = alternateTypes[this.currentMetric];
                    response = await fetch(`/api/history?device_id=${encodeURIComponent(deviceId)}&reading_type=${altType}&range=${this.currentTimeRange}`);
                    if (response.ok) {
                        data = await response.json();
                    }
                }

                return {
                    sensor,
                    historyData: data.historyData || []
                };
            });

            const allSensorData = await Promise.all(sensorDataPromises);
            this.renderChart(allSensorData);
        } catch (error) {
            console.error('Error loading room chart data:', error);
            this.renderChart([]);
        }
    }

    renderChart(sensorDataArray) {
        const canvas = document.getElementById('roomHistoryChart');
        if (!canvas) return;

        // Destroy existing chart
        if (this.chart) {
            this.chart.destroy();
        }

        // Handle legacy single-sensor format (array of data points)
        if (Array.isArray(sensorDataArray) && sensorDataArray.length > 0 && !sensorDataArray[0].sensor) {
            // Legacy format - wrap in single sensor object
            sensorDataArray = [{ sensor: { name: 'Sensor', deviceId: 'unknown' }, historyData: sensorDataArray }];
        }

        // Color palette for multi-sensor display
        const sensorColors = ['#3b82f6', '#22c55e', '#f59e0b', '#8b5cf6', '#ef4444', '#06b6d4'];

        // Calculate time range bounds
        const now = Date.now();
        const timeRangeMs = this.getTimeRangeMs(this.currentTimeRange);
        const xMin = now - timeRangeMs;
        const xMax = now;

        const gapThreshold = this.getGapThresholdMinutes(this.currentTimeRange);

        // Build datasets for each sensor
        const datasets = sensorDataArray
            .filter(sd => sd.historyData && sd.historyData.length > 0)
            .map((sensorData, idx) => {
                const color = sensorColors[idx % sensorColors.length];
                const timeData = this.convertToTimeData(sensorData.historyData, gapThreshold);

                // Extend line to current time if we have data
                if (timeData.length > 0) {
                    const lastPoint = timeData[timeData.length - 1];
                    if (lastPoint && lastPoint.y !== null) {
                        const timeSinceLastPoint = now - lastPoint.x;
                        if (timeSinceLastPoint < 30 * 60 * 1000 && timeSinceLastPoint > 0) {
                            timeData.push({ x: now, y: lastPoint.y });
                        }
                    }
                }

                // Get short display name for legend
                const shortId = sensorData.sensor.deviceId.length > 12
                    ? sensorData.sensor.deviceId.slice(-8)
                    : sensorData.sensor.deviceId;
                const displayName = sensorData.sensor.name !== sensorData.sensor.deviceId
                    ? sensorData.sensor.name
                    : shortId;

                return {
                    label: displayName,
                    data: timeData,
                    borderColor: color,
                    backgroundColor: color + '20',
                    fill: sensorDataArray.length === 1, // Only fill if single sensor
                    tension: 0.3,
                    pointRadius: 0,
                    borderWidth: 2,
                    spanGaps: false
                };
            });

        // Show legend only when multiple sensors have data
        const showLegend = datasets.length > 1;

        this.chart = new Chart(canvas, {
            type: 'line',
            data: { datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        display: showLegend,
                        position: 'top',
                        labels: {
                            color: '#94a3b8',
                            usePointStyle: true,
                            pointStyle: 'line',
                            font: { size: 10 },
                            padding: 8
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: (ctx) => {
                                if (ctx.parsed.y === null) return '';
                                const units = {
                                    temperature: '°C',
                                    humidity: '%',
                                    co2: ' ppm',
                                    pm2_5: ' µg/m³',
                                    voc_index: ''
                                };
                                const label = ctx.dataset.label || '';
                                return `${label}: ${ctx.parsed.y.toFixed(1)}${units[this.currentMetric] || ''}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        min: xMin,
                        max: xMax,
                        offset: false,
                        time: {
                            displayFormats: {
                                hour: 'HH:mm',
                                minute: 'HH:mm',
                                second: 'HH:mm',
                                millisecond: 'HH:mm',
                                day: 'd MMM'
                            }
                        },
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: {
                            color: '#94a3b8',
                            maxRotation: 0,
                            source: 'auto',
                            autoSkip: false,
                            maxTicksLimit: 6
                        },
                        afterBuildTicks: function(axis) {
                            const range = xMax - xMin;
                            const tickCount = 5;
                            const interval = range / tickCount;

                            const newTicks = [];
                            for (let i = 0; i <= tickCount; i++) {
                                const tickValue = i === tickCount ? xMax : xMin + (interval * i);
                                newTicks.push({ value: tickValue });
                            }

                            axis.ticks = newTicks;
                        }
                    },
                    y: {
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#94a3b8' }
                    }
                }
            }
        });
    }

    convertToTimeData(data, maxGapMinutes = 30) {
        if (!data || data.length === 0) return [];

        const result = [];
        const maxGapMs = maxGapMinutes * 60 * 1000;

        for (let i = 0; i < data.length; i++) {
            const point = data[i];
            const timestamp = new Date(point.timestamp || point.bucket).getTime();

            // Check for gap from previous point
            if (i > 0) {
                const prevTimestamp = new Date(data[i-1].timestamp || data[i-1].bucket).getTime();
                const gap = timestamp - prevTimestamp;
                if (gap > maxGapMs) {
                    result.push({ x: prevTimestamp + 1, y: null });
                }
            }

            result.push({ x: timestamp, y: point.value });
        }

        return result;
    }

    getTimeRangeMs(range) {
        const ranges = {
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000,
            '48h': 48 * 60 * 60 * 1000,
            '7d': 7 * 24 * 60 * 60 * 1000,
            '30d': 30 * 24 * 60 * 60 * 1000,
            '90d': 90 * 24 * 60 * 60 * 1000,
            '1y': 365 * 24 * 60 * 60 * 1000,
            'all': 10 * 365 * 24 * 60 * 60 * 1000
        };
        return ranges[range] || ranges['24h'];
    }

    // Get appropriate gap threshold in minutes based on time range
    getGapThresholdMinutes(range) {
        const thresholds = {
            '1h': 15,
            '2h': 15,
            '4h': 15,
            '8h': 20,
            '24h': 30,
            '48h': 90,
            '7d': 180,
            '30d': 540,
            '90d': 2160,
            '1y': 7200,
            'all': 14400
        };
        return thresholds[range] || 30;
    }

    showDeviceInfo(room) {
        const container = document.getElementById('roomHardwareInfo');
        if (!container) return;

        const sensors = room.sensors ? Object.values(room.sensors) : [];

        if (sensors.length === 0 && !room.sensorId) {
            container.innerHTML = '<div class="device-info-item">No sensor data</div>';
            return;
        }

        // Color palette for sensor display
        const sensorColors = ['#3b82f6', '#22c55e', '#f59e0b', '#8b5cf6', '#ef4444', '#06b6d4'];

        // Model labels for hardware info
        const modelLabels = {
            temperature: 'Temp',
            humidity: 'Humidity',
            co2: 'CO2',
            pressure: 'Pressure',
            voc: 'VOC',
            pm25: 'PM2.5'
        };

        // Always show contributing sensors section
        const sensorCardsHtml = sensors.map((sensor, idx) => {
            const color = sensorColors[idx % sensorColors.length];
            const shortId = sensor.deviceId.length > 12 ? sensor.deviceId.slice(-8) : sensor.deviceId;
            // Show original sensor name (not cleaned) so user can identify which physical sensor
            const displayName = sensor.name !== sensor.deviceId ? sensor.name : shortId;

            // Build metric rows with all available metrics
            const metricRows = [];
            if (sensor.temperature != null) metricRows.push(`<span class="sensor-metric-value">${sensor.temperature.toFixed(1)}°C</span>`);
            if (sensor.humidity != null) metricRows.push(`<span class="sensor-metric-value">${sensor.humidity.toFixed(0)}%</span>`);
            if (sensor.co2 != null) metricRows.push(`<span class="sensor-metric-value">${Math.round(sensor.co2)} ppm</span>`);
            if (sensor.pressure != null) metricRows.push(`<span class="sensor-metric-value">${sensor.pressure.toFixed(0)} hPa</span>`);
            if (sensor.voc != null) metricRows.push(`<span class="sensor-metric-value">VOC ${Math.round(sensor.voc)}</span>`);
            if (sensor.pm25 != null) metricRows.push(`<span class="sensor-metric-value">PM2.5 ${sensor.pm25.toFixed(1)}</span>`);

            // Build hardware info chips
            const hwChips = [];
            if (sensor.boardModel) hwChips.push(`<span class="hw-chip">${this.escapeHtml(sensor.boardModel)}</span>`);
            const sensorModels = sensor.sensorModels || {};
            for (const [type, model] of Object.entries(sensorModels)) {
                if (model) {
                    const label = modelLabels[type] || type;
                    hwChips.push(`<span class="hw-chip">${label}: ${model}</span>`);
                }
            }

            return `
                <div class="contributing-sensor-card" style="border-left: 3px solid ${color};">
                    <div class="contributing-sensor-header">
                        <span class="contributing-sensor-name">${this.escapeHtml(displayName)}</span>
                        <span class="contributing-sensor-color" style="background: ${color};"></span>
                    </div>
                    <div class="contributing-sensor-metrics">${metricRows.join(' · ') || 'No readings'}</div>
                    ${hwChips.length > 0 ? `<div class="contributing-sensor-hw">${hwChips.join(' ')}</div>` : ''}
                    <div class="contributing-sensor-id">${shortId}</div>
                </div>
            `;
        }).join('');

        const headerText = sensors.length > 1
            ? `Contributing Sensors (${sensors.length})`
            : 'Sensor Details';

        container.innerHTML = `
            <div class="contributing-sensors-section">
                <div class="contributing-sensors-header">${headerText}</div>
                <div class="contributing-sensors-grid">
                    ${sensorCardsHtml}
                </div>
            </div>
        `;
    }

    // Helper to escape HTML
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Global room sidebar instance
let roomDetailsSidebar = null;

// SensorPanel - Manages sensor selection panel
class SensorPanel {
    constructor() {
        this.panel = document.getElementById('sensorPanel');
        this.overlay = document.getElementById('sensorPanelOverlay');
        this.grid = document.getElementById('sensorPanelGrid');
        this.searchInput = document.getElementById('sensorPanelSearch');
        this.countEl = document.getElementById('sensorPanelCount');
        this.isOpen = false;
        this.allDevices = [];
        this.searchTerm = '';

        this.setupEventListeners();
    }

    setupEventListeners() {
        // Open panel button (gear icon)
        document.getElementById('manageSensorsBtn')?.addEventListener('click', () => this.open());

        // Open panel from empty state
        document.getElementById('emptyStateAddBtn')?.addEventListener('click', () => this.open());

        // Close button
        document.getElementById('sensorPanelClose')?.addEventListener('click', () => this.close());

        // Overlay click closes panel
        this.overlay?.addEventListener('click', () => this.close());

        // Search input
        this.searchInput?.addEventListener('input', (e) => {
            this.searchTerm = e.target.value.toLowerCase();
            this.renderDevices();
        });

        // Escape key closes panel
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });

        // View All button - shows the Overview
        document.getElementById('sensorPanelViewAll')?.addEventListener('click', () => {
            this.close();
            // Show the Overview view
            document.querySelectorAll('.dash-view').forEach(v => v.classList.remove('active'));
            document.getElementById('overviewView')?.classList.add('active');
            // Trigger render
            if (window.app) {
                window.app.renderOverview();
            }
        });
    }

    async open() {
        if (!this.panel || !this.overlay) {
            console.error('Sensor panel elements not found');
            return;
        }
        this.panel.classList.add('open');
        this.overlay.classList.add('visible');
        this.isOpen = true;

        // Load devices if not already loaded
        if (this.allDevices.length === 0) {
            await this.loadDevices();
        } else {
            this.renderDevices();
        }

        this.updateCount();
        this.searchInput?.focus();
    }

    close() {
        this.panel.classList.remove('open');
        this.overlay.classList.remove('visible');
        this.isOpen = false;

        // Trigger dashboard refresh if sensors changed
        if (window.app) {
            window.app.renderMySensors();
        }
    }

    async loadDevices() {
        if (!this.grid) {
            console.error('Sensor panel grid not found');
            return;
        }
        this.grid.innerHTML = '<div class="loading">Loading sensors...</div>';

        try {
            const response = await fetch('/api/devices');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const data = await response.json();
            this.allDevices = data.devices || [];
            this.renderDevices();
        } catch (error) {
            console.error('Failed to fetch devices:', error);
            this.grid.innerHTML = `<div class="loading">Failed to load sensors: ${error.message}</div>`;
        }
    }

    renderDevices() {
        const favorites = JSON.parse(localStorage.getItem('sensorFavorites') || '[]');
        const now = Date.now();

        // Get swarm data from main app sensors (if available)
        const swarmDataMap = {};
        if (window.app && window.app.sensors) {
            window.app.sensors.forEach(s => {
                swarmDataMap[s.deviceId] = {
                    swarm_size: s.swarm_size || 0,
                    swarm_status: s.swarm_status || 'shield',
                    swarm_icon: s.swarm_icon || '🛡️',
                    is_outlier: s.is_outlier || false,
                    outlier_metrics: s.outlier_metrics || []
                };
            });
        }

        // Filter by search term
        let devices = this.allDevices;
        if (this.searchTerm) {
            devices = devices.filter(d => {
                const name = (d.name || d.deviceId || '').toLowerCase();
                const location = (d.geo_subdivision || d.geo_country || '').toLowerCase();
                const id = (d.deviceId || '').toLowerCase();
                return name.includes(this.searchTerm) ||
                       location.includes(this.searchTerm) ||
                       id.includes(this.searchTerm);
            });
        }

        // Helper to check if device is "live" (within freshness threshold)
        const isDeviceLive = (device) => {
            if (!device.last_seen) return false;
            const lastSeenTime = new Date(device.last_seen).getTime();
            const threshold = getFreshnessThreshold(device.data_source);
            return (now - lastSeenTime) < threshold;
        };

        // Sort: favorites first, then live sensors, then by recency
        devices.sort((a, b) => {
            const aFav = favorites.includes(a.deviceId);
            const bFav = favorites.includes(b.deviceId);
            if (aFav && !bFav) return -1;
            if (!aFav && bFav) return 1;

            // Then by live status
            const aLive = isDeviceLive(a);
            const bLive = isDeviceLive(b);
            if (aLive && !bLive) return -1;
            if (!aLive && bLive) return 1;

            // Then by most recent
            const aTime = a.last_seen ? new Date(a.last_seen).getTime() : 0;
            const bTime = b.last_seen ? new Date(b.last_seen).getTime() : 0;
            return bTime - aTime;
        });

        if (devices.length === 0) {
            this.grid.innerHTML = '<div class="loading">No sensors found</div>';
            return;
        }

        this.grid.innerHTML = devices.map(device => {
            const isSelected = favorites.includes(device.deviceId);
            const isLive = isDeviceLive(device);
            const name = device.name || device.deviceId;
            const location = [device.geo_subdivision, device.geo_country].filter(Boolean).join(', ') || 'Unknown location';
            const temp = device.temperature != null ? `${device.temperature.toFixed(1)}°` : '--';
            const humidity = device.humidity != null ? `${device.humidity.toFixed(0)}%` : '';

            // Format deployment type (indoor/outdoor/mixed/etc)
            const deploymentType = device.deployment_type || '';
            const deploymentLabel = deploymentType ? deploymentType.charAt(0).toUpperCase() + deploymentType.slice(1).toLowerCase() : '';
            const deploymentClass = deploymentType ? deploymentType.toLowerCase() : '';

            // Get swarm data for this device
            const swarmData = swarmDataMap[device.deviceId] || {};
            const swarmSize = swarmData.swarm_size || 0;
            const swarmIcon = swarmData.swarm_icon || '';
            const isOutlier = swarmData.is_outlier || false;
            const outlierMetrics = swarmData.outlier_metrics || [];

            // Format last seen time
            let lastSeenText = '';
            if (device.last_seen) {
                const lastSeenTime = new Date(device.last_seen).getTime();
                const ageMs = now - lastSeenTime;
                const ageMins = Math.floor(ageMs / 60000);
                const ageHours = Math.floor(ageMins / 60);
                const ageDays = Math.floor(ageHours / 24);

                if (ageMins < 1) lastSeenText = 'just now';
                else if (ageMins < 60) lastSeenText = `${ageMins}m ago`;
                else if (ageHours < 24) lastSeenText = `${ageHours}h ago`;
                else lastSeenText = `${ageDays}d ago`;
            }

            // Swarm badge HTML (only show if swarm size > 0)
            const swarmBadgeHtml = swarmSize > 0 ? `<span class="sensor-swarm-badge" title="Swarm: ${swarmSize} sensors">${swarmIcon}${swarmSize}</span>` : '';

            // Outlier warning HTML
            const outlierHtml = isOutlier ? `<span class="sensor-outlier-warning" title="Outlier detected for: ${outlierMetrics.join(', ')}">⚠️</span>` : '';

            return `
                <div class="sensor-panel-card ${isSelected ? 'selected' : ''} ${isLive ? 'live' : 'stale'} ${isOutlier ? 'outlier' : ''}" data-device-id="${device.deviceId}">
                    <span class="sensor-star"><svg width="14" height="14" viewBox="0 0 24 24" fill="${isSelected ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg></span>
                    <div class="sensor-info">
                        <div class="sensor-name">${this.escapeHtml(name)}${deploymentLabel ? `<span class="sensor-deployment-badge ${deploymentClass}">${deploymentLabel}</span>` : ''}${swarmBadgeHtml}${outlierHtml}</div>
                        <div class="sensor-location">${this.escapeHtml(location)}${lastSeenText ? ` · ${lastSeenText}` : ''}</div>
                    </div>
                    <div class="sensor-reading">
                        <div class="sensor-temp">${temp}</div>
                        ${humidity ? `<div class="sensor-humidity">${humidity}</div>` : ''}
                    </div>
                </div>
            `;
        }).join('');

        // Add click handlers
        this.grid.querySelectorAll('.sensor-panel-card').forEach(card => {
            card.addEventListener('click', () => {
                const deviceId = card.dataset.deviceId;
                this.toggleDevice(deviceId, card);
            });
        });
    }

    toggleDevice(deviceId, cardEl) {
        const favorites = JSON.parse(localStorage.getItem('sensorFavorites') || '[]');
        const index = favorites.indexOf(deviceId);

        if (index === -1) {
            favorites.push(deviceId);
            cardEl.classList.add('selected');
            cardEl.querySelector('.sensor-star').innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>';
        } else {
            favorites.splice(index, 1);
            cardEl.classList.remove('selected');
            cardEl.querySelector('.sensor-star').innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>';
        }

        localStorage.setItem('sensorFavorites', JSON.stringify(favorites));
        this.updateCount();

        // Update the current location's sensor list
        if (locationManager) {
            locationManager.updateLocationSensors(favorites);
        }
    }

    updateCount() {
        const favorites = JSON.parse(localStorage.getItem('sensorFavorites') || '[]');
        if (this.countEl) {
            this.countEl.textContent = `${favorites.length} selected`;
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Global sensor panel instance
let sensorPanel = null;

// LocationManager - Manages locations/dwellings with per-location sensor sets
class LocationManager {
    constructor() {
        this.dropdown = document.getElementById('locationDropdown');
        this.dropdownList = document.getElementById('locationDropdownList');
        this.locationBtn = document.getElementById('locationSelector');
        this.locationName = document.getElementById('dashboardLocationName');
        this.locationSubtitle = document.getElementById('dashboardLocationSubtitle');
        this.addModal = document.getElementById('addLocationModal');
        this.addInput = document.getElementById('newLocationInput');

        // Empty state location selector elements
        this.emptyStateLocation = document.getElementById('emptyStateLocation');
        this.emptyStateLocationBtn = document.getElementById('emptyStateLocationBtn');
        this.emptyStateLocationName = document.getElementById('emptyStateLocationName');
        this.emptyStateDropdown = document.getElementById('emptyStateLocationDropdown');
        this.emptyStateDropdownList = document.getElementById('emptyStateLocationList');

        this.locations = this.loadLocations();
        this.currentLocationId = this.loadCurrentLocation();

        this.setupEventListeners();
        this.renderDropdown();
        this.updateDisplay();
    }

    loadLocations() {
        const stored = localStorage.getItem('wesenseLocations');
        if (stored) {
            return JSON.parse(stored);
        }
        // Default location - import any existing favorites
        const existingFavorites = JSON.parse(localStorage.getItem('sensorFavorites') || '[]');
        const defaultLocations = [
            { id: 'home', name: 'Home', sensors: existingFavorites }
        ];
        localStorage.setItem('wesenseLocations', JSON.stringify(defaultLocations));
        return defaultLocations;
    }

    saveLocations() {
        localStorage.setItem('wesenseLocations', JSON.stringify(this.locations));
    }

    loadCurrentLocation() {
        const stored = localStorage.getItem('wesenseCurrentLocation');
        if (stored && this.locations.find(l => l.id === stored)) {
            return stored;
        }
        return this.locations[0]?.id || 'home';
    }

    saveCurrentLocation() {
        localStorage.setItem('wesenseCurrentLocation', this.currentLocationId);
    }

    getCurrentLocation() {
        return this.locations.find(l => l.id === this.currentLocationId) || this.locations[0];
    }

    setupEventListeners() {
        // Toggle dropdown
        this.locationBtn?.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleDropdown();
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.dropdown?.contains(e.target) && !this.locationBtn?.contains(e.target)) {
                this.closeDropdown();
            }
        });

        // Add location button
        document.getElementById('addLocationBtn')?.addEventListener('click', () => {
            this.closeDropdown();
            this.openAddModal();
        });

        // Modal events
        document.getElementById('cancelAddLocation')?.addEventListener('click', () => {
            this.closeAddModal();
        });

        document.getElementById('confirmAddLocation')?.addEventListener('click', () => {
            this.addLocation();
        });

        this.addInput?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                this.addLocation();
            } else if (e.key === 'Escape') {
                this.closeAddModal();
            }
        });

        // Close modal on overlay click
        this.addModal?.addEventListener('click', (e) => {
            if (e.target === this.addModal) {
                this.closeAddModal();
            }
        });

        // Export button
        document.getElementById('exportLocationsBtn')?.addEventListener('click', () => {
            this.exportBackup();
        });

        // Import button
        document.getElementById('importLocationsBtn')?.addEventListener('click', () => {
            document.getElementById('importFileInput')?.click();
        });

        // Import file input
        document.getElementById('importFileInput')?.addEventListener('change', (e) => {
            const file = e.target.files?.[0];
            if (file) {
                this.importBackup(file);
                e.target.value = ''; // Reset so same file can be selected again
            }
        });

        // Empty state location dropdown toggle
        this.emptyStateLocationBtn?.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleEmptyStateDropdown();
        });

        // Close empty state dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.emptyStateDropdown?.contains(e.target) && !this.emptyStateLocationBtn?.contains(e.target)) {
                this.closeEmptyStateDropdown();
            }
        });
    }

    toggleDropdown() {
        this.dropdown?.classList.toggle('open');
    }

    closeDropdown() {
        this.dropdown?.classList.remove('open');
    }

    toggleEmptyStateDropdown() {
        this.emptyStateDropdown?.classList.toggle('open');
    }

    closeEmptyStateDropdown() {
        this.emptyStateDropdown?.classList.remove('open');
    }

    openAddModal() {
        this.addModal?.classList.add('open');
        this.addInput.value = '';
        setTimeout(() => this.addInput?.focus(), 100);
    }

    closeAddModal() {
        this.addModal?.classList.remove('open');
    }

    renderDropdown() {
        if (!this.dropdownList) return;

        const dropdownHtml = this.locations.map(loc => {
            const isActive = loc.id === this.currentLocationId;
            const canDelete = this.locations.length > 1;

            return `
                <div class="location-dropdown-item ${isActive ? 'active' : ''}" data-location-id="${loc.id}">
                    <span class="location-item-name">${this.escapeHtml(loc.name)}</span>
                    ${canDelete ? `<button class="location-delete-btn" data-delete-id="${loc.id}" title="Delete location">×</button>` : ''}
                </div>
            `;
        }).join('');

        this.dropdownList.innerHTML = dropdownHtml;

        // Add click handlers for selecting locations
        this.dropdownList.querySelectorAll('.location-dropdown-item').forEach(item => {
            item.addEventListener('click', (e) => {
                // Don't trigger if clicking delete button
                if (e.target.classList.contains('location-delete-btn')) return;

                const locationId = item.dataset.locationId;
                this.selectLocation(locationId);
            });
        });

        // Add click handlers for delete buttons
        this.dropdownList.querySelectorAll('.location-delete-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const locationId = btn.dataset.deleteId;
                this.deleteLocation(locationId);
            });
        });

        // Also render empty state dropdown (with delete buttons like main dropdown)
        if (this.emptyStateDropdownList) {
            this.emptyStateDropdownList.innerHTML = this.locations.map(loc => {
                const isActive = loc.id === this.currentLocationId;
                const canDelete = this.locations.length > 1;
                return `
                    <div class="location-dropdown-item ${isActive ? 'active' : ''}" data-location-id="${loc.id}">
                        <span class="location-item-name">${this.escapeHtml(loc.name)}</span>
                        ${canDelete ? `<button class="location-delete-btn" data-delete-id="${loc.id}" title="Delete location">×</button>` : ''}
                    </div>
                `;
            }).join('');

            // Add click handlers for empty state dropdown
            this.emptyStateDropdownList.querySelectorAll('.location-dropdown-item').forEach(item => {
                item.addEventListener('click', (e) => {
                    if (e.target.classList.contains('location-delete-btn')) return;
                    const locationId = item.dataset.locationId;
                    this.selectLocation(locationId);
                });
            });

            // Add click handlers for delete buttons in empty state dropdown
            this.emptyStateDropdownList.querySelectorAll('.location-delete-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const locationId = btn.dataset.deleteId;
                    this.deleteLocation(locationId);
                });
            });
        }

        // Show empty state location selector only if there are multiple locations
        this.updateEmptyStateLocationVisibility();
    }

    updateEmptyStateLocationVisibility() {
        if (this.emptyStateLocation) {
            // Show if there are multiple locations
            this.emptyStateLocation.style.display = this.locations.length > 1 ? 'block' : 'none';
        }
    }

    selectLocation(locationId) {
        const location = this.locations.find(l => l.id === locationId);
        if (!location) return;

        this.currentLocationId = locationId;
        this.saveCurrentLocation();
        this.closeDropdown();
        this.closeEmptyStateDropdown();
        this.renderDropdown();
        this.updateDisplay();

        // Update sensors for this location
        this.applySensorsForLocation(location);

        // Trigger dashboard refresh
        if (window.app) {
            window.app.refreshDashboard();
        }
    }

    applySensorsForLocation(location) {
        // Each location can have its own set of favorite sensors
        // For now, we'll store per-location sensor favorites
        const locationSensors = location.sensors || [];
        localStorage.setItem('sensorFavorites', JSON.stringify(locationSensors));

        // Update sensor panel count if open
        if (sensorPanel) {
            sensorPanel.updateCount();
        }
    }

    updateDisplay() {
        const location = this.getCurrentLocation();
        if (this.locationName) {
            this.locationName.textContent = location?.name || 'Home';
        }

        // Also update empty state location name
        if (this.emptyStateLocationName) {
            this.emptyStateLocationName.textContent = location?.name || 'Home';
        }

        // Update subtitle with sensor count
        const sensors = location?.sensors || [];
        if (this.locationSubtitle) {
            if (sensors.length === 0) {
                this.locationSubtitle.textContent = 'No sensors configured';
            } else {
                this.locationSubtitle.textContent = `${sensors.length} sensor${sensors.length !== 1 ? 's' : ''}`;
            }
        }
    }

    addLocation() {
        const name = this.addInput?.value?.trim();
        if (!name) return;

        // Generate unique ID
        const id = 'loc_' + Date.now().toString(36);

        const newLocation = {
            id,
            name,
            sensors: []
        };

        this.locations.push(newLocation);
        this.saveLocations();
        this.closeAddModal();
        this.renderDropdown();

        // Auto-select the new location
        this.selectLocation(id);
    }

    deleteLocation(locationId) {
        // Prevent deleting the last location
        if (this.locations.length <= 1) return;

        const location = this.locations.find(l => l.id === locationId);
        if (!location) return;

        // Confirm deletion
        if (!confirm(`Delete "${location.name}"? This cannot be undone.`)) return;

        // Remove the location
        this.locations = this.locations.filter(l => l.id !== locationId);
        this.saveLocations();

        // If we deleted the current location, switch to first available
        if (this.currentLocationId === locationId) {
            this.currentLocationId = this.locations[0].id;
            this.saveCurrentLocation();
            this.applySensorsForLocation(this.locations[0]);
        }

        this.renderDropdown();
        this.updateDisplay();

        // Refresh dashboard
        if (window.app) {
            window.app.refreshDashboard();
        }
    }

    // Called when sensors are updated in the sensor panel
    updateLocationSensors(sensors) {
        const location = this.getCurrentLocation();
        if (location) {
            location.sensors = sensors;
            this.saveLocations();
            this.updateDisplay();
        }
    }

    exportBackup() {
        this.closeDropdown();

        const backup = {
            version: 1,
            exportedAt: new Date().toISOString(),
            locations: this.locations,
            currentLocationId: this.currentLocationId
        };

        const json = JSON.stringify(backup, null, 2);
        const blob = new Blob([json], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        // Generate filename with date
        const date = new Date().toISOString().split('T')[0];
        const filename = `wesense-backup-${date}.json`;

        // Trigger download
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    async importBackup(file) {
        this.closeDropdown();

        try {
            const text = await file.text();
            const backup = JSON.parse(text);

            // Validate backup structure
            if (!backup.locations || !Array.isArray(backup.locations)) {
                throw new Error('Invalid backup file: missing locations array');
            }

            // Validate each location has required fields
            for (const loc of backup.locations) {
                if (!loc.id || !loc.name) {
                    throw new Error('Invalid backup file: location missing id or name');
                }
                if (!Array.isArray(loc.sensors)) {
                    loc.sensors = []; // Default to empty if missing
                }
            }

            // Confirm import
            const locationCount = backup.locations.length;
            const sensorCount = backup.locations.reduce((sum, loc) => sum + loc.sensors.length, 0);
            const message = `Import ${locationCount} location${locationCount !== 1 ? 's' : ''} with ${sensorCount} total sensor${sensorCount !== 1 ? 's' : ''}?\n\nThis will replace your current settings.`;

            if (!confirm(message)) {
                return;
            }

            // Apply backup
            this.locations = backup.locations;
            this.saveLocations();

            // Set current location (use backed up one if valid, otherwise first)
            if (backup.currentLocationId && this.locations.find(l => l.id === backup.currentLocationId)) {
                this.currentLocationId = backup.currentLocationId;
            } else {
                this.currentLocationId = this.locations[0].id;
            }
            this.saveCurrentLocation();

            // Apply sensors for current location
            this.applySensorsForLocation(this.getCurrentLocation());

            // Refresh UI
            this.renderDropdown();
            this.updateDisplay();

            // Refresh dashboard
            if (window.app) {
                window.app.refreshDashboard();
            }

            alert('Backup restored successfully!');

        } catch (error) {
            console.error('Import failed:', error);
            alert(`Failed to import backup: ${error.message}`);
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Global location manager instance
let locationManager = null;

// NewDashboardLayout - Manages the redesigned dashboard with metric cards
class NewDashboardLayout {
    constructor() {
        this.container = document.getElementById('newDashboardLayout');
        this.currentTimeRange = '24h';
        this.deviceIds = [];
        this.outdoorDeviceIds = []; // Outdoor-only devices for weather metrics and trends
        this.sparklineCharts = {};
        this.isActive = true; // New layout is now default

        // Initialize ambient effects
        this.fogEffect = new FogEffect('heroAmbientCanvas');
        this.rainEffect = new RainEffect('heroAmbientCanvas');
        this.insightEngine = new InsightEngine();

        // Store current conditions for effects and insights
        this.currentConditions = {
            temperature: null,
            humidity: null,
            pressure: null,
            pressureTrend: null,
            co2: null,
            pm25: null,
            aqi: null,
            iaqi: null
        };

        // Global swarm view state (affects all metrics and hero bar)
        // Load from localStorage for persistence
        this.globalSwarmView = localStorage.getItem('globalSwarmView') === 'true';

        // Swarm toggle state - per metric (now controlled by global toggle)
        this.swarmToggleState = {
            temperature: this.globalSwarmView,
            humidity: this.globalSwarmView,
            pressure: this.globalSwarmView
        };

        // Store swarm data for toggling
        this.swarmData = {
            temperature: { median: null, myValue: null, available: false },
            humidity: { median: null, myValue: null, available: false },
            pressure: { median: null, myValue: null, available: false }
        };

        // Track if swarm data is available for hero toggle
        this.hasAnySwarmData = false;

        // Small inline SVG icons for sensor card values (14px)
        // Each icon is wrapped in a span with data-tooltip for CSS-based instant tooltips
        this.metricIcons = {
            temperature: '<span class="icon-tooltip" data-tooltip="Temperature"><svg class="metric-icon temp" viewBox="0 0 24 24" fill="none"><rect x="9" y="3" width="6" height="13" rx="3" stroke="currentColor" stroke-width="1.5"/><circle cx="12" cy="19" r="3" fill="currentColor"/><rect x="10.5" y="7" width="3" height="9" rx="1.5" fill="currentColor"/></svg></span>',
            humidity: '<span class="icon-tooltip" data-tooltip="Humidity"><svg class="metric-icon humidity" viewBox="0 0 24 24" fill="none"><path d="M12 3C12 3 5 11 5 16c0 3.9 3.1 7 7 7s7-3.1 7-7c0-5-7-13-7-13z" stroke="currentColor" stroke-width="1.5" fill="none"/><path d="M12 7c0 0-4 5-4 9c0 2.2 1.8 4 4 4s4-1.8 4-4c0-4-4-9-4-9z" fill="currentColor" opacity="0.3"/></svg></span>',
            pressure: '<span class="icon-tooltip" data-tooltip="Pressure"><svg class="metric-icon pressure" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="9" stroke="currentColor" stroke-width="1.5"/><circle cx="12" cy="12" r="1.5" fill="currentColor"/><line x1="12" y1="12" x2="16" y2="8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><line x1="12" y1="5" x2="12" y2="7" stroke="currentColor" stroke-width="1"/></svg></span>',
            co2: '<span class="icon-tooltip" data-tooltip="Carbon Dioxide"><svg class="metric-icon co2" viewBox="0 0 24 24" fill="none"><circle cx="8" cy="12" r="4" stroke="currentColor" stroke-width="1.5"/><circle cx="16" cy="8" r="3" stroke="currentColor" stroke-width="1.5"/><circle cx="16" cy="16" r="3" stroke="currentColor" stroke-width="1.5"/><line x1="11" y1="10" x2="13" y2="9" stroke="currentColor" stroke-width="1.5"/><line x1="11" y1="14" x2="13" y2="15" stroke="currentColor" stroke-width="1.5"/></svg></span>',
            // PM icons - particle cloud with number in center
            pm1: '<span class="icon-tooltip" data-tooltip="PM1 Particulates ≤1µm"><svg class="metric-icon pm" viewBox="0 0 24 24" fill="none"><circle cx="5" cy="8" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="19" cy="7" r="1" fill="currentColor" opacity="0.3"/><circle cx="4" cy="16" r="1" fill="currentColor" opacity="0.3"/><circle cx="20" cy="15" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="8" cy="5" r="1" fill="currentColor" opacity="0.3"/><circle cx="16" cy="19" r="1" fill="currentColor" opacity="0.3"/><text x="12" y="16" text-anchor="middle" font-size="11" font-weight="bold" fill="currentColor">1</text></svg></span>',
            pm25: '<span class="icon-tooltip" data-tooltip="PM2.5 Particulates ≤2.5µm"><svg class="metric-icon pm" viewBox="0 0 24 24" fill="none"><circle cx="5" cy="8" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="19" cy="7" r="1" fill="currentColor" opacity="0.3"/><circle cx="4" cy="16" r="1" fill="currentColor" opacity="0.3"/><circle cx="20" cy="15" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="8" cy="5" r="1" fill="currentColor" opacity="0.3"/><circle cx="16" cy="19" r="1" fill="currentColor" opacity="0.3"/><text x="12" y="16" text-anchor="middle" font-size="9" font-weight="bold" fill="currentColor">2.5</text></svg></span>',
            pm10: '<span class="icon-tooltip" data-tooltip="PM10 Particulates ≤10µm"><svg class="metric-icon pm" viewBox="0 0 24 24" fill="none"><circle cx="5" cy="8" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="19" cy="7" r="1" fill="currentColor" opacity="0.3"/><circle cx="4" cy="16" r="1" fill="currentColor" opacity="0.3"/><circle cx="20" cy="15" r="1.5" fill="currentColor" opacity="0.4"/><circle cx="8" cy="5" r="1" fill="currentColor" opacity="0.3"/><circle cx="16" cy="19" r="1" fill="currentColor" opacity="0.3"/><text x="12" y="16" text-anchor="middle" font-size="9" font-weight="bold" fill="currentColor">10</text></svg></span>',
            // VOC icon - wavy vapor lines
            voc: '<span class="icon-tooltip" data-tooltip="Volatile Organic Compounds"><svg class="metric-icon voc" viewBox="0 0 24 24" fill="none"><path d="M6 18c0-2 2-3 2-5s-2-3-2-5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M12 18c0-2 2-3 2-5s-2-3-2-5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M18 18c0-2 2-3 2-5s-2-3-2-5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg></span>'
        };

        // Room type icons for location cards (20px display)
        this.roomTypeIcons = {
            // Kitchen - pot with handle
            'kitchen': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M5 11h14v7a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3v-7z" stroke="currentColor" stroke-width="1.5"/><path d="M5 11c0-2 1-4 7-4s7 2 7 4" stroke="currentColor" stroke-width="1.5"/><path d="M12 7V4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="12" cy="3" r="1" fill="currentColor"/></svg>',
            // Bathroom - bathtub
            'bathroom': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 12h16v5a3 3 0 0 1-3 3H7a3 3 0 0 1-3-3v-5z" stroke="currentColor" stroke-width="1.5"/><path d="M4 12V6a2 2 0 0 1 2-2h1a2 2 0 0 1 2 2v1" stroke="currentColor" stroke-width="1.5"/><circle cx="7" cy="8" r="1.5" fill="currentColor"/><path d="M6 20v1M18 20v1" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Bedroom - simple bed
            'bedroom': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 18v-5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v5" stroke="currentColor" stroke-width="1.5"/><path d="M3 18h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 11V8a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v3" stroke="currentColor" stroke-width="1.5"/><rect x="5" y="8" width="4" height="3" rx="1" fill="currentColor" opacity="0.3"/><path d="M4 18v2M20 18v2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Master bedroom - bed with crown
            'master-bedroom': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 18v-5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v5" stroke="currentColor" stroke-width="1.5"/><path d="M3 18h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 11V9a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v2" stroke="currentColor" stroke-width="1.5"/><rect x="5" y="8" width="4" height="3" rx="1" fill="currentColor" opacity="0.3"/><path d="M4 18v2M20 18v2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M9 4l3-2 3 2M9 4v2h6V4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
            // Guest bedroom - bed with star
            'guest-bedroom': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 18v-5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v5" stroke="currentColor" stroke-width="1.5"/><path d="M3 18h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 11V9a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v2" stroke="currentColor" stroke-width="1.5"/><rect x="5" y="8" width="4" height="3" rx="1" fill="currentColor" opacity="0.3"/><path d="M4 18v2M20 18v2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M12 2l.9 2h2.1l-1.7 1.3.6 2.2L12 6.2l-1.9 1.3.6-2.2L9 4h2.1z" fill="currentColor" stroke="currentColor" stroke-width="0.5"/></svg>',
            // Living room - sofa
            'living-room': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 14v-2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v2" stroke="currentColor" stroke-width="1.5"/><path d="M2 14a2 2 0 0 1 2-2v5h16v-5a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2v-3z" stroke="currentColor" stroke-width="1.5"/><path d="M6 17v-4M18 17v-4" stroke="currentColor" stroke-width="1.5"/><path d="M5 19v1M19 19v1" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Dining room - table with chairs
            'dining-room': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="5" y="10" width="14" height="2" rx="0.5" stroke="currentColor" stroke-width="1.5"/><path d="M7 12v7M17 12v7" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M3 8v9a1 1 0 0 0 1 1h1v-7H4a1 1 0 0 1-1-1V8a1 1 0 0 1 1-1h1v3" stroke="currentColor" stroke-width="1.5"/><path d="M21 8v9a1 1 0 0 1-1 1h-1v-7h1a1 1 0 0 0 1-1V8a1 1 0 0 0-1-1h-1v3" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Office - desk with monitor
            'office': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="6" y="4" width="12" height="9" rx="1" stroke="currentColor" stroke-width="1.5"/><path d="M12 13v3M9 16h6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><rect x="3" y="18" width="18" height="2" rx="0.5" stroke="currentColor" stroke-width="1.5"/><rect x="8" y="6" width="8" height="5" rx="0.5" fill="currentColor" opacity="0.2"/></svg>',
            // Hallway - door/archway
            'hallway': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M6 4h12v16H6V4z" stroke="currentColor" stroke-width="1.5"/><path d="M6 4c0 0 3 2 6 2s6-2 6-2" stroke="currentColor" stroke-width="1.5"/><circle cx="15" cy="13" r="1" fill="currentColor"/></svg>',
            // Laundry - washing machine
            'laundry': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="4" y="3" width="16" height="18" rx="2" stroke="currentColor" stroke-width="1.5"/><circle cx="12" cy="13" r="5" stroke="currentColor" stroke-width="1.5"/><circle cx="12" cy="13" r="2.5" stroke="currentColor" stroke-width="1" opacity="0.5"/><circle cx="7" cy="6" r="1" fill="currentColor"/><circle cx="10" cy="6" r="1" fill="currentColor"/></svg>',
            // Garage - car/garage door
            'garage': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 10l9-6 9 6v10H3V10z" stroke="currentColor" stroke-width="1.5"/><path d="M6 20v-7h12v7" stroke="currentColor" stroke-width="1.5"/><path d="M6 15h12M6 17h12" stroke="currentColor" stroke-width="1" opacity="0.5"/></svg>',
            // Basement - stairs going down
            'basement': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 6h4v4h4v4h4v4h4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M4 6v14h16" stroke="currentColor" stroke-width="1.5"/><path d="M17 18l3-3m0 3l-3-3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Attic - roof/stairs up
            'attic': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 12l8-8 8 8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M6 10v10h12V10" stroke="currentColor" stroke-width="1.5"/><path d="M10 20v-6h4v6" stroke="currentColor" stroke-width="1.5"/><path d="M12 7l2-2m-2 2l-2-2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Shed - small house
            'shed': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 11l8-7 8 7" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M6 10v10h12V10" stroke="currentColor" stroke-width="1.5"/><rect x="9" y="14" width="6" height="6" stroke="currentColor" stroke-width="1.5"/><path d="M12 14v6M9 17h6" stroke="currentColor" stroke-width="1"/></svg>',
            // Patio - outdoor table with umbrella
            'patio': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M12 3v5" stroke="currentColor" stroke-width="1.5"/><path d="M5 8c0-1 3-3 7-3s7 2 7 3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><ellipse cx="12" cy="14" rx="6" ry="1" stroke="currentColor" stroke-width="1.5"/><path d="M12 14v6" stroke="currentColor" stroke-width="1.5"/><path d="M8 20h8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Server room - server rack
            'server-room': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="5" y="3" width="14" height="6" rx="1" stroke="currentColor" stroke-width="1.5"/><rect x="5" y="10" width="14" height="6" rx="1" stroke="currentColor" stroke-width="1.5"/><circle cx="8" cy="6" r="1" fill="currentColor"/><circle cx="8" cy="13" r="1" fill="currentColor"/><path d="M11 6h5M11 13h5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M8 18v3M16 18v3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>',
            // Nursery - crib
            'nursery': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="4" y="8" width="16" height="10" rx="1" stroke="currentColor" stroke-width="1.5"/><path d="M4 11h16" stroke="currentColor" stroke-width="1.5"/><path d="M7 11v7M10 11v7M14 11v7M17 11v7" stroke="currentColor" stroke-width="1" opacity="0.5"/><path d="M5 18v2M19 18v2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="8" cy="5" r="2" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Gym - dumbbell
            'gym': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="2" y="9" width="3" height="6" rx="0.5" stroke="currentColor" stroke-width="1.5"/><rect x="5" y="7" width="2" height="10" rx="0.5" stroke="currentColor" stroke-width="1.5"/><rect x="19" y="9" width="3" height="6" rx="0.5" stroke="currentColor" stroke-width="1.5"/><rect x="17" y="7" width="2" height="10" rx="0.5" stroke="currentColor" stroke-width="1.5"/><path d="M7 12h10" stroke="currentColor" stroke-width="2"/></svg>',
            // Unknown - generic room with question mark
            'unknown': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="3" y="5" width="18" height="14" rx="2" stroke="currentColor" stroke-width="1.5"/><path d="M12 9c-1 0-2 .5-2 1.5s1 1.5 2 2v1" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="12" cy="16" r="0.5" fill="currentColor"/></svg>'
        };

        // Outdoor area type icons (20px display)
        this.areaTypeIcons = {
            // Balcony - railing with view
            'balcony': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 12h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 12v6M9 12v6M15 12v6M19 12v6" stroke="currentColor" stroke-width="1.5"/><path d="M3 18h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 12V8a7 7 0 0 1 14 0v4" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Yard/Garden - tree and grass
            'yard': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M12 20v-8" stroke="currentColor" stroke-width="1.5"/><circle cx="12" cy="8" r="5" stroke="currentColor" stroke-width="1.5"/><path d="M4 20c1-2 2-3 4-3s3 1 4 3" stroke="currentColor" stroke-width="1.5"/><path d="M12 20c1-2 2-3 4-3s3 1 4 3" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Rooftop - roof with antenna
            'rooftop': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 12l9-8 9 8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M5 11v9h14v-9" stroke="currentColor" stroke-width="1.5"/><path d="M16 6v-3M14 5h4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="16" cy="3" r="1" fill="currentColor"/></svg>',
            // Driveway - car path
            'driveway': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M6 4v16M18 4v16" stroke="currentColor" stroke-width="1.5" stroke-dasharray="3 3"/><path d="M12 6v3M12 12v3M12 18v2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><rect x="8" y="8" width="8" height="5" rx="1" stroke="currentColor" stroke-width="1.5"/><circle cx="9" cy="14" r="1" fill="currentColor"/><circle cx="15" cy="14" r="1" fill="currentColor"/></svg>',
            // Paddock/Field - fence
            'paddock': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 8h18M3 14h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 6v12M10 6v12M14 6v12M19 6v12" stroke="currentColor" stroke-width="1.5"/><path d="M5 6l2-3 2 3M14 6l2-3 2 3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
            // Deck - wooden planks
            'deck': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 8h16M4 12h16M4 16h16" stroke="currentColor" stroke-width="1.5"/><path d="M6 8v10M10 8v10M14 8v10M18 8v10" stroke="currentColor" stroke-width="1" opacity="0.5"/><path d="M5 8V5h14v3" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Porch - covered entrance
            'porch': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 10l8-6 8 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M4 10v10h16V10" stroke="currentColor" stroke-width="1.5"/><path d="M6 10v10M18 10v10" stroke="currentColor" stroke-width="1.5"/><rect x="9" y="13" width="6" height="7" stroke="currentColor" stroke-width="1.5"/><circle cx="14" cy="17" r="0.5" fill="currentColor"/></svg>',
            // Pool - water waves
            'pool': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 12c2 0 2-2 4-2s2 2 4 2 2-2 4-2 2 2 4 2" stroke="currentColor" stroke-width="1.5"/><path d="M3 16c2 0 2-2 4-2s2 2 4 2 2-2 4-2 2 2 4 2" stroke="currentColor" stroke-width="1.5"/><rect x="4" y="6" width="16" height="14" rx="2" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Greenhouse - glass house with plants
            'greenhouse': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 10l8-6 8 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M4 10v10h16V10" stroke="currentColor" stroke-width="1.5"/><path d="M4 14h16M12 10v10" stroke="currentColor" stroke-width="1"/><path d="M9 16v4M15 16v4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><circle cx="9" cy="14" r="1.5" stroke="currentColor" stroke-width="1"/><circle cx="15" cy="14" r="1.5" stroke="currentColor" stroke-width="1"/></svg>',
            // Carport - covered parking
            'carport': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 10h16" stroke="currentColor" stroke-width="1.5"/><path d="M6 10v10M18 10v10" stroke="currentColor" stroke-width="1.5"/><path d="M2 10l10-6 10 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><rect x="8" y="14" width="8" height="4" rx="1" stroke="currentColor" stroke-width="1.5"/><circle cx="10" cy="18" r="1" fill="currentColor"/><circle cx="14" cy="18" r="1" fill="currentColor"/></svg>',
            // Terrace - tiered outdoor
            'terrace': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 18h16M4 14h12M4 10h8" stroke="currentColor" stroke-width="1.5"/><path d="M4 10v8M12 10v8M16 14v4M20 18v2" stroke="currentColor" stroke-width="1.5"/><circle cx="8" cy="7" r="2" stroke="currentColor" stroke-width="1.5"/><path d="M8 9v1" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Veranda - covered with columns
            'veranda': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M3 8h18" stroke="currentColor" stroke-width="1.5"/><path d="M5 8v12M9 8v12M15 8v12M19 8v12" stroke="currentColor" stroke-width="1.5"/><path d="M3 8l9-4 9 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M3 20h18" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Courtyard - enclosed outdoor
            'courtyard': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><rect x="4" y="4" width="16" height="16" stroke="currentColor" stroke-width="1.5"/><rect x="8" y="8" width="8" height="8" stroke="currentColor" stroke-width="1.5" stroke-dasharray="2 2"/><circle cx="12" cy="12" r="2" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Pergola - open roof structure
            'pergola': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><path d="M4 6h16M4 10h16M4 14h16" stroke="currentColor" stroke-width="1.5"/><path d="M6 6v14M18 6v14" stroke="currentColor" stroke-width="1.5"/><path d="M6 20h12" stroke="currentColor" stroke-width="1.5"/></svg>',
            // Unknown outdoor - sun icon
            'unknown': '<svg class="room-type-icon" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="4" stroke="currentColor" stroke-width="1.5"/><path d="M12 3v2M12 19v2M3 12h2M19 12h2M5.6 5.6l1.4 1.4M17 17l1.4 1.4M5.6 18.4l1.4-1.4M17 7l1.4-1.4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>'
        };

        this.initEventHandlers();
        this.startClockUpdate();

        // Handle window resize - debounced to avoid excessive updates
        let resizeTimeout;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(() => this.handleResize(), 150);
        });
    }

    handleResize() {
        // Force all sparkline charts to resize
        Object.values(this.sparklineCharts).forEach(chart => {
            if (chart && typeof chart.resize === 'function') {
                chart.resize();
            }
        });
    }

    initEventHandlers() {
        // Metric cards - click to open sidebar
        document.querySelectorAll('.metric-card, .air-quality-card, .air-quality-subblock').forEach(card => {
            card.addEventListener('click', () => {
                const metric = card.dataset.metric;
                if (metric && detailsSidebar) {
                    // Get current value from the card
                    let currentValue = '--';
                    // Outdoor metrics (temperature, humidity, pressure, pm2_5) use outdoor-only device IDs
                    const outdoorMetrics = ['temperature', 'humidity', 'pressure', 'pm2_5'];
                    const deviceIdsToUse = outdoorMetrics.includes(metric) ? this.outdoorDeviceIds : this.deviceIds;

                    // Map metric names to aggregate keys (pm2_5 -> pm25, co2 -> co2)
                    const aggregateKeyMap = { 'pm2_5': 'pm25', 'co2': 'co2' };
                    const aggregateKey = aggregateKeyMap[metric] || metric;

                    if (metric === 'temperature') {
                        currentValue = document.getElementById('tempCardValue')?.textContent || '--';
                    } else if (metric === 'humidity') {
                        currentValue = document.getElementById('humidityCardValue')?.textContent || '--';
                    } else if (metric === 'pressure') {
                        currentValue = document.getElementById('pressureCardValue')?.textContent || '--';
                    } else if (metric === 'pm2_5') {
                        // Get outdoor PM2.5 directly from aggregates
                        const pm25Avg = this.currentAggregates?.outdoor?.pm25?.avg;
                        currentValue = pm25Avg != null ? pm25Avg.toFixed(1) : '--';
                    } else if (metric === 'co2') {
                        // Get indoor CO2 directly from aggregates
                        const co2Avg = this.currentAggregates?.indoor?.co2?.avg;
                        currentValue = co2Avg != null ? Math.round(co2Avg).toString() : '--';
                    }

                    // Get contributing sensors data from stored aggregates
                    let sensorData = [];
                    let avgValue = null;
                    let sourceType = null;

                    // Air quality metrics: get sensors from outdoorSensors/rooms based on which card was clicked
                    const aqMetrics = ['pm2_5', 'pm10', 'pm1', 'co2', 'voc_index', 'nox_index'];
                    const aqMetricToKey = { 'pm2_5': 'pm25', 'pm10': 'pm10', 'pm1': 'pm1', 'co2': 'co2', 'voc_index': 'voc', 'nox_index': 'nox' };

                    if (aqMetrics.includes(metric)) {
                        const metricKey = aqMetricToKey[metric] || metric;
                        // Determine source type by which card was clicked, not by metric type
                        const isOutdoorCard = card.id === 'outdoorAirCard' || card.closest('#outdoorSection') !== null;
                        sourceType = isOutdoorCard ? 'outdoor' : 'indoor';

                        if (isOutdoorCard) {
                            // Get outdoor sensors that have this metric
                            const outdoorSensors = this.currentAggregates?.outdoorSensors || {};
                            sensorData = Object.values(outdoorSensors)
                                .filter(s => s[metricKey] != null)
                                .map(s => ({
                                    id: s.deviceId,
                                    name: s.name,
                                    value: s[metricKey],
                                    isIndoor: false,
                                    boardModel: s.boardModel,
                                    sensorModel: s.sensorModels?.[metricKey]
                                }));
                            avgValue = this.currentAggregates?.outdoor?.[metricKey]?.avg;
                        } else {
                            // Get indoor sensors (from rooms) that have this metric
                            const rooms = this.currentAggregates?.rooms || {};
                            sensorData = [];
                            Object.values(rooms).forEach(room => {
                                const sensors = Object.values(room.sensors || {});
                                sensors.forEach(s => {
                                    if (s[metricKey] != null) {
                                        sensorData.push({
                                            id: s.deviceId,
                                            name: s.name,
                                            value: s[metricKey],
                                            isIndoor: true,
                                            boardModel: s.boardModel,
                                            sensorModel: s.sensorModels?.[metricKey]
                                        });
                                    }
                                });
                            });
                            avgValue = this.currentAggregates?.indoor?.[metricKey]?.avg;
                        }
                    } else {
                        // Non-AQ metrics
                        sensorData = this.currentAggregates?.[aggregateKey]?.sensors || [];
                        avgValue = this.currentAggregates?.[aggregateKey]?.avg;
                    }

                    detailsSidebar.open(metric, currentValue, {
                        deviceIds: deviceIdsToUse,
                        sensors: sensorData,
                        avg: avgValue,
                        sourceType: sourceType
                    });
                }
            });
        });

        // Time navigation buttons
        document.querySelectorAll('.time-nav-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const range = e.target.dataset.range;
                if (range === 'more') {
                    // TODO: Show more options dropdown
                    return;
                }
                document.querySelectorAll('.time-nav-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeRange = range;
                this.updateSparklines();
            });
        });

        // Swarm toggle buttons on metric cards - now sync with global toggle
        ['temperature', 'humidity', 'pressure'].forEach(metric => {
            const toggleId = metric === 'temperature' ? 'tempSwarmToggle' :
                            metric === 'humidity' ? 'humiditySwarmToggle' : 'pressureSwarmToggle';
            const toggleBtn = document.getElementById(toggleId);
            if (toggleBtn) {
                toggleBtn.addEventListener('click', (e) => {
                    e.stopPropagation(); // Prevent card click
                    // Clicking individual toggle now toggles global state
                    this.toggleGlobalSwarmView();
                });
            }
        });

        // Hero swarm toggle button - master toggle for all swarm views
        const heroToggle = document.getElementById('heroSwarmToggle');
        if (heroToggle) {
            heroToggle.addEventListener('click', () => {
                this.toggleGlobalSwarmView();
            });
        }
    }

    // Toggle global swarm view (affects all metrics and hero bar)
    toggleGlobalSwarmView() {
        this.globalSwarmView = !this.globalSwarmView;

        // Persist to localStorage
        localStorage.setItem('globalSwarmView', this.globalSwarmView.toString());

        // Update all metric toggle states
        ['temperature', 'humidity', 'pressure'].forEach(metric => {
            this.swarmToggleState[metric] = this.globalSwarmView;
        });

        // Update hero toggle button
        const heroToggle = document.getElementById('heroSwarmToggle');
        if (heroToggle) {
            heroToggle.classList.toggle('active', this.globalSwarmView);
        }

        // Update all displays
        ['temperature', 'humidity', 'pressure'].forEach(metric => {
            this.updateSwarmDisplay(metric);
        });

        // Update hero bar values
        this.updateHeroForSwarmView();
    }

    // Update hero bar based on swarm view state
    updateHeroForSwarmView() {
        // Temperature affects high/low display
        if (this.swarmData.temperature?.available && this.currentAggregates) {
            const tempData = this.swarmData.temperature;

            if (this.globalSwarmView && tempData.median != null) {
                // Show swarm median in hero (we don't have swarm min/max, so just update condition)
                document.getElementById('weatherCondition').textContent =
                    this.determineWeatherCondition(tempData.median, this.swarmData.humidity?.median, null).title + ' (Swarm)';
            } else {
                // Show original condition
                const temp = this.currentAggregates.temperature?.avg;
                const humidity = this.currentAggregates.humidity?.avg;
                const condition = this.determineWeatherCondition(temp, humidity, null);
                document.getElementById('weatherCondition').textContent = condition.title;
            }
        }

        // Update humidity quick stat
        const humidityEl = document.getElementById('weatherHumidity');
        if (humidityEl && this.swarmData.humidity?.available) {
            if (this.globalSwarmView && this.swarmData.humidity.median != null) {
                humidityEl.textContent = `${this.swarmData.humidity.median.toFixed(1)}%`;
            } else if (this.swarmData.humidity.myValue != null) {
                humidityEl.textContent = `${this.swarmData.humidity.myValue.toFixed(1)}%`;
            }
        }
    }

    // Legacy method - now triggers global toggle
    toggleSwarmView(metric) {
        this.toggleGlobalSwarmView();
    }

    // Update the display for a metric based on toggle state
    updateSwarmDisplay(metric) {
        const isSwarmView = this.swarmToggleState[metric];
        const data = this.swarmData[metric];

        // Get element IDs based on metric
        const prefix = metric === 'temperature' ? 'temp' :
                      metric === 'humidity' ? 'humidity' : 'pressure';
        const unit = metric === 'temperature' ? '°C' :
                    metric === 'humidity' ? '%' : ' hPa';
        const decimals = metric === 'pressure' ? 2 : 1;

        const valueEl = document.getElementById(`${prefix}CardValue`);
        const toggleBtn = document.getElementById(`${prefix}SwarmToggle`);
        const swarmValueEl = document.getElementById(`${prefix}SwarmValue`);
        const myValueEl = document.getElementById(`${prefix}MyValue`);

        if (!valueEl || !data.available) return;

        // Update toggle button state
        if (toggleBtn) {
            toggleBtn.classList.toggle('active', isSwarmView);
        }

        if (isSwarmView && data.median != null) {
            // Show swarm as primary, my value as secondary
            this.animateValueChange(valueEl, data.median.toFixed(decimals));

            // Hide swarm secondary, show my value secondary
            if (swarmValueEl) swarmValueEl.classList.add('hidden');
            if (myValueEl && data.myValue != null) {
                myValueEl.classList.remove('hidden');
                myValueEl.querySelector('.my-reading').textContent = `${data.myValue.toFixed(decimals)}${unit}`;
            }
        } else {
            // Show my value as primary, swarm as secondary
            if (data.myValue != null) {
                this.animateValueChange(valueEl, data.myValue.toFixed(decimals));
            }

            // Hide my value secondary, show swarm secondary
            if (myValueEl) myValueEl.classList.add('hidden');
            if (swarmValueEl && data.median != null) {
                swarmValueEl.classList.remove('hidden');
                swarmValueEl.querySelector('.swarm-reading').textContent = `${data.median.toFixed(decimals)}${unit}`;
            }
        }
    }

    startClockUpdate() {
        this.updateDateTime();
        setInterval(() => this.updateDateTime(), 60000); // Update every minute
    }

    updateDateTime() {
        const now = new Date();
        const timeEl = document.getElementById('dashboardTime');
        const dateEl = document.getElementById('dashboardDate');

        if (timeEl) {
            const hours = now.getHours().toString().padStart(2, '0');
            const mins = now.getMinutes().toString().padStart(2, '0');
            timeEl.textContent = `${hours}:${mins}`;
        }
        if (dateEl) {
            dateEl.textContent = now.toLocaleDateString([], { weekday: 'short', day: 'numeric', month: 'short' });
        }
    }

    show(deviceIds, aggregates, outdoorDeviceIds = null) {
        if (!this.isActive) return;

        this.deviceIds = deviceIds;
        this.outdoorDeviceIds = outdoorDeviceIds || deviceIds; // Fallback to all if not provided
        this.currentAggregates = aggregates; // Store for sidebar access
        this.container.style.display = 'block';

        this.updateLocation();
        this.updateAstronomy();
        this.updateWeatherHero(aggregates);
        this.updateMetricCards(aggregates);
        this.updateAirQualityCards(aggregates);
        this.updateSparklines();
        this.fetchComparisonData();

        // Update insights based on current conditions
        this.updateInsights(aggregates);
    }

    updateInsights(aggregates) {
        if (!this.insightEngine) return;

        // Collect all conditions for insight generation
        const conditions = {
            temperature: aggregates.temperature?.avg ?? null,
            humidity: aggregates.humidity?.avg ?? null,
            pressure: aggregates.pressure?.avg ?? null,
            pressureTrend: aggregates.pressure?.trend ?? null,
            co2: aggregates.co2?.avg ?? null,
            pm25: aggregates.pm2_5?.avg ?? null
        };

        // Calculate AQI from PM2.5 if available
        if (conditions.pm25 !== null) {
            conditions.aqi = this.calculateAQI(conditions.pm25);
        }

        // Calculate IAQI from CO2 if available
        if (conditions.co2 !== null) {
            conditions.iaqi = this.calculateIAQI(conditions.co2);
        }

        // Add sunrise/sunset times for photography tips
        conditions.sunrise = document.getElementById('sunriseTime')?.textContent || null;
        conditions.sunset = document.getElementById('sunsetTime')?.textContent || null;

        // Add swarm data for swarm-based insights
        conditions.swarm = {
            temperature: this.swarmData?.temperature || {},
            humidity: this.swarmData?.humidity || {},
            pressure: this.swarmData?.pressure || {}
        };

        // Calculate overall swarm status from all metrics
        const swarmMetrics = ['temperature', 'humidity', 'pressure'];
        let totalSwarmSize = 0;
        let hasAnySwarm = false;
        let allVerified = true;

        swarmMetrics.forEach(metric => {
            const data = this.swarmData?.[metric];
            if (data?.available) {
                hasAnySwarm = true;
                totalSwarmSize = Math.max(totalSwarmSize, (data.mySensorCount || 0) + (data.swarmPeerCount || 0));
            }
            if (!data?.available || (data.mySensorCount + data.swarmPeerCount) < 5) {
                allVerified = false;
            }
        });

        conditions.swarmStatus = {
            hasSwarm: hasAnySwarm,
            totalSize: totalSwarmSize,
            allVerified: allVerified,
            mySensorCount: this.swarmData?.temperature?.mySensorCount || 0,
            swarmPeerCount: this.swarmData?.temperature?.swarmPeerCount || 0
        };

        // Check for any outliers in user's sensors
        const appSensors = window.app?.sensors || [];
        const outlierSensors = appSensors.filter(s => s.is_outlier);
        conditions.outlierCount = outlierSensors.length;

        this.insightEngine.updateConditions(conditions);
    }

    calculateAQI(pm25) {
        // Simplified AQI calculation for PM2.5
        if (pm25 <= 12) return Math.round((50 / 12) * pm25);
        if (pm25 <= 35.4) return Math.round(50 + (50 / 23.4) * (pm25 - 12));
        if (pm25 <= 55.4) return Math.round(100 + (50 / 20) * (pm25 - 35.4));
        if (pm25 <= 150.4) return Math.round(150 + (50 / 95) * (pm25 - 55.4));
        if (pm25 <= 250.4) return Math.round(200 + (100 / 100) * (pm25 - 150.4));
        return Math.round(300 + (100 / 150) * (pm25 - 250.4));
    }

    calculateIAQI(co2) {
        // Indoor Air Quality Index based on CO2
        if (co2 <= 400) return 0;
        if (co2 <= 600) return Math.round((50 / 200) * (co2 - 400));
        if (co2 <= 1000) return Math.round(50 + (50 / 400) * (co2 - 600));
        if (co2 <= 1500) return Math.round(100 + (50 / 500) * (co2 - 1000));
        if (co2 <= 2000) return Math.round(150 + (50 / 500) * (co2 - 1500));
        return Math.round(200 + (100 / 1000) * (co2 - 2000));
    }

    hide() {
        this.container.style.display = 'none';
    }

    updateLocation() {
        // Location name is now managed by LocationManager
        // Only update subtitle if location manager hasn't set it
        if (!locationManager) {
            const subtitleEl = document.getElementById('dashboardLocationSubtitle');
            if (subtitleEl) {
                const cachedLocation = localStorage.getItem('dashboardLocation');
                if (cachedLocation) {
                    subtitleEl.textContent = cachedLocation;
                } else {
                    subtitleEl.textContent = 'Local sensors';
                }
            }
        }
    }

    updateAstronomy() {
        // Calculate sunrise/sunset using a simplified algorithm
        // In production, use a proper astronomy library
        const now = new Date();
        const lat = -36.85; // Default Auckland latitude
        const lon = 174.76; // Default Auckland longitude

        // Get sunrise/sunset times (simplified calculation)
        const { sunrise, sunset } = this.calculateSunTimes(lat, lon, now);

        document.getElementById('sunriseTime').textContent = this.formatTime(sunrise);
        document.getElementById('sunsetTime').textContent = this.formatTime(sunset);

        // Calculate daylight duration
        const daylightMs = sunset - sunrise;
        const hours = Math.floor(daylightMs / (1000 * 60 * 60));
        const minutes = Math.floor((daylightMs % (1000 * 60 * 60)) / (1000 * 60));
        document.getElementById('daylightDuration').textContent = `${hours}h ${minutes}m`;

        // Moon phase
        const moonPhase = this.getMoonPhase(now);
        document.getElementById('moonPhase').textContent = moonPhase;

        // Update photography times
        this.updatePhotoTimes(now, sunrise, sunset, lat, lon);
    }

    updatePhotoTimes(now, todaySunrise, todaySunset, lat, lon) {
        // Calculate tomorrow's sun times
        const tomorrow = new Date(now);
        tomorrow.setDate(tomorrow.getDate() + 1);
        const { sunrise: tomorrowSunrise, sunset: tomorrowSunset } = this.calculateSunTimes(lat, lon, tomorrow);

        // Build list of all photo windows for today and tomorrow
        // Each entry: { type, label, start, end }
        const photoWindows = [
            // Today
            { type: 'blue', label: 'Blue', start: new Date(todaySunrise.getTime() - 30 * 60 * 1000), end: new Date(todaySunrise) },
            { type: 'golden', label: 'Golden', start: new Date(todaySunrise), end: new Date(todaySunrise.getTime() + 60 * 60 * 1000) },
            { type: 'golden', label: 'Golden', start: new Date(todaySunset.getTime() - 60 * 60 * 1000), end: new Date(todaySunset) },
            { type: 'blue', label: 'Blue', start: new Date(todaySunset), end: new Date(todaySunset.getTime() + 30 * 60 * 1000) },
            // Tomorrow
            { type: 'blue', label: 'Blue', start: new Date(tomorrowSunrise.getTime() - 30 * 60 * 1000), end: new Date(tomorrowSunrise), tomorrow: true },
            { type: 'golden', label: 'Golden', start: new Date(tomorrowSunrise), end: new Date(tomorrowSunrise.getTime() + 60 * 60 * 1000), tomorrow: true },
            { type: 'golden', label: 'Golden', start: new Date(tomorrowSunset.getTime() - 60 * 60 * 1000), end: new Date(tomorrowSunset), tomorrow: true },
            { type: 'blue', label: 'Blue', start: new Date(tomorrowSunset), end: new Date(tomorrowSunset.getTime() + 30 * 60 * 1000), tomorrow: true },
        ];

        // Find windows that are current or upcoming (not fully passed)
        const upcomingWindows = photoWindows.filter(w => now <= w.end);

        // Take the next 4
        const next4 = upcomingWindows.slice(0, 4);

        // Get elements (now in chronological display order)
        const elements = [
            document.getElementById('blueHourAM'),
            document.getElementById('goldenHourAM'),
            document.getElementById('goldenHourPM'),
            document.getElementById('blueHourPM')
        ];

        // Update each element
        next4.forEach((window, i) => {
            const el = elements[i];
            if (!el) return;

            const isNow = now >= window.start && now <= window.end;
            const timeStr = this.formatTime(window.start);
            const suffix = window.tomorrow ? ' tmrw' : '';

            el.textContent = isNow ? 'Now' : timeStr + suffix;
            el.className = `photo-time ${window.type}${isNow ? ' now' : ''}`;
            el.title = isNow
                ? `${window.label} hour now!`
                : `${window.label} hour: ${timeStr}${window.tomorrow ? ' tomorrow' : ''}`;
        });

        // If we have fewer than 4 upcoming, hide the rest
        for (let i = next4.length; i < 4; i++) {
            if (elements[i]) {
                elements[i].style.display = 'none';
            }
        }
        // Show the ones we're using
        for (let i = 0; i < next4.length; i++) {
            if (elements[i]) {
                elements[i].style.display = '';
            }
        }
    }

    calculateSunTimes(lat, lon, date) {
        // Simplified sunrise/sunset calculation with timezone correction
        const dayOfYear = Math.floor((date - new Date(date.getFullYear(), 0, 0)) / (1000 * 60 * 60 * 24));
        const declination = -23.45 * Math.cos((360/365) * (dayOfYear + 10) * Math.PI / 180);

        const hourAngle = Math.acos(-Math.tan(lat * Math.PI / 180) * Math.tan(declination * Math.PI / 180)) * 180 / Math.PI;

        // Calculate solar noon in UTC, then convert to local time
        const solarNoonUTC = 12 - (lon / 15);
        // getTimezoneOffset returns minutes, negative for east of UTC
        const tzOffsetHours = -date.getTimezoneOffset() / 60;
        const solarNoonLocal = solarNoonUTC + tzOffsetHours;

        // Normalize to 0-24 range
        const solarNoon = ((solarNoonLocal % 24) + 24) % 24;

        const sunriseHours = solarNoon - hourAngle / 15;
        const sunsetHours = solarNoon + hourAngle / 15;

        const sunrise = new Date(date);
        sunrise.setHours(Math.floor(sunriseHours), Math.round((sunriseHours % 1) * 60), 0, 0);

        const sunset = new Date(date);
        sunset.setHours(Math.floor(sunsetHours), Math.round((sunsetHours % 1) * 60), 0, 0);

        return { sunrise, sunset };
    }

    getMoonPhase(date) {
        // Calculate moon phase (simplified)
        const lp = 2551443; // Lunar period in seconds
        const new_moon = new Date(1970, 0, 7, 20, 35, 0).getTime() / 1000;
        const phase = ((date.getTime() / 1000 - new_moon) % lp) / lp;

        if (phase < 0.0625) return '🌑 New Moon';
        if (phase < 0.1875) return '🌒 Waxing Crescent';
        if (phase < 0.3125) return '🌓 First Quarter';
        if (phase < 0.4375) return '🌔 Waxing Gibbous';
        if (phase < 0.5625) return '🌕 Full Moon';
        if (phase < 0.6875) return '🌖 Waning Gibbous';
        if (phase < 0.8125) return '🌗 Last Quarter';
        if (phase < 0.9375) return '🌘 Waning Crescent';
        return '🌑 New Moon';
    }

    // ============= WEATHER HERO SECTION =============

    updateWeatherHero(aggregates) {
        const temp = aggregates.temperature?.avg;
        const humidity = aggregates.humidity?.avg;
        const pressure = aggregates.pressure?.avg;
        const pm25 = aggregates.pm25?.avg;
        const tempMin = aggregates.temperature?.min;
        const tempMax = aggregates.temperature?.max;

        // Determine weather condition based on sensor data
        const condition = this.determineWeatherCondition(temp, humidity, pressure);

        // Update main weather icon (now in center)
        const iconContainer = document.getElementById('weatherIconMain');
        if (iconContainer) {
            iconContainer.innerHTML = this.getWeatherSVG(condition.icon, 100);
        }

        // Update condition text
        document.getElementById('weatherCondition').textContent = condition.title;

        // Update quick stats
        if (tempMin != null && tempMax != null) {
            document.getElementById('weatherHighLow').textContent = `${tempMax.toFixed(0)}° / ${tempMin.toFixed(0)}°`;
        } else {
            document.getElementById('weatherHighLow').textContent = '--° / --°';
        }
        if (humidity != null) {
            document.getElementById('weatherHumidity').textContent = `${humidity.toFixed(1)}%`;
        } else {
            document.getElementById('weatherHumidity').textContent = '--%';
        }
        if (pm25 != null) {
            const aqi = this.pm25ToAQI(pm25);
            document.getElementById('weatherAqi').textContent = aqi;
        } else {
            document.getElementById('weatherAqi').textContent = '--';
        }

        // Update historical comparison panel (fetched asynchronously)
        this.updateHistoricalComparison(temp);

        // Update predictions panel (fetched asynchronously)
        this.updatePredictions(temp);

        // Update pressure trend indicator
        this.updatePressureTrend(aggregates);
    }

    determineWeatherCondition(temp, humidity, pressure) {
        // Default condition
        let condition = {
            icon: 'clear-day',
            title: 'Clear',
            description: 'Current conditions from sensor data',
            forecast: 'Conditions stable',
            forecastIcon: 'clear-day'
        };

        // Determine time of day
        const hour = new Date().getHours();
        const isNight = hour < 6 || hour > 20;

        if (temp == null) {
            return { ...condition, title: 'Loading...', description: 'Waiting for sensor data' };
        }

        // Temperature-based descriptions
        if (temp >= 30) {
            condition.title = isNight ? 'Warm Night' : 'Hot';
            condition.icon = isNight ? 'clear-night' : 'hot';
            condition.description = humidity > 70 ? 'Hot and humid conditions' : 'Hot and dry conditions';
        } else if (temp >= 24) {
            condition.title = isNight ? 'Mild Night' : 'Warm';
            condition.icon = isNight ? 'clear-night' : 'clear-day';
            condition.description = humidity > 70 ? 'Warm with high humidity' : 'Pleasant warm conditions';
        } else if (temp >= 18) {
            condition.title = isNight ? 'Cool Night' : 'Mild';
            condition.icon = isNight ? 'clear-night' : 'partly-cloudy';
            condition.description = 'Comfortable temperatures';
        } else if (temp >= 10) {
            condition.title = isNight ? 'Cold Night' : 'Cool';
            condition.icon = isNight ? 'clear-night' : 'cloudy';
            condition.description = 'Cool conditions, consider a jacket';
        } else {
            condition.title = isNight ? 'Cold Night' : 'Cold';
            condition.icon = 'cold';
            condition.description = 'Cold conditions';
        }

        // Humidity modifiers
        if (humidity != null) {
            if (humidity > 85) {
                condition.description = condition.description.replace('conditions', 'with high humidity');
                if (!isNight && temp > 15) {
                    condition.forecastIcon = 'rain-chance';
                    condition.forecast = 'Possible precipitation';
                }
            } else if (humidity < 30) {
                condition.description = condition.description.replace('conditions', 'and very dry');
            }
        }

        // Pressure-based modifications
        if (pressure != null) {
            if (pressure < 1000) {
                condition.icon = 'storm';
                condition.forecastIcon = 'storm';
                condition.forecast = 'Low pressure - unsettled weather';
                condition.description = 'Low pressure system present';
            } else if (pressure > 1020) {
                condition.forecastIcon = isNight ? 'clear-night' : 'clear-day';
                condition.forecast = 'High pressure - stable conditions';
            }
        }

        return condition;
    }

    updatePressureTrend(aggregates) {
        const indicator = document.getElementById('pressureTrendIndicator');
        const iconEl = document.getElementById('pressureTrendIcon');
        const textEl = document.getElementById('pressureTrendText');

        if (!indicator || !aggregates.pressure) return;

        // Calculate pressure trend from data (would need historical data)
        // For now, use mock trend or derive from min/max
        const currentPressure = aggregates.pressure.avg;
        const trend = aggregates.pressure.trend;

        let trendClass = 'steady';
        let trendIcon = '→';
        let trendText = 'Pressure steady';

        if (trend) {
            // Use trend if provided by backend
            if (trend.includes('Rising') || trend.includes('↑')) {
                trendClass = 'rising';
                trendIcon = '↑';
                trendText = 'Pressure rising - clearing weather';
            } else if (trend.includes('Falling') || trend.includes('↓')) {
                trendClass = 'falling';
                trendIcon = '↓';
                trendText = 'Pressure falling - change incoming';

                // Check for rapid fall (storm warning)
                if (currentPressure && currentPressure < 1005) {
                    trendClass = 'storm-warning';
                    trendIcon = '!';
                    trendText = 'Rapid drop - possible storm';
                }
            }
        } else if (currentPressure) {
            // Infer from current value
            if (currentPressure < 1000) {
                trendClass = 'storm-warning';
                trendIcon = '!';
                trendText = 'Low pressure - unsettled';
            } else if (currentPressure > 1025) {
                trendClass = 'rising';
                trendIcon = '↑';
                trendText = 'High pressure - stable';
            }
        }

        indicator.className = `pressure-trend-indicator ${trendClass}`;
        iconEl.textContent = trendIcon;
        textEl.textContent = trendText;
    }

    async updateHistoricalComparison(currentTemp) {
        // Get DOM elements
        const histYesterday = document.getElementById('histYesterday');
        const histYesterdayDiff = document.getElementById('histYesterdayDiff');
        const histYesterdayTime = document.getElementById('histYesterdayTime');
        const iconYesterday = document.getElementById('iconYesterday');

        const hist24hChange = document.getElementById('hist24hChange');
        const hist24hTrend = document.getElementById('hist24hTrend');
        const icon24h = document.getElementById('icon24h');

        const histLastWeek = document.getElementById('histLastWeek');
        const histLastWeekDiff = document.getElementById('histLastWeekDiff');
        const histLastWeekDay = document.getElementById('histLastWeekDay');
        const iconLastWeek = document.getElementById('iconLastWeek');

        if (!histYesterday) return;

        // Need outdoor device IDs for the query
        if (!this.outdoorDeviceIds || this.outdoorDeviceIds.length === 0) {
            return;
        }

        // Calculate time strings
        const now = new Date();
        const yesterdayTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const lastWeekDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const hours = now.getHours().toString().padStart(2, '0');
        const mins = now.getMinutes().toString().padStart(2, '0');
        const timeStr = `${hours}:${mins}`;
        const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        const lastWeekDayName = dayNames[lastWeekDate.getDay()];

        try {
            const response = await fetch(`/api/history/comparison?devices=${this.outdoorDeviceIds.join(',')}&type=temperature`);
            if (!response.ok) return;

            const result = await response.json();
            const comparison = result.comparison;

            if (!comparison) {
                histYesterday.textContent = '--°';
                histYesterdayDiff.textContent = '';
                hist24hChange.textContent = '--';
                histLastWeek.textContent = '--°';
                histLastWeekDiff.textContent = '';
                return;
            }

            // Yesterday at same time
            if (comparison.yesterday != null) {
                histYesterday.textContent = `${comparison.yesterday.toFixed(0)}°`;
                if (histYesterdayTime) histYesterdayTime.textContent = `at ${timeStr}`;
                if (iconYesterday) iconYesterday.innerHTML = this.getMiniWeatherIcon(comparison.yesterday, yesterdayTime.getHours());

                // Show difference from current
                if (currentTemp != null) {
                    const diff = currentTemp - comparison.yesterday;
                    const diffText = diff >= 0 ? `+${diff.toFixed(0)}°` : `${diff.toFixed(0)}°`;
                    histYesterdayDiff.textContent = diffText;

                    if (Math.abs(diff) < 1) {
                        histYesterdayDiff.className = 'mini-card-diff same';
                    } else if (diff > 0) {
                        histYesterdayDiff.className = 'mini-card-diff warmer';
                    } else {
                        histYesterdayDiff.className = 'mini-card-diff cooler';
                    }
                } else {
                    histYesterdayDiff.textContent = '';
                }
            } else {
                histYesterday.textContent = '--°';
                histYesterdayDiff.textContent = '';
                if (iconYesterday) iconYesterday.innerHTML = this.getMiniWeatherIcon(null, now.getHours());
            }

            // 24h change
            if (comparison.ago24h != null && currentTemp != null) {
                const change = currentTemp - comparison.ago24h;
                const sign = change >= 0 ? '+' : '';
                hist24hChange.textContent = `${sign}${change.toFixed(1)}°`;

                // Set trend text and icon
                let trendText = 'Steady';
                let trendIcon = 'steady';
                if (change > 2) {
                    trendText = 'Much warmer';
                    trendIcon = 'up';
                } else if (change > 0.5) {
                    trendText = 'Warmer';
                    trendIcon = 'up';
                } else if (change < -2) {
                    trendText = 'Much cooler';
                    trendIcon = 'down';
                } else if (change < -0.5) {
                    trendText = 'Cooler';
                    trendIcon = 'down';
                }

                if (hist24hTrend) hist24hTrend.textContent = trendText;
                if (icon24h) icon24h.innerHTML = this.getTrendIcon(trendIcon, change);

                if (Math.abs(change) < 0.5) {
                    hist24hChange.style.color = '#94a3b8';
                } else if (change > 0) {
                    hist24hChange.style.color = '#f97316';
                } else {
                    hist24hChange.style.color = '#60a5fa';
                }
            } else {
                hist24hChange.textContent = '--';
                hist24hChange.style.color = '#94a3b8';
                if (icon24h) icon24h.innerHTML = this.getTrendIcon('steady', 0);
            }

            // Last week same time
            if (comparison.lastWeek != null) {
                histLastWeek.textContent = `${comparison.lastWeek.toFixed(0)}°`;
                if (histLastWeekDay) histLastWeekDay.textContent = `${lastWeekDayName} at ${timeStr}`;
                if (iconLastWeek) iconLastWeek.innerHTML = this.getMiniWeatherIcon(comparison.lastWeek, now.getHours());

                // Show difference from current
                if (currentTemp != null) {
                    const diff = currentTemp - comparison.lastWeek;
                    const diffText = diff >= 0 ? `+${diff.toFixed(0)}°` : `${diff.toFixed(0)}°`;
                    histLastWeekDiff.textContent = diffText;

                    if (Math.abs(diff) < 1) {
                        histLastWeekDiff.className = 'mini-card-diff same';
                    } else if (diff > 0) {
                        histLastWeekDiff.className = 'mini-card-diff warmer';
                    } else {
                        histLastWeekDiff.className = 'mini-card-diff cooler';
                    }
                } else {
                    histLastWeekDiff.textContent = '';
                }
            } else {
                histLastWeek.textContent = '--°';
                histLastWeekDiff.textContent = '';
                if (iconLastWeek) iconLastWeek.innerHTML = this.getMiniWeatherIcon(null, now.getHours());
            }

        } catch (error) {
            console.error('Failed to fetch historical comparison:', error);
        }
    }

    getMiniWeatherIcon(temp, hour) {
        const isNight = hour < 6 || hour >= 20;

        // Determine icon type based on temperature, matching the main hero icon logic
        let iconType;
        if (temp == null) {
            iconType = isNight ? 'clear-night' : 'clear-day';
        } else if (temp >= 30) {
            // Hot - always show sunny/clear
            iconType = isNight ? 'clear-night' : 'hot';
        } else if (temp >= 20) {
            // Warm - clear day/night
            iconType = isNight ? 'clear-night' : 'clear-day';
        } else if (temp >= 10) {
            // Mild - partly cloudy
            iconType = isNight ? 'clear-night' : 'partly-cloudy';
        } else if (temp >= 0) {
            // Cool - cloudy
            iconType = 'cloudy';
        } else {
            // Cold/freezing
            iconType = 'cold';
        }

        return this.getWeatherSVG(iconType, 48);
    }

    getTrendIcon(trend, change) {
        const color = change > 0 ? '#f97316' : (change < 0 ? '#60a5fa' : '#94a3b8');

        if (trend === 'up') {
            return `<svg viewBox="0 0 36 36" fill="none">
                <path d="M18 8 L26 20 L22 20 L22 28 L14 28 L14 20 L10 20 Z" fill="${color}"/>
            </svg>`;
        } else if (trend === 'down') {
            return `<svg viewBox="0 0 36 36" fill="none">
                <path d="M18 28 L26 16 L22 16 L22 8 L14 8 L14 16 L10 16 Z" fill="${color}"/>
            </svg>`;
        } else {
            return `<svg viewBox="0 0 36 36" fill="none">
                <path d="M8 18 L16 12 L16 16 L28 16 L28 20 L16 20 L16 24 Z" fill="${color}" opacity="0.5"/>
            </svg>`;
        }
    }

    async updatePredictions(currentTemp) {
        // Get DOM elements
        const pred3h = document.getElementById('pred3h');
        const pred3hDiff = document.getElementById('pred3hDiff');
        const pred3hTime = document.getElementById('pred3hTime');
        const icon3h = document.getElementById('icon3h');

        const predTonight = document.getElementById('predTonight');
        const predTonightRange = document.getElementById('predTonightRange');
        const iconTonight = document.getElementById('iconTonight');

        const predTomorrow = document.getElementById('predTomorrow');
        const predTomorrowRange = document.getElementById('predTomorrowRange');
        const iconTomorrow = document.getElementById('iconTomorrow');

        if (!pred3h) return;

        // Need outdoor device IDs for the query
        if (!this.outdoorDeviceIds || this.outdoorDeviceIds.length === 0) {
            return;
        }

        // Calculate future time for 3h
        const now = new Date();
        const in3h = new Date(now.getTime() + 3 * 60 * 60 * 1000);
        const hours3h = in3h.getHours().toString().padStart(2, '0');
        const mins3h = in3h.getMinutes().toString().padStart(2, '0');
        const time3hStr = `${hours3h}:${mins3h}`;

        try {
            const response = await fetch(`/api/prediction?devices=${this.outdoorDeviceIds.join(',')}&hours=24`);
            if (!response.ok) return;

            const result = await response.json();
            const prediction = result.prediction;

            if (!prediction || !prediction.predictions || prediction.predictions.length < 4) {
                pred3h.textContent = '--°';
                pred3hDiff.textContent = '';
                predTonight.textContent = '--°';
                predTomorrow.textContent = '--°';
                if (icon3h) icon3h.innerHTML = this.getMiniWeatherIcon(null, in3h.getHours());
                if (iconTonight) iconTonight.innerHTML = this.getMiniWeatherIcon(null, 22);
                if (iconTomorrow) iconTomorrow.innerHTML = this.getMiniWeatherIcon(null, 14);
                return;
            }

            // 3 hours from now
            const pred3hValue = prediction.predictions[3]?.temp;
            if (pred3hValue != null) {
                pred3h.textContent = `${pred3hValue.toFixed(0)}°`;
                if (pred3hTime) pred3hTime.textContent = `at ${time3hStr}`;
                if (icon3h) icon3h.innerHTML = this.getMiniWeatherIcon(pred3hValue, in3h.getHours());

                // Show difference from current
                if (currentTemp != null) {
                    const diff = pred3hValue - currentTemp;
                    const diffText = diff >= 0 ? `+${diff.toFixed(0)}°` : `${diff.toFixed(0)}°`;
                    pred3hDiff.textContent = diffText;

                    if (Math.abs(diff) < 1) {
                        pred3hDiff.className = 'mini-card-diff same';
                    } else if (diff > 0) {
                        pred3hDiff.className = 'mini-card-diff warmer';
                    } else {
                        pred3hDiff.className = 'mini-card-diff cooler';
                    }
                } else {
                    pred3hDiff.textContent = '';
                }
            } else {
                pred3h.textContent = '--°';
                pred3hDiff.textContent = '';
                if (icon3h) icon3h.innerHTML = this.getMiniWeatherIcon(null, in3h.getHours());
            }

            // Tonight (show low with range)
            if (prediction.tonight) {
                predTonight.textContent = `${prediction.tonight.low}°`;
                if (predTonightRange) predTonightRange.textContent = `Low tonight`;
                if (iconTonight) iconTonight.innerHTML = this.getMiniWeatherIcon(prediction.tonight.low, 22); // Night icon
            } else {
                predTonight.textContent = '--°';
                if (iconTonight) iconTonight.innerHTML = this.getMiniWeatherIcon(null, 22);
            }

            // Tomorrow (show high with range)
            if (prediction.tomorrow) {
                predTomorrow.textContent = `${prediction.tomorrow.high}°`;
                if (predTomorrowRange) predTomorrowRange.textContent = `Low ${prediction.tomorrow.low}°`;
                if (iconTomorrow) iconTomorrow.innerHTML = this.getMiniWeatherIcon(prediction.tomorrow.high, 14); // Day icon
            } else {
                predTomorrow.textContent = '--°';
                if (iconTomorrow) iconTomorrow.innerHTML = this.getMiniWeatherIcon(null, 14);
            }

        } catch (error) {
            console.error('Failed to fetch predictions:', error);
        }
    }

    getWeatherSVG(type, size = 64) {
        const svgs = {
            'clear-day': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <circle cx="32" cy="32" r="12" fill="#fbbf24"/>
                <g stroke="#fbbf24" stroke-width="3" stroke-linecap="round">
                    <line x1="32" y1="6" x2="32" y2="14"/>
                    <line x1="32" y1="50" x2="32" y2="58"/>
                    <line x1="6" y1="32" x2="14" y2="32"/>
                    <line x1="50" y1="32" x2="58" y2="32"/>
                    <line x1="13.6" y1="13.6" x2="19.3" y2="19.3"/>
                    <line x1="44.7" y1="44.7" x2="50.4" y2="50.4"/>
                    <line x1="13.6" y1="50.4" x2="19.3" y2="44.7"/>
                    <line x1="44.7" y1="19.3" x2="50.4" y2="13.6"/>
                </g>
            </svg>`,
            'clear-night': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M28 8c-11 0-20 9-20 20s9 20 20 20c3 0 6-.5 8.5-1.5C31 43 27 37 27 30c0-10 7-18 16-20-4-1.5-9-2-15-2z" fill="#94a3b8"/>
                <circle cx="44" cy="14" r="1.5" fill="#fbbf24"/>
                <circle cx="52" cy="22" r="1" fill="#fbbf24"/>
                <circle cx="48" cy="10" r="1" fill="#fbbf24"/>
            </svg>`,
            'partly-cloudy': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <circle cx="24" cy="20" r="8" fill="#fbbf24"/>
                <g stroke="#fbbf24" stroke-width="2" stroke-linecap="round">
                    <line x1="24" y1="4" x2="24" y2="8"/>
                    <line x1="24" y1="32" x2="24" y2="36"/>
                    <line x1="8" y1="20" x2="12" y2="20"/>
                    <line x1="36" y1="20" x2="40" y2="20"/>
                </g>
                <path d="M48 44c4.4 0 8-3.6 8-8s-3.6-8-8-8c-.5 0-1 0-1.5.1C45 23.5 40.5 20 35 20c-7.2 0-13 5.8-13 13 0 .7 0 1.3.1 2H20c-4.4 0-8 3.6-8 8s3.6 8 8 8h28c0 0 0 0 0 0z" fill="#e2e8f0" stroke="#94a3b8" stroke-width="2"/>
            </svg>`,
            'cloudy': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M52 42c5.5 0 10-4.5 10-10s-4.5-10-10-10c-.6 0-1.2.1-1.8.1C48.5 15.5 42 10 34 10c-9.9 0-18 8.1-18 18 0 .9.1 1.8.2 2.6C10.5 31.5 6 36.5 6 42.5 6 49 11 54 17.5 54H52z" fill="#94a3b8" stroke="#64748b" stroke-width="2"/>
            </svg>`,
            'rain': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M48 32c4.4 0 8-3.6 8-8s-3.6-8-8-8c-.5 0-1 0-1.5.1C45 11.5 40.5 8 35 8c-7.2 0-13 5.8-13 13 0 .7 0 1.3.1 2H20c-4.4 0-8 3.6-8 8s3.6 8 8 8h28z" fill="#94a3b8"/>
                <g stroke="#3b82f6" stroke-width="2" stroke-linecap="round">
                    <line x1="22" y1="44" x2="18" y2="56"/>
                    <line x1="32" y1="44" x2="28" y2="56"/>
                    <line x1="42" y1="44" x2="38" y2="56"/>
                </g>
            </svg>`,
            'rain-chance': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M48 32c4.4 0 8-3.6 8-8s-3.6-8-8-8c-.5 0-1 0-1.5.1C45 11.5 40.5 8 35 8c-7.2 0-13 5.8-13 13 0 .7 0 1.3.1 2H20c-4.4 0-8 3.6-8 8s3.6 8 8 8h28z" fill="#94a3b8"/>
                <g stroke="#3b82f6" stroke-width="2" stroke-linecap="round" opacity="0.5">
                    <line x1="24" y1="44" x2="22" y2="52"/>
                    <line x1="40" y1="44" x2="38" y2="52"/>
                </g>
            </svg>`,
            'storm': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M48 28c4.4 0 8-3.6 8-8s-3.6-8-8-8c-.5 0-1 0-1.5.1C45 7.5 40.5 4 35 4c-7.2 0-13 5.8-13 13 0 .7 0 1.3.1 2H20c-4.4 0-8 3.6-8 8s3.6 8 8 8h28z" fill="#64748b"/>
                <path d="M36 32l-4 12h8l-6 16 2-12h-8l8-16z" fill="#fbbf24" stroke="#f59e0b" stroke-width="1"/>
                <g stroke="#3b82f6" stroke-width="2" stroke-linecap="round">
                    <line x1="18" y1="40" x2="14" y2="52"/>
                    <line x1="48" y1="40" x2="44" y2="52"/>
                </g>
            </svg>`,
            'hot': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <circle cx="32" cy="32" r="14" fill="#f97316"/>
                <g stroke="#f97316" stroke-width="3" stroke-linecap="round">
                    <line x1="32" y1="4" x2="32" y2="12"/>
                    <line x1="32" y1="52" x2="32" y2="60"/>
                    <line x1="4" y1="32" x2="12" y2="32"/>
                    <line x1="52" y1="32" x2="60" y2="32"/>
                    <line x1="12.2" y1="12.2" x2="18" y2="18"/>
                    <line x1="46" y1="46" x2="51.8" y2="51.8"/>
                    <line x1="12.2" y1="51.8" x2="18" y2="46"/>
                    <line x1="46" y1="18" x2="51.8" y2="12.2"/>
                </g>
                <g fill="#fbbf24" opacity="0.6">
                    <path d="M28 56 Q32 52 36 56 Q32 60 28 56"/>
                    <path d="M22 58 Q26 54 30 58 Q26 62 22 58"/>
                </g>
            </svg>`,
            'cold': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <g stroke="#60a5fa" stroke-width="2" stroke-linecap="round">
                    <line x1="32" y1="8" x2="32" y2="56"/>
                    <line x1="12" y1="32" x2="52" y2="32"/>
                    <line x1="18" y1="18" x2="46" y2="46"/>
                    <line x1="18" y1="46" x2="46" y2="18"/>
                </g>
                <g fill="#60a5fa">
                    <polygon points="32,8 28,16 36,16"/>
                    <polygon points="32,56 28,48 36,48"/>
                    <polygon points="12,32 20,28 20,36"/>
                    <polygon points="52,32 44,28 44,36"/>
                </g>
                <circle cx="32" cy="32" r="4" fill="#93c5fd"/>
            </svg>`,
            'wind': `<svg viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
                <g stroke="#94a3b8" stroke-width="3" stroke-linecap="round">
                    <path d="M8 24h32c4.4 0 8-3.6 8-8s-3.6-8-8-8"/>
                    <path d="M8 36h40c4.4 0 8 3.6 8 8s-3.6 8-8 8"/>
                    <path d="M8 48h24c2.2 0 4-1.8 4-4s-1.8-4-4-4"/>
                </g>
            </svg>`
        };

        return svgs[type] || svgs['clear-day'];
    }

    formatTime(date) {
        const hours = date.getHours().toString().padStart(2, '0');
        const mins = date.getMinutes().toString().padStart(2, '0');
        return `${hours}:${mins}`;
    }

    updateMetricCards(aggregates) {
        // Temperature
        const tempValueEl = document.getElementById('tempCardValue');
        if (aggregates.temperature?.avg != null) {
            const newValue = aggregates.temperature.avg.toFixed(1);
            this.animateValueChange(tempValueEl, newValue);

            // Calculate feels-like temperature (simplified)
            const feelsLike = this.calculateFeelsLike(aggregates.temperature.avg, aggregates.humidity?.avg || 50);
            document.getElementById('tempCardSecondary').textContent = `Feels like ${feelsLike.toFixed(0)}°C`;

            // Update ambient temperature zone
            this.updateTemperatureAmbient(aggregates.temperature.avg);
        } else {
            tempValueEl.textContent = '--';
            document.getElementById('tempCardSecondary').textContent = 'No recent data';
            this.updateTemperatureAmbient(null);
        }

        // Humidity
        const humidityValueEl = document.getElementById('humidityCardValue');
        if (aggregates.humidity?.avg != null) {
            const newValue = aggregates.humidity.avg.toFixed(1);
            this.animateValueChange(humidityValueEl, newValue);

            // Calculate dew point
            const dewPoint = this.calculateDewPoint(aggregates.temperature?.avg || 20, aggregates.humidity.avg);
            document.getElementById('humidityCardSecondary').textContent = `Dew point ${dewPoint.toFixed(0)}°C`;

            // Update fog effect based on humidity
            this.updateHumidityAmbient(aggregates.humidity.avg);
        } else {
            humidityValueEl.textContent = '--';
            document.getElementById('humidityCardSecondary').textContent = 'No recent data';
            this.updateHumidityAmbient(null);
        }

        // Pressure
        const pressureValueEl = document.getElementById('pressureCardValue');
        if (aggregates.pressure?.avg != null) {
            const newValue = aggregates.pressure.avg.toFixed(2);
            this.animateValueChange(pressureValueEl, newValue);

            // Pressure trend
            const trend = aggregates.pressure.trend || 'Steady';
            document.getElementById('pressureCardSecondary').textContent = trend;

            // Update pressure card ambient
            this.updatePressureAmbient(trend);
        } else {
            pressureValueEl.textContent = '--';
            document.getElementById('pressureCardSecondary').textContent = 'No recent data';
            this.updatePressureAmbient(null);
        }

        // Update sensor quality badges
        this.updateQualityBadges(aggregates);

        // Update swarm toggle buttons and data
        this.updateSwarmToggles(aggregates);
    }

    // Update swarm toggle buttons based on available swarm data
    updateSwarmToggles(aggregates) {
        const metrics = ['temperature', 'humidity', 'pressure'];

        metrics.forEach(metric => {
            const prefix = metric === 'temperature' ? 'temp' :
                          metric === 'humidity' ? 'humidity' : 'pressure';
            const toggleBtn = document.getElementById(`${prefix}SwarmToggle`);
            const swarmValueEl = document.getElementById(`${prefix}SwarmValue`);
            const myValueEl = document.getElementById(`${prefix}MyValue`);

            // Get swarm median and peer count from sensors that have peer verification
            // Use per-metric swarm_sizes:
            // - Temperature/Humidity: outdoor sensors only (swarm_sizes.temperature/humidity)
            // - Pressure: indoor + outdoor (swarm_sizes.pressure)
            const sensors = aggregates[metric]?.sensors || [];
            const mySensorCount = sensors.length;
            let swarmMedian = null;
            let totalSwarmSize = 0;
            let swarmPeerCount = 0;

            // Find a sensor with swarm_medians data for this specific metric
            for (const s of sensors) {
                const fullSensor = window.app?.sensors?.find(fs => fs.deviceId === (s.id || s.deviceId));
                // Check per-metric swarm size (5+ required for verification)
                const metricSwarmSize = fullSensor?.swarm_sizes?.[metric] || 0;
                if (fullSensor && metricSwarmSize >= 5 && fullSensor.swarm_medians) {
                    // Get the swarm_median for this specific metric
                    swarmMedian = fullSensor.swarm_medians[metric];
                    totalSwarmSize = metricSwarmSize;
                    // Swarm peers = total swarm size minus my sensors
                    swarmPeerCount = Math.max(0, totalSwarmSize - mySensorCount);
                    if (swarmMedian != null) break;
                }
            }

            // Store swarm data
            const myValue = aggregates[metric]?.avg;
            const hasSwarmData = swarmMedian != null && myValue != null;

            this.swarmData[metric] = {
                median: swarmMedian,
                myValue: myValue,
                available: hasSwarmData,
                mySensorCount: mySensorCount,
                swarmPeerCount: swarmPeerCount
            };

            // Show/hide toggle button and update its label
            if (toggleBtn) {
                toggleBtn.classList.toggle('hidden', !hasSwarmData);
                if (hasSwarmData) {
                    // Update button to show "mine + peers" count
                    const labelEl = toggleBtn.querySelector('.swarm-toggle-label');
                    if (labelEl) {
                        labelEl.textContent = `${mySensorCount} + ${swarmPeerCount}`;
                    }
                    toggleBtn.title = `Toggle swarm view: ${mySensorCount} of yours + ${swarmPeerCount} peers`;
                }
            }

            // Update secondary display based on current toggle state
            if (hasSwarmData) {
                this.updateSwarmDisplay(metric);
            } else {
                // Hide both secondary displays when no swarm data
                if (swarmValueEl) swarmValueEl.classList.add('hidden');
                if (myValueEl) myValueEl.classList.add('hidden');
            }
        });

        // Update hero swarm toggle button
        this.updateHeroSwarmToggle();
    }

    // Update the hero swarm toggle button visibility and state
    updateHeroSwarmToggle() {
        const heroToggle = document.getElementById('heroSwarmToggle');
        if (!heroToggle) return;

        // Check if any metric has swarm data
        const hasAnySwarm = Object.values(this.swarmData).some(d => d.available);
        this.hasAnySwarmData = hasAnySwarm;

        // Show/hide hero toggle
        heroToggle.classList.toggle('hidden', !hasAnySwarm);

        if (hasAnySwarm) {
            // Find the metric with the best swarm data (most peers)
            // This handles cases where only pressure has swarm data (indoor sensors)
            let bestMetric = null;
            let bestPeerCount = 0;
            for (const [metric, data] of Object.entries(this.swarmData)) {
                if (data.available && (data.swarmPeerCount || 0) >= bestPeerCount) {
                    bestMetric = metric;
                    bestPeerCount = data.swarmPeerCount || 0;
                }
            }

            const bestData = bestMetric ? this.swarmData[bestMetric] : this.swarmData.temperature;
            const mySensorCount = bestData?.mySensorCount || 1;
            const swarmPeerCount = bestData?.swarmPeerCount || 0;

            // Update count display
            const countEl = document.getElementById('heroSwarmCount');
            if (countEl) {
                countEl.textContent = `${mySensorCount} + ${swarmPeerCount}`;
            }

            // Update active state
            heroToggle.classList.toggle('active', this.globalSwarmView);
            heroToggle.title = this.globalSwarmView
                ? 'Showing swarm averages - click to show your sensors'
                : 'Showing your sensors - click to show swarm averages';
        }
    }

    // Update sensor quality badges on metric cards (now with swarm info)
    updateQualityBadges(aggregates) {
        const badges = [
            { metric: 'temperature', elementId: 'tempQualityBadge' },
            { metric: 'humidity', elementId: 'humidityQualityBadge' },
            { metric: 'pressure', elementId: 'pressureQualityBadge' }
        ];

        badges.forEach(({ metric, elementId }) => {
            const container = document.getElementById(elementId);
            if (!container) return;

            // Get sensor models from the sensors array for this metric
            const sensors = aggregates[metric]?.sensors || [];
            const sensorModels = sensors
                .map(s => s.sensorModel)
                .filter(m => m != null);

            // Get badge based on sensor quality
            const badge = SensorQuality.getBadge(sensorModels);

            // Calculate swarm info from contributing sensors
            // Get the most common swarm status and average swarm size
            const swarmData = this.calculateSwarmDataForMetric(sensors);

            // User's sensor count for this metric (shown in badge center)
            const mySensorCount = sensors.length;

            if (badge && badge.name) {
                // Generate broadcast badge with user's sensor count
                container.innerHTML = SensorQuality.generateBroadcastBadgeSVG(
                    badge,
                    mySensorCount,
                    swarmData.dominantStatus,
                    22
                );
                const statusLabel = SensorQuality.getSwarmStatusLabel(swarmData.dominantStatus);
                if (swarmData.avgSwarmSize > 0) {
                    container.setAttribute('data-tooltip',
                        `${badge.name} • ${mySensorCount} sensor${mySensorCount !== 1 ? 's' : ''} • ${statusLabel}`
                    );
                } else {
                    container.setAttribute('data-tooltip',
                        `${badge.name} • ${mySensorCount} sensor${mySensorCount !== 1 ? 's' : ''}`
                    );
                }
                container.style.display = 'inline-flex';
            } else if (mySensorCount > 0) {
                // No sensor quality badge, but we have sensors - show unknown tier badge
                const defaultBadge = { color: '#64748b', colorDark: '#475569', name: 'Unknown' };
                container.innerHTML = SensorQuality.generateBroadcastBadgeSVG(
                    defaultBadge,
                    mySensorCount,
                    swarmData.dominantStatus,
                    22
                );
                const statusLabel = SensorQuality.getSwarmStatusLabel(swarmData.dominantStatus);
                container.setAttribute('data-tooltip',
                    `${mySensorCount} sensor${mySensorCount !== 1 ? 's' : ''} • ${statusLabel}`
                );
                container.style.display = 'inline-flex';
            } else {
                // No sensors - hide container
                container.innerHTML = '';
                container.removeAttribute('data-tooltip');
                container.style.display = 'none';
            }
        });
    }

    // Calculate aggregate swarm data for sensors contributing to a metric
    calculateSwarmDataForMetric(sensors) {
        if (!sensors || sensors.length === 0) {
            return { avgSwarmSize: 0, dominantStatus: 'shield' };
        }

        // Get swarm data from window.app.sensors (full sensor objects with swarm info)
        // Note: sensors from aggregates use 'id', not 'deviceId'
        const sensorIds = sensors.map(s => s.id || s.deviceId).filter(Boolean);
        const appSensors = (window.app && window.app.sensors) ? window.app.sensors : [];
        const fullSensors = appSensors.filter(s => sensorIds.includes(s.deviceId));

        if (fullSensors.length === 0) {
            return { avgSwarmSize: 0, dominantStatus: 'shield' };
        }

        // Calculate average swarm size
        const swarmSizes = fullSensors.map(s => s.swarm_size || 0);
        const avgSwarmSize = Math.round(swarmSizes.reduce((a, b) => a + b, 0) / swarmSizes.length);

        // Find dominant swarm status (most common, or highest if tied)
        const statusCounts = {};
        fullSensors.forEach(s => {
            const status = s.swarm_status || 'shield';
            statusCounts[status] = (statusCounts[status] || 0) + 1;
        });

        // Priority: super_swarm > swarm > shield
        const statusPriority = ['super_swarm', 'swarm', 'shield'];
        let dominantStatus = 'shield';
        let maxCount = 0;

        for (const status of statusPriority) {
            if ((statusCounts[status] || 0) > maxCount) {
                maxCount = statusCounts[status];
                dominantStatus = status;
            }
        }

        return { avgSwarmSize, dominantStatus };
    }

    // Fetch fresh current values directly from history endpoint
    // This ensures the dashboard shows the most recent data, not cached values
    async refreshCurrentValues() {
        if (!this.outdoorDeviceIds || this.outdoorDeviceIds.length === 0) return;

        const deviceList = this.outdoorDeviceIds.join(',');
        const metrics = ['temperature', 'humidity', 'pressure'];
        // Use longest threshold for aggregate data (may include Meshtastic sensors)
        const aggregateFreshnessThreshold = FRESHNESS_THRESHOLDS['MESHTASTIC_PUBLIC'];
        const now = Date.now();

        try {
            // Fetch all metrics in parallel - use 10m range to get recent data
            const results = await Promise.all(
                metrics.map(async (metric) => {
                    const url = `/api/history/aggregate?devices=${deviceList}&type=${metric}&range=10m`;
                    const response = await fetch(url);
                    if (!response.ok) return { metric, value: null, stale: true };
                    const result = await response.json();
                    const data = result.data || [];
                    if (data.length > 0) {
                        const latest = data[data.length - 1];
                        const latestTime = new Date(latest.timestamp).getTime();
                        const isStale = (now - latestTime) > aggregateFreshnessThreshold;
                        return { metric, value: latest.value, stale: isStale };
                    }
                    return { metric, value: null, stale: true };
                })
            );

            // Update each metric card with fresh values (or show unavailable)
            for (const { metric, value, stale } of results) {
                if (metric === 'temperature') {
                    const tempValueEl = document.getElementById('tempCardValue');
                    const secondaryEl = document.getElementById('tempCardSecondary');

                    if (value === null || stale) {
                        tempValueEl.textContent = '--';
                        secondaryEl.textContent = 'No recent data';
                        this.updateTemperatureAmbient(null);
                    } else {
                        const newValue = value.toFixed(1);
                        this.animateValueChange(tempValueEl, newValue);

                        // Update feels-like with fresh temperature
                        const humidity = results.find(r => r.metric === 'humidity')?.value || 50;
                        const feelsLike = this.calculateFeelsLike(value, humidity);
                        secondaryEl.textContent = `Feels like ${feelsLike.toFixed(0)}°C`;

                        // Update ambient temperature zone
                        this.updateTemperatureAmbient(value);

                        // Update stored aggregates for sidebar
                        if (this.currentAggregates) {
                            this.currentAggregates.temperature = this.currentAggregates.temperature || {};
                            this.currentAggregates.temperature.avg = value;
                        }
                    }
                } else if (metric === 'humidity') {
                    const humidityValueEl = document.getElementById('humidityCardValue');
                    const secondaryEl = document.getElementById('humidityCardSecondary');

                    if (value === null || stale) {
                        humidityValueEl.textContent = '--';
                        secondaryEl.textContent = 'No recent data';
                        this.updateHumidityAmbient(null);
                    } else {
                        const newValue = Math.round(value).toString();
                        this.animateValueChange(humidityValueEl, newValue);

                        // Update dew point with fresh values
                        const temp = results.find(r => r.metric === 'temperature')?.value || 20;
                        const dewPoint = this.calculateDewPoint(temp, value);
                        secondaryEl.textContent = `Dew point ${dewPoint.toFixed(0)}°C`;

                        // Update humidity ambient
                        this.updateHumidityAmbient(value);

                        // Update stored aggregates for sidebar
                        if (this.currentAggregates) {
                            this.currentAggregates.humidity = this.currentAggregates.humidity || {};
                            this.currentAggregates.humidity.avg = value;
                        }
                    }
                } else if (metric === 'pressure') {
                    const pressureValueEl = document.getElementById('pressureCardValue');
                    const secondaryEl = document.getElementById('pressureCardSecondary');

                    if (value === null || stale) {
                        pressureValueEl.textContent = '--';
                        secondaryEl.textContent = 'No recent data';
                        this.updatePressureAmbient(null);
                    } else {
                        const newValue = value.toFixed(2);
                        this.animateValueChange(pressureValueEl, newValue);

                        // Update stored aggregates for sidebar
                        if (this.currentAggregates) {
                            this.currentAggregates.pressure = this.currentAggregates.pressure || {};
                            this.currentAggregates.pressure.avg = value;
                        }
                    }
                }
            }
        } catch (error) {
            console.error('Error refreshing current values:', error);
        }
    }

    // Start periodic refresh of current values (every 30 seconds)
    startCurrentValueRefresh() {
        // Initial refresh after 5 seconds (let initial load complete)
        setTimeout(() => this.refreshCurrentValues(), 5000);
        // Then refresh every 30 seconds
        this.currentValueRefreshInterval = setInterval(() => this.refreshCurrentValues(), 30000);
    }

    // Stop periodic refresh
    stopCurrentValueRefresh() {
        if (this.currentValueRefreshInterval) {
            clearInterval(this.currentValueRefreshInterval);
            this.currentValueRefreshInterval = null;
        }
    }

    // Animate value changes with a subtle effect
    animateValueChange(element, newValue) {
        if (!element) return;
        const oldValue = element.textContent;
        if (oldValue !== newValue && oldValue !== '--') {
            element.classList.add('updating');
            element.textContent = newValue;
            setTimeout(() => element.classList.remove('updating'), 400);
        } else {
            element.textContent = newValue;
        }
    }

    // Get temperature zone for ambient styling
    getTemperatureZone(temp) {
        if (temp === null || temp === undefined) return null;
        if (temp <= 0) return 'freezing';
        if (temp <= 10) return 'cold';
        if (temp <= 15) return 'cool';
        if (temp <= 22) return 'mild';
        if (temp <= 28) return 'warm';
        if (temp <= 35) return 'hot';
        return 'extreme';
    }

    // Update weather hero and card ambient based on temperature
    updateTemperatureAmbient(temp) {
        const zone = this.getTemperatureZone(temp);
        const heroEl = document.getElementById('weatherHero');
        const tempValueEl = document.getElementById('tempCardValue');
        const tempCardEl = document.getElementById('metricCardTemp');

        if (heroEl) {
            if (zone) {
                heroEl.setAttribute('data-temp-zone', zone);
            } else {
                heroEl.removeAttribute('data-temp-zone');
            }
        }

        if (tempValueEl) {
            if (zone) {
                tempValueEl.setAttribute('data-temp-zone', zone);
            } else {
                tempValueEl.removeAttribute('data-temp-zone');
            }
        }

        // Update temperature card background
        if (tempCardEl) {
            if (zone) {
                tempCardEl.setAttribute('data-temp-zone', zone);
            } else {
                tempCardEl.removeAttribute('data-temp-zone');
            }
        }
    }

    // Update fog effect and humidity card based on humidity
    updateHumidityAmbient(humidity) {
        this.currentHumidity = humidity;

        if (this.fogEffect) {
            this.fogEffect.setHumidity(humidity);
        }

        // Update humidity card background
        const humidityCardEl = document.getElementById('metricCardHumidity');
        if (humidityCardEl) {
            if (humidity >= 85) {
                humidityCardEl.setAttribute('data-humidity-level', 'very-high');
            } else if (humidity >= 70) {
                humidityCardEl.setAttribute('data-humidity-level', 'high');
            } else {
                humidityCardEl.removeAttribute('data-humidity-level');
            }
        }

        // Update rain effect with current conditions
        this.updateRainEffect();
    }

    // Update pressure card based on trend
    updatePressureAmbient(trend) {
        this.currentPressureTrend = trend;

        const pressureCardEl = document.getElementById('metricCardPressure');
        if (!pressureCardEl) return;

        if (trend && trend.includes('Rising')) {
            pressureCardEl.setAttribute('data-pressure-trend', 'rising');
        } else if (trend && trend.includes('Falling')) {
            if (trend.includes('Rapidly') || trend.includes('Storm')) {
                pressureCardEl.setAttribute('data-pressure-trend', 'storm');
            } else {
                pressureCardEl.setAttribute('data-pressure-trend', 'falling');
            }
        } else {
            pressureCardEl.removeAttribute('data-pressure-trend');
        }

        // Update rain effect with current conditions
        this.updateRainEffect();
    }

    // Update rain effect based on current conditions
    updateRainEffect() {
        if (this.rainEffect && this.currentHumidity !== null) {
            this.rainEffect.setRainConditions(this.currentPressureTrend, this.currentHumidity);
        }
        // After checking rain, check for clear/sunny conditions
        this.updateClearSkyEffect();
    }

    // Update clear sky / sunny effect based on conditions
    updateClearSkyEffect() {
        const heroEl = document.getElementById('weatherHero');
        if (!heroEl) return;

        // Only show clear/sunny if not raining
        if (this.rainEffect && this.rainEffect.isRaining) {
            return; // Rain effect takes precedence
        }

        const humidity = this.currentHumidity;
        const trend = this.currentPressureTrend;

        // Conditions for clear sky:
        // - Humidity below 70% (not humid)
        // - Pressure stable or rising (no storm coming)
        const isLowHumidity = humidity !== null && humidity < 70;
        const isStableOrRising = !trend ||
            trend.includes('Stable') ||
            trend.includes('Rising') ||
            trend.includes('High');

        if (isLowHumidity && isStableOrRising) {
            // Check for golden hour (sunrise/sunset times)
            const hour = new Date().getHours();
            const isGoldenHour = (hour >= 6 && hour <= 7) || (hour >= 17 && hour <= 19);

            if (isGoldenHour) {
                heroEl.setAttribute('data-weather', 'golden-hour');
            } else if (humidity < 50) {
                // Very clear - sunny
                heroEl.setAttribute('data-weather', 'sunny');
            } else {
                // Moderately clear
                heroEl.setAttribute('data-weather', 'clear');
            }
        } else {
            // Not clear conditions and not raining - remove weather attribute
            const currentWeather = heroEl.getAttribute('data-weather');
            if (currentWeather === 'clear' || currentWeather === 'sunny' || currentWeather === 'golden-hour') {
                heroEl.removeAttribute('data-weather');
            }
        }
    }

    calculateFeelsLike(temp, humidity) {
        // Heat index formula (simplified)
        if (temp < 27) return temp;
        return temp + 0.33 * (humidity / 100 * 6.105 * Math.exp(17.27 * temp / (237.7 + temp))) - 4;
    }

    calculateDewPoint(temp, humidity) {
        // Magnus formula
        const a = 17.27;
        const b = 237.7;
        const alpha = (a * temp) / (b + temp) + Math.log(humidity / 100);
        return (b * alpha) / (a - alpha);
    }

    updateAirQualityCards(aggregates) {
        // Outdoor Air - all metrics from outdoor/mixed sensors
        const outdoorPm25 = aggregates.outdoor?.pm25?.avg;
        const outdoorPm10 = aggregates.outdoor?.pm10?.avg;
        const outdoorNox = aggregates.outdoor?.nox?.avg;
        const outdoorCo2 = aggregates.outdoor?.co2?.avg;
        const outdoorVoc = aggregates.outdoor?.voc?.avg;

        // Check if we have ANY outdoor sensor data (fresh or stale sensors that should show warning)
        const hasOutdoorData = (aggregates.outdoor?.pm25?.count > 0) ||
                               (aggregates.outdoor?.co2?.count > 0) ||
                               (aggregates.outdoor?.voc?.count > 0) ||
                               Object.keys(aggregates.outdoorSensors || {}).length > 0 ||
                               Object.keys(aggregates.outdoorAreas || {}).length > 0;

        const outdoorSection = document.getElementById('outdoorSection');
        if (hasOutdoorData) {
            // Show outdoor section
            if (outdoorSection) outdoorSection.style.display = '';

            // Determine primary metric for AQI display
            let aqi, level;
            if (outdoorPm25 != null) {
                aqi = this.pm25ToAQI(outdoorPm25);
                level = this.getAQILevel(aqi);
                document.getElementById('outdoorAqiValue').textContent = `AQI ${aqi}`;
            } else if (outdoorCo2 != null) {
                // Use CO2-based index if no PM2.5
                aqi = Math.round(outdoorCo2 / 10); // Simple CO2 to index
                level = this.getIAQILevel(outdoorCo2);
                document.getElementById('outdoorAqiValue').textContent = `CO₂ ${Math.round(outdoorCo2)}`;
            } else {
                level = { label: 'Data', class: 'good' };
                document.getElementById('outdoorAqiValue').textContent = '--';
            }

            document.getElementById('outdoorAqiLabel').textContent = level.label;
            const badge = document.getElementById('outdoorAqiBadge');
            badge.className = `air-quality-badge ${level.class}`;

            // Build details string with ALL available outdoor metrics
            let details = [];
            if (outdoorPm25 != null) details.push(`PM2.5: ${outdoorPm25.toFixed(1)}`);
            if (outdoorPm10 != null) details.push(`PM10: ${outdoorPm10.toFixed(0)}`);
            if (outdoorCo2 != null) details.push(`CO₂: ${Math.round(outdoorCo2)}`);
            if (outdoorVoc != null) details.push(`VOC: ${outdoorVoc.toFixed(0)}`);
            if (outdoorNox != null) details.push(`NOx: ${outdoorNox.toFixed(0)}`);
            document.getElementById('outdoorAqiDetails').textContent = details.join(' · ') || 'No readings';

            // Render outdoor area cards (grouped sensors)
            this.renderOutdoorAreas(aggregates.outdoorAreas);
        } else {
            // Hide outdoor section when no outdoor sensors
            if (outdoorSection) outdoorSection.style.display = 'none';
        }

        // Indoor Air - all metrics from indoor sensors
        const indoorCo2 = aggregates.indoor?.co2?.avg;
        const indoorVoc = aggregates.indoor?.voc?.avg;
        const indoorPm1 = aggregates.indoor?.pm1?.avg;
        const indoorPm25 = aggregates.indoor?.pm25?.avg;

        // Check if we have ANY indoor sensor data (including temperature/humidity)
        const hasIndoorData = (aggregates.indoor?.co2?.count > 0) ||
                              (aggregates.indoor?.voc?.count > 0) ||
                              (aggregates.indoor?.pm25?.count > 0) ||
                              (aggregates.indoor?.temperature?.count > 0) ||
                              (aggregates.indoor?.humidity?.count > 0) ||
                              Object.keys(aggregates.rooms || {}).length > 0;

        const indoorSection = document.getElementById('indoorSection');
        if (hasIndoorData) {
            // Show indoor section
            if (indoorSection) indoorSection.style.display = '';

            // Determine primary metric for IAQI display
            let level;
            if (indoorCo2 != null) {
                const iaqi = this.co2ToIAQI(indoorCo2);
                level = this.getIAQILevel(indoorCo2);
                document.getElementById('indoorAqiValue').textContent = `IAQI ${iaqi}`;
            } else if (indoorVoc != null) {
                level = this.getVOCLevel(indoorVoc);
                document.getElementById('indoorAqiValue').textContent = `VOC ${Math.round(indoorVoc)}`;
            } else if (indoorPm25 != null) {
                const aqi = this.pm25ToAQI(indoorPm25);
                level = this.getAQILevel(aqi);
                document.getElementById('indoorAqiValue').textContent = `AQI ${aqi}`;
            } else {
                level = { label: 'Data', class: 'good' };
                document.getElementById('indoorAqiValue').textContent = '--';
            }

            document.getElementById('indoorAqiLabel').textContent = level.label;
            const badge = document.getElementById('indoorAqiBadge');
            badge.className = `air-quality-badge ${level.class}`;

            // Build details string with ALL available indoor metrics
            let details = [];
            if (indoorCo2 != null) details.push(`CO₂: ${Math.round(indoorCo2)}`);
            if (indoorVoc != null) details.push(`VOC: ${indoorVoc.toFixed(0)}`);
            if (indoorPm1 != null) details.push(`PM1: ${indoorPm1.toFixed(1)}`);
            if (indoorPm25 != null) details.push(`PM2.5: ${indoorPm25.toFixed(1)}`);
            document.getElementById('indoorAqiDetails').textContent = details.join(' · ') || 'No readings';

            // Render per-room cards
            this.renderRoomCards(aggregates.rooms);
        } else {
            // Hide indoor section when no indoor sensors
            if (indoorSection) indoorSection.style.display = 'none';
        }

        // Handle full-width expansion when one section is missing
        if (outdoorSection && indoorSection) {
            if (hasOutdoorData && !hasIndoorData) {
                outdoorSection.classList.add('full-width');
            } else {
                outdoorSection.classList.remove('full-width');
            }
            if (hasIndoorData && !hasOutdoorData) {
                indoorSection.classList.add('full-width');
            } else {
                indoorSection.classList.remove('full-width');
            }
        }

        // Show hints for missing sensor types
        this.updateSensorHints(hasOutdoorData, hasIndoorData);

        // Update air quality sensor badges
        this.updateAirQualityBadges(aggregates);
    }

    // Show hints when indoor or outdoor sensors are missing
    updateSensorHints(hasOutdoorData, hasIndoorData) {
        const hintEl = document.getElementById('contextualMessage');
        if (!hintEl) return;

        const hints = [];
        if (!hasOutdoorData) {
            hints.push('Add outdoor sensors for weather and air quality data');
        }
        if (!hasIndoorData) {
            hints.push('Add indoor sensors for room-by-room monitoring');
        }

        if (hints.length > 0) {
            hintEl.innerHTML = `<span class="hint-icon">💡</span> ${hints.join(' · ')}`;
            hintEl.style.display = 'block';
        } else {
            hintEl.style.display = 'none';
        }
    }

    // Update sensor quality badges on air quality cards
    updateAirQualityBadges(aggregates) {
        // Outdoor air quality badge - primarily PM2.5 sensors
        const outdoorBadgeEl = document.getElementById('outdoorAirQualityBadge');
        if (outdoorBadgeEl) {
            // Collect all outdoor air quality sensor models
            const outdoorSensorModels = [
                ...(aggregates.outdoor?.pm25?.sensorModels || []),
                ...(aggregates.outdoor?.pm10?.sensorModels || []),
                ...(aggregates.outdoor?.co2?.sensorModels || []),
                ...(aggregates.outdoor?.voc?.sensorModels || [])
            ];

            const badge = SensorQuality.getBadge(outdoorSensorModels);
            if (badge && badge.name) {
                outdoorBadgeEl.innerHTML = SensorQuality.generateBadgeSVG(badge, 16);
                outdoorBadgeEl.setAttribute('data-tooltip', `${badge.name}: ${badge.description}`);
                outdoorBadgeEl.style.display = 'inline-flex';
            } else {
                outdoorBadgeEl.innerHTML = '';
                outdoorBadgeEl.removeAttribute('data-tooltip');
                outdoorBadgeEl.style.display = 'none';
            }
        }

        // Indoor air quality badge - primarily CO2 sensors
        const indoorBadgeEl = document.getElementById('indoorAirQualityBadge');
        if (indoorBadgeEl) {
            // Collect all indoor air quality sensor models
            const indoorSensorModels = [
                ...(aggregates.indoor?.co2?.sensorModels || []),
                ...(aggregates.indoor?.voc?.sensorModels || []),
                ...(aggregates.indoor?.pm25?.sensorModels || []),
                ...(aggregates.indoor?.pm1?.sensorModels || [])
            ];

            const badge = SensorQuality.getBadge(indoorSensorModels);
            if (badge && badge.name) {
                indoorBadgeEl.innerHTML = SensorQuality.generateBadgeSVG(badge, 16);
                indoorBadgeEl.setAttribute('data-tooltip', `${badge.name}: ${badge.description}`);
                indoorBadgeEl.style.display = 'inline-flex';
            } else {
                indoorBadgeEl.innerHTML = '';
                indoorBadgeEl.removeAttribute('data-tooltip');
                indoorBadgeEl.style.display = 'none';
            }
        }
    }

    renderRoomCards(rooms) {
        const container = document.getElementById('indoorRoomsContainer');
        if (!container) return;

        // Clear existing cards
        container.innerHTML = '';

        // Sensor state thresholds
        // FRESHNESS_THRESHOLD depends on data_source (WESENSE: 10min, Meshtastic: 61min)
        const STALE_WARNING_MS = 60 * 60 * 1000;       // 1 hour - show removal countdown
        const REMOVAL_THRESHOLD_MS = 3 * 24 * 60 * 60 * 1000; // 3 days - hide sensor
        const now = Date.now();

        // Sort rooms alphabetically, filtering out rooms past removal threshold
        const roomNames = Object.keys(rooms)
            .filter(roomName => {
                const room = rooms[roomName];
                const lastSeen = room.lastSeenTimestamp ? new Date(room.lastSeenTimestamp).getTime() : 0;
                const timeSinceLastSeen = now - lastSeen;
                return timeSinceLastSeen < REMOVAL_THRESHOLD_MS;
            })
            .sort();

        if (roomNames.length === 0) {
            return;
        }

        roomNames.forEach(roomName => {
            const room = rooms[roomName];

            // Check sensor data state - threshold depends on data_source
            const freshnessThreshold = getFreshnessThreshold(room.data_source);
            const lastSeen = room.lastSeenTimestamp ? new Date(room.lastSeenTimestamp).getTime() : 0;
            const timeSinceLastSeen = now - lastSeen;
            const hasFreshData = timeSinceLastSeen < freshnessThreshold;
            const isStale = timeSinceLastSeen >= STALE_WARNING_MS;

            // Get actionable insight for this room
            let insight = this.generateRoomInsight(room);

            // Override insight based on data state
            if (isStale) {
                // > 1 hour: Show countdown to removal
                const timeRemaining = REMOVAL_THRESHOLD_MS - timeSinceLastSeen;
                const totalMinutes = Math.floor(timeRemaining / 60000);
                const days = Math.floor(totalMinutes / (24 * 60));
                const hours = Math.floor((totalMinutes % (24 * 60)) / 60);

                let countdownText;
                if (days > 0) {
                    countdownText = `${days}d ${hours}h`;
                } else {
                    const minutes = totalMinutes % 60;
                    countdownText = hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
                }
                insight = {
                    text: `No data - removing in ${countdownText}`,
                    icon: '',
                    class: 'nodata',
                    priority: 0
                };
            } else if (!hasFreshData) {
                // 7 min - 1 hour: Show "No data"
                insight = {
                    text: 'No data',
                    icon: '',
                    class: 'nodata',
                    priority: 0
                };
            }

            // Build room card HTML
            const card = document.createElement('div');
            card.className = `room-card clickable ${insight.class}`;

            // Add faded class for stale sensors to grey out the text
            if (!hasFreshData) {
                card.classList.add('stale');
            }

            // Build values row, showing "No readings" if all values are null
            const hasValues = room.temperature != null || room.humidity != null ||
                              room.co2 != null || room.pressure != null;

            // Get room type icon
            const roomType = room.roomType || 'unknown';
            const roomTypeIcon = this.roomTypeIcons[roomType] || this.roomTypeIcons['unknown'];

            // Count contributing sensors
            const sensorCount = room.sensors ? Object.keys(room.sensors).length : 1;
            const sensorBadge = sensorCount > 1 ? `<span class="sensor-count-badge">${sensorCount} sensors</span>` : '';

            // Format last seen time for non-fresh sensors
            let lastSeenText = '';
            if (!hasFreshData && room.lastSeenTimestamp) {
                const lastSeenTime = new Date(room.lastSeenTimestamp);
                const diffMs = now - lastSeenTime.getTime();
                const diffMins = Math.floor(diffMs / 60000);
                const diffHours = diffMs / (60 * 60 * 1000);
                if (diffHours >= 24) {
                    const days = Math.floor(diffHours / 24);
                    const remainingHours = Math.floor(diffHours % 24);
                    lastSeenText = remainingHours > 0 ? `Last seen ${days}d ${remainingHours}h ago` : `Last seen ${days}d ago`;
                } else if (diffMins >= 60) {
                    const hours = Math.floor(diffMins / 60);
                    const remainingMins = diffMins % 60;
                    lastSeenText = `Last seen ${hours}h ${remainingMins}m ago`;
                } else {
                    lastSeenText = `Last seen ${diffMins}m ago`;
                }
            }

            card.innerHTML = `
                <div class="room-card-header">
                    <span class="room-type-icon-wrapper">${roomTypeIcon}</span>
                    <span class="room-card-name" title="${this.escapeHtml(roomName)}">${this.escapeHtml(extractRoomDisplayName(roomName))}</span>
                    ${sensorBadge}
                    <span class="room-card-status ${insight.class}"></span>
                </div>
                <div class="room-card-row">
                    ${hasValues ? '' : '<span class="value">No readings</span>'}
                    ${room.temperature != null ? `<span class="value temp">${this.metricIcons.temperature}${room.temperature.toFixed(1)}°</span>` : ''}
                    ${room.humidity != null ? `<span class="value humidity">${this.metricIcons.humidity}${room.humidity.toFixed(1)}%</span>` : ''}
                </div>
                ${hasValues ? `<div class="room-card-row">
                    ${room.co2 != null ? `<span class="value co2">${this.metricIcons.co2}${Math.round(room.co2)} ppm</span>` : ''}
                    ${room.pressure != null ? `<span class="value pressure">${this.metricIcons.pressure}${Math.round(room.pressure)} hPa</span>` : ''}
                </div>` : ''}
                ${(room.pm1 != null || room.pm25 != null || room.pm10 != null) ? `<div class="room-card-row">
                    ${room.pm1 != null ? `<span class="value pm">${this.metricIcons.pm1}${room.pm1.toFixed(1)}</span>` : ''}
                    ${room.pm25 != null ? `<span class="value pm">${this.metricIcons.pm25}${room.pm25.toFixed(1)}</span>` : ''}
                    ${room.pm10 != null ? `<span class="value pm">${this.metricIcons.pm10}${room.pm10.toFixed(1)}</span>` : ''}
                </div>` : ''}
                <div class="room-card-insight ${insight.class}">
                    <span class="insight-icon">${insight.icon}</span>
                    <span class="insight-text">${insight.text}</span>
                </div>
                ${lastSeenText ? `<div class="room-card-lastseen">${lastSeenText}</div>` : ''}
            `;

            // Make card clickable to show room details sidebar
            card.addEventListener('click', (e) => {
                e.stopPropagation(); // Prevent bubbling to parent indoorAirCard
                this.showRoomDetails(roomName, room);
            });

            container.appendChild(card);
        });
    }

    // Render outdoor area cards (grouped sensors by area)
    renderOutdoorAreas(outdoorAreas) {
        const container = document.getElementById('outdoorSensorsContainer');
        if (!container) return;

        // Clear existing cards
        container.innerHTML = '';

        // Area state thresholds
        const STALE_WARNING_MS = 60 * 60 * 1000;       // 1 hour - show removal countdown
        const REMOVAL_THRESHOLD_MS = 3 * 24 * 60 * 60 * 1000; // 3 days - hide area
        const now = Date.now();

        // Get area list sorted by name, filtering out areas past removal threshold
        const areaNames = Object.keys(outdoorAreas)
            .filter(areaName => {
                const area = outdoorAreas[areaName];
                const lastSeen = area.lastSeenTimestamp ? new Date(area.lastSeenTimestamp).getTime() : 0;
                const timeSinceLastSeen = now - lastSeen;
                return timeSinceLastSeen < REMOVAL_THRESHOLD_MS;
            })
            .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));

        if (areaNames.length === 0) {
            return;
        }

        areaNames.forEach(areaName => {
            const area = outdoorAreas[areaName];

            // Check area data state - threshold depends on data_source
            const freshnessThreshold = getFreshnessThreshold(area.data_source);
            const lastSeen = area.lastSeenTimestamp ? new Date(area.lastSeenTimestamp).getTime() : 0;
            const timeSinceLastSeen = now - lastSeen;
            const hasFreshData = timeSinceLastSeen < freshnessThreshold;
            const isStale = timeSinceLastSeen >= STALE_WARNING_MS;

            // Get insight for this area
            const areaData = {
                temperature: area.temperature,
                humidity: area.humidity,
                co2: area.co2,
                pm25: area.pm25,
                voc: area.voc
            };
            let insight = this.generateRoomInsight(areaData);

            // Override insight based on data state
            if (isStale) {
                const timeRemaining = REMOVAL_THRESHOLD_MS - timeSinceLastSeen;
                const totalMinutes = Math.floor(timeRemaining / 60000);
                const days = Math.floor(totalMinutes / (24 * 60));
                const hours = Math.floor((totalMinutes % (24 * 60)) / 60);

                let countdownText;
                if (days > 0) {
                    countdownText = `${days}d ${hours}h`;
                } else {
                    const minutes = totalMinutes % 60;
                    countdownText = hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m`;
                }
                insight = {
                    text: `No data - removing in ${countdownText}`,
                    icon: '',
                    class: 'nodata',
                    priority: 0
                };
            } else if (!hasFreshData) {
                insight = {
                    text: 'No data',
                    icon: '',
                    class: 'nodata',
                    priority: 0
                };
            }

            // Get area type icon
            const areaType = area.areaType || 'unknown';
            const areaTypeIcon = this.areaTypeIcons[areaType] || this.areaTypeIcons['unknown'];

            // Count contributing sensors
            const sensorCount = area.sensors ? Object.keys(area.sensors).length : 1;
            const sensorBadge = sensorCount > 1 ? `<span class="sensor-count-badge">${sensorCount} sensors</span>` : '';

            // Build area card HTML
            const card = document.createElement('div');
            card.className = `outdoor-sensor-card ${insight.class}`;

            // Add stale class to grey out text for non-fresh areas
            if (!hasFreshData) {
                card.classList.add('stale');
            }

            // Build values display
            let values = [];
            if (area.pm1 != null) values.push(`<span class="sensor-value pm">${this.metricIcons.pm1}${area.pm1.toFixed(1)}</span>`);
            if (area.pm25 != null) values.push(`<span class="sensor-value pm">${this.metricIcons.pm25}${area.pm25.toFixed(1)}</span>`);
            if (area.pm10 != null) values.push(`<span class="sensor-value pm">${this.metricIcons.pm10}${area.pm10.toFixed(1)}</span>`);
            if (area.temperature != null) values.push(`<span class="sensor-value temp">${this.metricIcons.temperature}${area.temperature.toFixed(1)}°</span>`);
            if (area.humidity != null) values.push(`<span class="sensor-value humidity">${this.metricIcons.humidity}${area.humidity.toFixed(1)}%</span>`);
            if (area.co2 != null) values.push(`<span class="sensor-value co2">${this.metricIcons.co2}${Math.round(area.co2)}</span>`);
            if (area.voc != null) values.push(`<span class="sensor-value voc">${this.metricIcons.voc}${Math.round(area.voc)}</span>`);

            // Format last seen time for non-fresh areas
            let lastSeenText = '';
            if (!hasFreshData && area.lastSeenTimestamp) {
                const lastSeenTime = new Date(area.lastSeenTimestamp);
                const diffMs = now - lastSeenTime.getTime();
                const diffMins = Math.floor(diffMs / 60000);
                const diffHours = diffMs / (60 * 60 * 1000);
                if (diffHours >= 24) {
                    const days = Math.floor(diffHours / 24);
                    const remainingHours = Math.floor(diffHours % 24);
                    lastSeenText = remainingHours > 0 ? `Last seen ${days}d ${remainingHours}h ago` : `Last seen ${days}d ago`;
                } else if (diffMins >= 60) {
                    const hours = Math.floor(diffMins / 60);
                    const remainingMins = diffMins % 60;
                    lastSeenText = `Last seen ${hours}h ${remainingMins}m ago`;
                } else {
                    lastSeenText = `Last seen ${diffMins}m ago`;
                }
            }

            card.innerHTML = `
                <div class="sensor-header">
                    <span class="room-type-icon-wrapper">${areaTypeIcon}</span>
                    <span class="sensor-name" title="${this.escapeHtml(areaName)}">${this.escapeHtml(extractRoomDisplayName(areaName))}</span>
                    ${sensorBadge}
                    <span class="sensor-status ${insight.class}"></span>
                </div>
                <div class="sensor-values">${values.length > 0 ? values.join('') : '<span class="sensor-value">No readings</span>'}</div>
                <div class="sensor-insight ${insight.class}">
                    <span class="insight-icon">${insight.icon}</span>
                    <span class="insight-text">${insight.text}</span>
                </div>
                ${lastSeenText ? `<div class="sensor-updated">${lastSeenText}</div>` : ''}
            `;

            // Make card clickable to show area details sidebar
            card.addEventListener('click', (e) => {
                e.stopPropagation();
                this.showOutdoorAreaDetails(areaName, area);
            });

            container.appendChild(card);
        });
    }

    // Show outdoor sensor details in sidebar (legacy - for individual sensors)
    showOutdoorSensorDetails(sensor) {
        // Reuse the RoomDetailsSidebar for outdoor sensors
        // Map the outdoor sensor data to the room data format
        if (roomDetailsSidebar) {
            const sensorData = {
                temperature: sensor.temperature,
                humidity: sensor.humidity,
                co2: sensor.co2,
                pm25: sensor.pm25,
                voc: sensor.voc,
                pressure: null,
                sensorId: sensor.deviceId,
                boardModel: sensor.boardModel,
                sensorModels: sensor.sensorModels || {}
            };
            roomDetailsSidebar.open(sensor.name, sensorData);
        }
    }

    // Show outdoor area details in sidebar (new - for grouped areas)
    showOutdoorAreaDetails(areaName, area) {
        if (roomDetailsSidebar) {
            // Area already has the sensors object, just like rooms
            roomDetailsSidebar.open(areaName, area);
        }
    }

    // Show room details in sidebar
    showRoomDetails(roomName, room) {
        if (roomDetailsSidebar) {
            roomDetailsSidebar.open(roomName, room);
        }
    }

    // Generate actionable insight for a room based on its conditions
    generateRoomInsight(room) {
        const insights = [];

        // CO2 checks (most important for indoor air)
        if (room.co2 != null) {
            if (room.co2 > 2000) {
                return { text: 'Ventilate now', icon: '', class: 'critical', priority: 1 };
            } else if (room.co2 > 1500) {
                return { text: 'Open a window', icon: '', class: 'warning', priority: 2 };
            } else if (room.co2 > 1000) {
                insights.push({ text: 'Air getting stale', icon: '', class: 'moderate', priority: 3 });
            }
        }

        // PM2.5 checks (particles - 3D printer, dust, etc.)
        if (room.pm25 != null) {
            if (room.pm25 > 35) {
                return { text: 'High particles - ventilate', icon: '', class: 'warning', priority: 2 };
            } else if (room.pm25 > 12) {
                insights.push({ text: 'Particles elevated', icon: '', class: 'moderate', priority: 4 });
            }
        }

        // VOC checks
        if (room.voc != null) {
            if (room.voc > 300) {
                return { text: 'VOCs high - ventilate', icon: '', class: 'warning', priority: 2 };
            } else if (room.voc > 200) {
                insights.push({ text: 'VOCs moderate', icon: '', class: 'moderate', priority: 5 });
            }
        }

        // Temperature comfort checks
        if (room.temperature != null) {
            if (room.temperature > 28) {
                insights.push({ text: 'Too warm', icon: '', class: 'moderate', priority: 6 });
            } else if (room.temperature < 16) {
                insights.push({ text: 'Too cold', icon: '', class: 'moderate', priority: 6 });
            }
        }

        // Humidity checks
        if (room.humidity != null) {
            if (room.humidity > 70) {
                insights.push({ text: 'Humid - ventilate', icon: '', class: 'moderate', priority: 7 });
            } else if (room.humidity < 30) {
                insights.push({ text: 'Air too dry', icon: '', class: 'info', priority: 8 });
            }
        }

        // Return highest priority insight, or all good
        if (insights.length > 0) {
            insights.sort((a, b) => a.priority - b.priority);
            return insights[0];
        }

        // Check if we actually have any data
        const hasData = room.co2 != null || room.pm25 != null || room.voc != null ||
                        room.temperature != null || room.humidity != null;

        if (!hasData) {
            return { text: 'No recent data', icon: '', class: 'nodata', priority: 10 };
        }

        return { text: 'Air quality good', icon: '', class: 'good', priority: 10 };
    }

    getVOCLevel(voc) {
        // VOC index levels (SGP40/SGP41 scale: 0-500, higher = worse)
        if (voc <= 100) return { label: 'Excellent', class: 'good' };
        if (voc <= 200) return { label: 'Good', class: 'good' };
        if (voc <= 300) return { label: 'Moderate', class: 'moderate' };
        if (voc <= 400) return { label: 'Poor', class: 'poor' };
        return { label: 'Very Poor', class: 'poor' };
    }

    pm25ToAQI(pm25) {
        // Simplified PM2.5 to AQI conversion
        if (pm25 <= 12) return Math.round((50 / 12) * pm25);
        if (pm25 <= 35.4) return Math.round(50 + (50 / 23.4) * (pm25 - 12));
        if (pm25 <= 55.4) return Math.round(100 + (50 / 20) * (pm25 - 35.4));
        return Math.round(150 + (50 / 94.6) * (pm25 - 55.4));
    }

    getAQILevel(aqi) {
        if (aqi <= 25) return { label: 'Excellent', class: 'good' };
        if (aqi <= 50) return { label: 'Good', class: 'good' };
        if (aqi <= 100) return { label: 'Moderate', class: 'moderate' };
        if (aqi <= 150) return { label: 'Unhealthy (Sensitive)', class: 'moderate' };
        return { label: 'Poor', class: 'poor' };
    }

    co2ToIAQI(co2) {
        // Simplified CO2 to indoor air quality index
        if (co2 <= 600) return Math.round((50 / 600) * co2);
        if (co2 <= 1000) return Math.round(50 + (50 / 400) * (co2 - 600));
        return Math.round(100 + (50 / 500) * (co2 - 1000));
    }

    getIAQILevel(co2) {
        if (co2 <= 600) return { label: 'Excellent', class: 'good' };
        if (co2 <= 1000) return { label: 'Good', class: 'good' };
        if (co2 <= 1500) return { label: 'Moderate', class: 'moderate' };
        return { label: 'Poor', class: 'poor' };
    }

    updateContextualAlerts(aggregates) {
        const alertsBar = document.getElementById('contextualAlertsBar');
        const alertsScroll = document.getElementById('alertsScroll');
        if (!alertsBar || !alertsScroll) return;

        // Get dismissed alerts from localStorage (expire after 24 hours)
        const dismissedKey = 'wesense_dismissed_alerts';
        const dismissed = JSON.parse(localStorage.getItem(dismissedKey) || '{}');
        const now = Date.now();
        const twentyFourHours = 24 * 60 * 60 * 1000;

        // Clean up expired dismissals
        Object.keys(dismissed).forEach(key => {
            if (now - dismissed[key] > twentyFourHours) {
                delete dismissed[key];
            }
        });
        localStorage.setItem(dismissedKey, JSON.stringify(dismissed));

        const alerts = [];

        // Get indoor and outdoor values
        const outdoorTemp = aggregates.temperature?.avg;
        const indoorTemp = aggregates.indoor?.temperature?.avg;
        const outdoorHumidity = aggregates.humidity?.avg;
        const indoorHumidity = aggregates.indoor?.humidity?.avg;
        const indoorCo2 = aggregates.indoor?.co2?.avg;
        const outdoorPm25 = aggregates.outdoor?.pm25?.avg;
        const indoorPm25 = aggregates.indoor?.pm25?.avg;
        const pressure = aggregates.pressure?.avg;

        // High indoor humidity with lower outdoor humidity - ventilation opportunity
        if (indoorHumidity != null && outdoorHumidity != null) {
            if (indoorHumidity > 65 && outdoorHumidity < indoorHumidity - 15) {
                alerts.push({
                    id: 'humidity_high',
                    type: 'warning',
                    icon: '',
                    title: 'High Indoor Humidity',
                    action: `Open windows - outdoor is ${Math.round(outdoorHumidity)}%`
                });
            }
        }

        // High CO2 - need ventilation
        if (indoorCo2 != null) {
            if (indoorCo2 > 1500) {
                alerts.push({
                    id: 'co2_very_high',
                    type: 'alert',
                    icon: '',
                    title: `CO₂ Very High (${Math.round(indoorCo2)} ppm)`,
                    action: 'Open windows immediately'
                });
            } else if (indoorCo2 > 1000) {
                alerts.push({
                    id: 'co2_elevated',
                    type: 'warning',
                    icon: '',
                    title: `CO₂ Elevated (${Math.round(indoorCo2)} ppm)`,
                    action: 'Consider ventilating'
                });
            }
        }

        // Poor outdoor air - keep windows closed
        if (outdoorPm25 != null && outdoorPm25 > 35) {
            alerts.push({
                id: 'pm25_outdoor_high',
                type: 'alert',
                icon: '',
                title: `Poor Outdoor Air (PM2.5: ${outdoorPm25.toFixed(1)})`,
                action: 'Keep windows closed'
            });
        }

        // Good ventilation opportunity - indoor worse than outdoor
        if (indoorPm25 != null && outdoorPm25 != null) {
            if (indoorPm25 > outdoorPm25 + 5 && outdoorPm25 < 20) {
                alerts.push({
                    id: 'indoor_stale',
                    type: 'info',
                    icon: '',
                    title: 'Indoor Air Stale',
                    action: 'Outdoor air is cleaner - ventilate'
                });
            }
        }

        // Note: Low pressure is shown in the pressure indicator under hero, not as a dismissable alert

        // Temperature comfort alerts
        if (indoorTemp != null) {
            if (indoorTemp > 28) {
                alerts.push({
                    id: 'temp_high',
                    type: 'warning',
                    icon: '',
                    title: 'High Indoor Temperature',
                    action: outdoorTemp != null && outdoorTemp < indoorTemp ? 'Outdoor is cooler - ventilate' : 'Consider cooling'
                });
            } else if (indoorTemp < 16) {
                alerts.push({
                    id: 'temp_low',
                    type: 'info',
                    icon: '',
                    title: 'Low Indoor Temperature',
                    action: 'Consider heating for comfort'
                });
            }
        }

        // Filter out dismissed alerts
        const visibleAlerts = alerts.filter(alert => !dismissed[alert.id]);

        // Show/hide alerts bar
        if (visibleAlerts.length === 0) {
            alertsBar.style.display = 'none';
        } else {
            alertsBar.style.display = 'block';
            alertsScroll.innerHTML = visibleAlerts.map(alert => `
                <div class="alert-chip ${alert.type}" data-alert-id="${alert.id}">
                    <span class="alert-chip-icon">${alert.icon}</span>
                    <div class="alert-chip-content">
                        <div class="alert-chip-title">${alert.title}</div>
                        <div class="alert-chip-action">${alert.action}</div>
                    </div>
                    <button class="alert-dismiss" data-alert-id="${alert.id}">×</button>
                </div>
            `).join('');

            // Add dismiss handlers
            alertsScroll.querySelectorAll('.alert-dismiss').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const alertId = e.target.dataset.alertId;
                    const chip = e.target.closest('.alert-chip');

                    // Save to localStorage
                    const currentDismissed = JSON.parse(localStorage.getItem(dismissedKey) || '{}');
                    currentDismissed[alertId] = Date.now();
                    localStorage.setItem(dismissedKey, JSON.stringify(currentDismissed));

                    // Animate removal
                    chip.style.opacity = '0';
                    chip.style.transform = 'translateY(-10px)';
                    setTimeout(() => {
                        chip.remove();
                        // Hide bar if no alerts left
                        if (alertsScroll.children.length === 0) {
                            alertsBar.style.display = 'none';
                        }
                    }, 200);
                });
            });
        }
    }

    async updateSparklines() {
        if (this.deviceIds.length === 0) return;

        // Weather/outdoor metrics use outdoor-only sensors
        const outdoorMetrics = ['temperature', 'humidity', 'pressure', 'pm2_5'];

        const metrics = [
            { id: 'tempCardSparkline', type: 'temperature', color: '#3b82f6', yPrefix: 'temp', xPrefix: 'temp', unit: '°', decimals: 1 },
            { id: 'humidityCardSparkline', type: 'humidity', color: '#06b6d4', yPrefix: 'humidity', xPrefix: 'humidity', unit: '%', decimals: 1 },
            { id: 'pressureCardSparkline', type: 'pressure', color: '#8b5cf6', yPrefix: 'pressure', xPrefix: 'pressure', unit: '', decimals: 2 },
            { id: 'outdoorAqiSparkline', type: 'pm2_5', color: '#22c55e', yPrefix: null, xPrefix: null, unit: '', decimals: 1 },
            { id: 'indoorAqiSparkline', type: 'co2', color: '#3b82f6', yPrefix: null, xPrefix: null, unit: '', decimals: 0 }
        ];

        for (const metric of metrics) {
            try {
                // Use outdoor device IDs for weather metrics, all devices for others
                const deviceIdsToUse = outdoorMetrics.includes(metric.type)
                    ? (this.outdoorDeviceIds || this.deviceIds)
                    : this.deviceIds;

                // Skip if no devices for this metric type
                if (deviceIdsToUse.length === 0) {
                    this.clearSparkline(metric.id, metric.yPrefix);
                    continue;
                }

                const response = await fetch(`/api/history/aggregate?devices=${deviceIdsToUse.join(',')}&type=${metric.type}&range=${this.currentTimeRange}`);
                if (!response.ok) {
                    this.clearSparkline(metric.id, metric.yPrefix);
                    continue;
                }

                const result = await response.json();
                if (!result.data || result.data.length === 0) {
                    this.clearSparkline(metric.id, metric.yPrefix);
                    continue;
                }
                this.renderSparkline(metric.id, result.data, metric.color, metric);
            } catch (error) {
                console.error(`Failed to fetch sparkline data for ${metric.type}:`, error);
                this.clearSparkline(metric.id, metric.yPrefix);
            }
        }
    }

    clearSparkline(canvasId, yPrefix) {
        const canvas = document.getElementById(canvasId);
        if (canvas) {
            if (this.sparklineCharts[canvasId]) {
                this.sparklineCharts[canvasId].destroy();
                delete this.sparklineCharts[canvasId];
            }
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
        }
        // Clear Y-axis labels
        if (yPrefix) {
            const yMax = document.getElementById(`${yPrefix}YMax`);
            const yMid = document.getElementById(`${yPrefix}YMid`);
            const yMin = document.getElementById(`${yPrefix}YMin`);
            if (yMax) yMax.textContent = '--';
            if (yMid) yMid.textContent = '--';
            if (yMin) yMin.textContent = '--';
        }
    }

    // Get time range in milliseconds
    getTimeRangeMs(range) {
        const ranges = {
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000,
            '48h': 48 * 60 * 60 * 1000,
            '7d': 7 * 24 * 60 * 60 * 1000,
            '30d': 30 * 24 * 60 * 60 * 1000,
            '90d': 90 * 24 * 60 * 60 * 1000,
            '1y': 365 * 24 * 60 * 60 * 1000,
            'all': 10 * 365 * 24 * 60 * 60 * 1000
        };
        return ranges[range] || ranges['24h'];
    }

    // Get appropriate gap threshold in minutes based on time range
    getGapThresholdMinutes(range) {
        const thresholds = {
            '30m': 10,
            '1h': 15,
            '2h': 15,
            '4h': 15,
            '8h': 20,
            '24h': 30,
            '48h': 90,
            '7d': 180,
            '30d': 540,
            '90d': 2160,
            '1y': 7200,
            'all': 14400
        };
        return thresholds[range] || 30;
    }

    // Convert data to time-based format with gap detection
    convertToTimeData(data, maxGapMinutes = 30) {
        if (!data || data.length === 0) return [];

        const result = [];
        const maxGapMs = maxGapMinutes * 60 * 1000;

        for (let i = 0; i < data.length; i++) {
            const point = data[i];
            const timestamp = new Date(point.timestamp || point.bucket).getTime();

            // Check for gap from previous point
            if (i > 0) {
                const prevTimestamp = new Date(data[i-1].timestamp || data[i-1].bucket).getTime();
                const gap = timestamp - prevTimestamp;

                // If gap is too large, insert a null to break the line
                if (gap > maxGapMs) {
                    result.push({ x: prevTimestamp + 1, y: null });
                }
            }

            result.push({ x: timestamp, y: point.value });
        }

        return result;
    }

    renderSparkline(canvasId, data, color, metricConfig) {
        const canvas = document.getElementById(canvasId);
        if (!canvas || !data || data.length === 0) return;

        // Destroy existing chart
        if (this.sparklineCharts[canvasId]) {
            this.sparklineCharts[canvasId].destroy();
        }

        const ctx = canvas.getContext('2d');

        // Convert to time-based data with gap detection
        const gapThreshold = this.getGapThresholdMinutes(this.currentTimeRange);
        const timeData = this.convertToTimeData(data, gapThreshold);
        const values = data.map(d => d.value);

        // Calculate time axis bounds based on current time range
        const now = Date.now();
        const timeRangeMs = this.getTimeRangeMs(this.currentTimeRange);
        const xMin = now - timeRangeMs;
        const xMax = now;

        // Update Y-axis labels if prefix is provided
        if (metricConfig.yPrefix) {
            const minVal = Math.min(...values);
            const maxVal = Math.max(...values);
            const midVal = (minVal + maxVal) / 2;
            const unit = metricConfig.unit;
            const decimals = metricConfig.decimals;

            document.getElementById(`${metricConfig.yPrefix}YMax`).textContent = maxVal.toFixed(decimals) + unit;
            document.getElementById(`${metricConfig.yPrefix}YMid`).textContent = midVal.toFixed(decimals) + unit;
            document.getElementById(`${metricConfig.yPrefix}YMin`).textContent = minVal.toFixed(decimals) + unit;

            // Update range bar
            this.updateRangeBar(metricConfig.yPrefix, minVal, maxVal, values[values.length - 1], unit, decimals);

            // Update weather hero High/Low if this is temperature
            if (metricConfig.type === 'temperature') {
                const highLowEl = document.getElementById('weatherHighLow');
                if (highLowEl) {
                    highLowEl.textContent = `${maxVal.toFixed(0)}° / ${minVal.toFixed(0)}°`;
                }
            }
        }

        // Update X-axis labels if prefix is provided
        if (metricConfig.xPrefix && data.length > 0) {
            const xAxisEl = document.getElementById(`${metricConfig.xPrefix}XAxis`);
            if (xAxisEl) {
                const firstBucket = data[0].bucket || data[0].timestamp;
                const midIdx = Math.floor(data.length / 2);
                const midBucket = data[midIdx].bucket || data[midIdx].timestamp;

                // Parse timestamps (handle ClickHouse format "YYYY-MM-DD HH:mm:ss")
                const parseTimestamp = (ts) => {
                    if (!ts) return null;
                    const isoStr = typeof ts === 'string' ? ts.replace(' ', 'T') : ts;
                    const date = new Date(isoStr);
                    return isNaN(date.getTime()) ? null : date;
                };

                const firstTime = parseTimestamp(firstBucket);
                const midTime = parseTimestamp(midBucket);

                const spans = xAxisEl.querySelectorAll('span');
                if (spans.length >= 3) {
                    spans[0].textContent = firstTime ? this.formatXAxisTime(firstTime) : '--';
                    spans[1].textContent = midTime ? this.formatXAxisTime(midTime) : '--';
                    spans[2].textContent = 'now';
                }
            }
        }

        // Create gradient
        const gradient = ctx.createLinearGradient(0, 0, canvas.width, 0);
        gradient.addColorStop(0, color + '4D'); // 30% opacity
        gradient.addColorStop(1, color);

        this.sparklineCharts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    data: timeData,
                    borderColor: gradient,
                    borderWidth: 2,
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    spanGaps: false // Don't connect points across gaps
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false }
                },
                scales: {
                    x: {
                        type: 'time',
                        display: false,
                        min: xMin,
                        max: xMax,
                        time: {
                            unit: 'hour'
                        }
                    },
                    y: { display: false }
                },
                elements: {
                    line: {
                        capBezierPoints: true
                    }
                },
                animation: this.getChartAnimation()
            }
        });
    }

    // Get chart animation config, respecting reduced motion preference
    getChartAnimation() {
        // Check if user prefers reduced motion
        const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

        if (prefersReducedMotion) {
            return { duration: 0 };
        }

        return {
            duration: 800,
            easing: 'easeOutQuart',
            // Draw-in effect: animate from left to right
            x: {
                type: 'number',
                easing: 'easeOutQuart',
                duration: 800,
                from: NaN,
                delay(ctx) {
                    if (ctx.type !== 'data' || ctx.xStarted) return 0;
                    ctx.xStarted = true;
                    return ctx.index * 20;
                }
            },
            y: {
                type: 'number',
                easing: 'easeOutQuart',
                duration: 800,
                from: NaN,
                delay(ctx) {
                    if (ctx.type !== 'data' || ctx.yStarted) return 0;
                    ctx.yStarted = true;
                    return ctx.index * 20;
                }
            }
        };
    }

    updateRangeBar(prefix, min, max, current, unit, decimals) {
        const lowEl = document.getElementById(`${prefix}RangeLow`);
        const highEl = document.getElementById(`${prefix}RangeHigh`);
        const markerEl = document.getElementById(`${prefix}RangeMarker`);

        if (lowEl) lowEl.textContent = min.toFixed(decimals) + unit;
        if (highEl) highEl.textContent = max.toFixed(decimals) + unit;

        if (markerEl && max > min) {
            const percentage = ((current - min) / (max - min)) * 100;
            markerEl.style.left = `${Math.max(0, Math.min(100, percentage))}%`;
        }
    }

    formatXAxisTime(date) {
        if (['1h', '2h', '4h', '8h', '24h', '48h'].includes(this.currentTimeRange)) {
            const hours = date.getHours().toString().padStart(2, '0');
            const mins = date.getMinutes().toString().padStart(2, '0');
            return `${hours}:${mins}`;
        } else {
            const day = date.getDate();
            const month = date.toLocaleString('en', { month: 'short' });
            return `${day} ${month}`;
        }
    }

    async fetchComparisonData() {
        if (this.deviceIds.length === 0) return;

        try {
            const response = await fetch(`/api/comparison?devices=${this.deviceIds.join(',')}`);
            if (!response.ok) return;

            const comparison = await response.json();
            this.updateComparisonBadges(comparison);
        } catch (error) {
            console.error('Failed to fetch comparison data:', error);
        }
    }

    updateComparisonBadges(comparison) {
        const metrics = [
            { key: 'temperature', id: 'tempCardDiff', unit: '°C', decimals: 1 },
            { key: 'humidity', id: 'humidityCardDiff', unit: '%', decimals: 1 },
            { key: 'pressure', id: 'pressureCardDiff', unit: ' hPa', decimals: 2 }
        ];

        for (const metric of metrics) {
            const el = document.getElementById(metric.id);
            if (!el) continue;

            const data = comparison[metric.key];
            if (!data || data.yesterdayDiff === null) {
                el.textContent = '--';
                el.className = 'diff neutral';
                continue;
            }

            const diff = data.yesterdayDiff;
            const sign = diff >= 0 ? '+' : '';
            const arrow = diff >= 0 ? '↑' : '↓';
            el.textContent = `${sign}${diff.toFixed(metric.decimals)}${metric.unit} ${arrow}`;

            // Color based on significance
            if (Math.abs(diff) < 0.5) {
                el.className = 'diff neutral';
            } else if (diff > 0) {
                el.className = 'diff positive';
            } else {
                el.className = 'diff negative';
            }
        }
    }

    showContextualMessage(message) {
        const msgEl = document.getElementById('contextualMessage');
        if (msgEl) {
            msgEl.textContent = message;
            msgEl.classList.add('visible');
        }
    }

    hideContextualMessage() {
        const msgEl = document.getElementById('contextualMessage');
        if (msgEl) {
            msgEl.classList.remove('visible');
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Sensor Quality Rating System
// Based on WeSense wiki sensor recommendations and stability ratings
const SensorQuality = {
    // Sensor models mapped to quality ratings (1-5 stars, higher is better)
    // Priority 1 = 5 stars, Priority 2 = 4 stars, Not Recommended = 2-3 stars
    ratings: {
        // Temperature/Humidity - Priority 1 (5-star)
        'SHT45': { stars: 5, priority: 1, type: 'temperature' },
        'SHT41': { stars: 5, priority: 1, type: 'temperature' },
        'SHT4X': { stars: 5, priority: 1, type: 'temperature' },  // Generic SHT4x
        'TMP117': { stars: 5, priority: 1, type: 'temperature' },
        // Temperature/Humidity - Priority 2 (4-star)
        'SCD30': { stars: 5, priority: 1, type: 'co2' },  // Also good for temp
        'SEN55': { stars: 4, priority: 2, type: 'multi' },
        // Temperature/Humidity - Not Recommended (2-3 star)
        'BME280': { stars: 3, priority: 3, type: 'multi' },
        'BME680': { stars: 2, priority: 3, type: 'multi' },
        'AHT20': { stars: 2, priority: 3, type: 'temperature' },

        // CO2 - Priority 1 (5-star)
        // SCD30 already listed above
        // CO2 - Priority 2 (4-star)
        'SCD40': { stars: 4, priority: 2, type: 'co2' },
        'SCD41': { stars: 4, priority: 2, type: 'co2' },
        'SCD4X': { stars: 4, priority: 2, type: 'co2' },  // Generic SCD4x
        'SEN66': { stars: 4, priority: 2, type: 'co2' },
        'SEN68': { stars: 4, priority: 2, type: 'co2' },
        // CO2 - Not Recommended (1-star)
        'CM1106': { stars: 1, priority: 3, type: 'co2' },
        'CM1106-C': { stars: 1, priority: 3, type: 'co2' },

        // Pressure - Priority 1 (4-star, no 5-star zero-cal option)
        'MS5611': { stars: 4, priority: 1, type: 'pressure' },
        // Pressure - Priority 2 (4-star)
        'BMP390': { stars: 4, priority: 2, type: 'pressure' },
        'BMP390L': { stars: 4, priority: 2, type: 'pressure' },
        // Pressure - Not Recommended (3-star)
        'BMP280': { stars: 3, priority: 3, type: 'pressure' },
        // BME680 pressure already covered above

        // Particulate Matter - Priority 1 (5-star)
        'SPS30': { stars: 5, priority: 1, type: 'pm' },
        // PM - Priority 2 (4-star)
        // SEN55 already covered above
        // PM - Not Recommended (2-star)
        'PMS5003': { stars: 2, priority: 3, type: 'pm' },
        'SDS011': { stars: 2, priority: 3, type: 'pm' },

        // VOC/Air Quality - Not Recommended (2-star, but best available)
        'SGP41': { stars: 2, priority: 3, type: 'voc' },
        'SGP40': { stars: 2, priority: 3, type: 'voc' },

        // Light - Priority 1 (4-star)
        'TSL2591': { stars: 4, priority: 1, type: 'light' },
        'VEML7700': { stars: 2, priority: 3, type: 'light' },
        'BH1750': { stars: 3, priority: 2, type: 'light' },

        // UV - Priority 1 (4-star)
        'LTR-390UV': { stars: 4, priority: 1, type: 'uv' },
        'LTR390': { stars: 4, priority: 1, type: 'uv' },
        'VEML6075': { stars: 2, priority: 3, type: 'uv' },

        // Power - Priority 1 (5-star)
        'INA226': { stars: 5, priority: 1, type: 'power' },
        'INA219': { stars: 5, priority: 2, type: 'power' },
    },

    // Badge tiers based on average quality - shield design with levels
    // Colors based on traditional metallic tones
    badges: {
        platinum: { minStars: 5.0, level: 4, name: 'Platinum', color: '#e5e7eb', colorDark: '#9ca3af', description: 'Best-in-class sensors' },
        gold: { minStars: 4.5, level: 3, name: 'Gold', color: '#fcd34d', colorDark: '#b45309', description: 'Premium quality sensors' },
        silver: { minStars: 3.5, level: 2, name: 'Silver', color: '#d1d5db', colorDark: '#6b7280', description: 'Good quality sensors' },
        bronze: { minStars: 2.5, level: 1, name: 'Bronze', color: '#cd7f32', colorDark: '#8b4513', description: 'Entry-level sensors' },
        none: { minStars: 0, level: 0, name: null, color: null, description: 'Unknown sensors' }
    },

    // Get quality rating for a sensor model
    getQuality(sensorModel) {
        if (!sensorModel) return null;

        // Normalize the sensor model name (uppercase, remove common suffixes)
        const normalized = sensorModel.toUpperCase()
            .replace(/[-_\s]/g, '')
            .replace(/^SENSIRION/, '')
            .replace(/^BOSCH/, '');

        // Try exact match first
        for (const [key, value] of Object.entries(this.ratings)) {
            if (normalized === key.toUpperCase().replace(/[-_\s]/g, '')) {
                return value;
            }
        }

        // Try partial match (e.g., "SHT4X" matches "SHT45", "SHT41")
        for (const [key, value] of Object.entries(this.ratings)) {
            const keyNorm = key.toUpperCase().replace(/[-_\s]/g, '');
            if (normalized.includes(keyNorm) || keyNorm.includes(normalized)) {
                return value;
            }
        }

        return null;
    },

    // Calculate average quality from array of sensor models
    calculateAverageQuality(sensorModels) {
        if (!sensorModels || sensorModels.length === 0) return null;

        const qualities = sensorModels
            .map(model => this.getQuality(model))
            .filter(q => q !== null);

        if (qualities.length === 0) return null;

        const avgStars = qualities.reduce((sum, q) => sum + q.stars, 0) / qualities.length;
        return avgStars;
    },

    // Get badge tier based on sensor models
    getBadge(sensorModels) {
        const avgStars = this.calculateAverageQuality(sensorModels);

        if (avgStars === null) return this.badges.none;

        if (avgStars >= this.badges.platinum.minStars) return this.badges.platinum;
        if (avgStars >= this.badges.gold.minStars) return this.badges.gold;
        if (avgStars >= this.badges.silver.minStars) return this.badges.silver;
        if (avgStars >= this.badges.bronze.minStars) return this.badges.bronze;

        return this.badges.none;
    },

    // Generate SVG badge (legacy - now uses broadcast style)
    generateBadgeSVG(badge, size = 24) {
        if (!badge || !badge.name) return '';
        // Use broadcast badge with level number and shield status (no swarm)
        return this.generateBroadcastBadgeSVG(badge, badge.level || 1, 'shield', size);
    },

    // Legacy shield badge (kept for reference)
    generateShieldBadgeSVG(badge, size = 24) {
        if (!badge || !badge.name) return '';

        const level = badge.level || 1;
        const fontSize = size * 0.5;
        const centerX = size / 2;
        const centerY = size / 2 + fontSize * 0.15; // Slight offset for visual centering

        // Shield path scaled to size
        const scale = size / 24;
        const shieldPath = this.getShieldPath(scale);

        return `
            <svg class="sensor-quality-badge" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}"
                 title="${badge.name} (Level ${level}): ${badge.description}" aria-label="${badge.name} Level ${level} badge">
                <defs>
                    <linearGradient id="shieldGradient_${badge.name}_${size}" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" style="stop-color:${badge.color};stop-opacity:1" />
                        <stop offset="100%" style="stop-color:${badge.colorDark};stop-opacity:1" />
                    </linearGradient>
                </defs>
                <path d="${shieldPath}" fill="url(#shieldGradient_${badge.name}_${size})" stroke="${badge.colorDark}" stroke-width="${scale}"/>
                <text x="${centerX}" y="${centerY}"
                      font-family="system-ui, -apple-system, sans-serif"
                      font-size="${fontSize}px"
                      font-weight="bold"
                      fill="white"
                      text-anchor="middle"
                      dominant-baseline="middle"
                      style="text-shadow: 0 1px 2px rgba(0,0,0,0.3)">${level}</text>
            </svg>
        `;
    },

    // Generate broadcast badge with radiating rings - shows sensor count and quality tier
    // mySensorCount = number of user's sensors for this metric
    // swarmStatus = shield | swarm | super_swarm (affects ring animation)
    generateBroadcastBadgeSVG(badge, mySensorCount, swarmStatus, size = 24) {
        if (!badge) badge = { name: 'Unknown', color: '#64748b', colorDark: '#475569' };

        const center = size / 2;
        const innerRadius = size * 0.25;
        const fontSize = size * 0.4;

        // Unique ID for gradients/filters
        const uid = Math.random().toString(36).substr(2, 9);

        // Determine animation class based on tier
        const tierClass = badge.name?.toLowerCase() || 'unknown';
        const isGold = tierClass === 'gold' || tierClass === 'platinum';
        const isSilver = tierClass === 'silver';
        const isVerified = swarmStatus === 'swarm' || swarmStatus === 'super_swarm';

        // Ring configuration - 3 concentric arcs
        const rings = [
            { radius: size * 0.38, opacity: isVerified ? 0.8 : 0.5, delay: '0s' },
            { radius: size * 0.48, opacity: isVerified ? 0.5 : 0.3, delay: '0.15s' },
            { radius: size * 0.58, opacity: isVerified ? 0.3 : 0.15, delay: '0.3s' }
        ];

        // Arc path - quarter circle arcs on each corner
        const createArc = (r, startAngle, endAngle) => {
            const start = {
                x: center + r * Math.cos(startAngle * Math.PI / 180),
                y: center + r * Math.sin(startAngle * Math.PI / 180)
            };
            const end = {
                x: center + r * Math.cos(endAngle * Math.PI / 180),
                y: center + r * Math.sin(endAngle * Math.PI / 180)
            };
            return `M ${start.x} ${start.y} A ${r} ${r} 0 0 1 ${end.x} ${end.y}`;
        };

        // Generate ring arcs (top-right and bottom-left quadrants for broadcast feel)
        const ringsSVG = rings.map((ring, i) => `
            <g class="broadcast-ring ${isGold ? 'gold-pulse' : ''} ${isSilver ? 'silver-pulse' : ''}" style="animation-delay: ${ring.delay}">
                <path d="${createArc(ring.radius, -60, 60)}"
                      fill="none"
                      stroke="${badge.color}"
                      stroke-width="1.5"
                      stroke-linecap="round"
                      opacity="${ring.opacity}"/>
                <path d="${createArc(ring.radius, 120, 240)}"
                      fill="none"
                      stroke="${badge.color}"
                      stroke-width="1.5"
                      stroke-linecap="round"
                      opacity="${ring.opacity}"/>
            </g>
        `).join('');

        // Glow filter for gold tier
        const glowFilter = isGold ? `
            <filter id="glow_${uid}" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="1.5" result="coloredBlur"/>
                <feMerge>
                    <feMergeNode in="coloredBlur"/>
                    <feMergeNode in="SourceGraphic"/>
                </feMerge>
            </filter>
        ` : '';

        return `
            <svg class="sensor-quality-badge broadcast-badge tier-${tierClass}"
                 width="${size}" height="${size}" viewBox="0 0 ${size} ${size}"
                 aria-label="${badge.name || 'Unknown'} quality badge with ${mySensorCount} sensors">
                <defs>
                    <linearGradient id="centerGrad_${uid}" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" style="stop-color:${badge.color};stop-opacity:1" />
                        <stop offset="100%" style="stop-color:${badge.colorDark || badge.color};stop-opacity:1" />
                    </linearGradient>
                    ${glowFilter}
                </defs>
                ${ringsSVG}
                <circle cx="${center}" cy="${center}" r="${innerRadius}"
                        fill="url(#centerGrad_${uid})"
                        stroke="${badge.colorDark || badge.color}"
                        stroke-width="1"
                        ${isGold ? `filter="url(#glow_${uid})"` : ''}/>
                <text x="${center}" y="${center}"
                      font-family="system-ui, -apple-system, sans-serif"
                      font-size="${fontSize}px"
                      font-weight="bold"
                      fill="white"
                      text-anchor="middle"
                      dominant-baseline="central"
                      style="text-shadow: 0 1px 2px rgba(0,0,0,0.4)">${mySensorCount}</text>
            </svg>
        `;
    },

    // Legacy method - redirects to broadcast badge
    generateSwarmBadgeSVG(badge, swarmSize, swarmStatus, size = 24) {
        // For backward compatibility, use swarmSize as mySensorCount
        // This will be updated by callers to pass actual user sensor count
        return this.generateBroadcastBadgeSVG(badge, swarmSize, swarmStatus, size);
    },

    // Get swarm status label for display
    getSwarmStatusLabel(swarmStatus) {
        const labels = {
            'shield': 'Unverified',
            'swarm': 'Peer Verified',
            'super_swarm': 'Super Swarm'
        };
        return labels[swarmStatus] || 'Unknown';
    },

    // Shield path - classic badge/shield shape
    getShieldPath(scale = 1) {
        // Shield path for 24x24 viewBox, then scaled
        const s = scale;
        return `M${12*s} ${2*s}
                L${4*s} ${5*s}
                L${4*s} ${11*s}
                C${4*s} ${16*s} ${8*s} ${20*s} ${12*s} ${22*s}
                C${16*s} ${20*s} ${20*s} ${16*s} ${20*s} ${11*s}
                L${20*s} ${5*s}
                Z`;
    },

    // Darken a hex color
    darkenColor(hex, percent) {
        const num = parseInt(hex.replace('#', ''), 16);
        const amt = Math.round(2.55 * percent);
        const R = Math.max((num >> 16) - amt, 0);
        const G = Math.max((num >> 8 & 0x00FF) - amt, 0);
        const B = Math.max((num & 0x0000FF) - amt, 0);
        return '#' + (0x1000000 + R * 0x10000 + G * 0x100 + B).toString(16).slice(1);
    }
};

// Global new dashboard instance
let newDashboardLayout = null;

class Respiro {
    constructor() {
        this.map = null;
        this.tileLayer = null;
        this.markers = new Map();
        this.markerCluster = null;
        this.sensors = [];
        this.allSensorsForAging = [];  // All sensors (30d) for aging distribution
        this.leaderboardData = { byNodes: [], bySensors: [], byTypes: [] };  // Town leaderboards (ADM2)
        this.envLeaderboardData = { cleanestAir: [], bestWeather: [], mostStable: [], hottest: [] };  // Environmental leaderboards
        this.totalRegionsCount = 0;  // Total unique ADM2 regions with sensors
        this.refreshInterval = null;
        this.selectedSensorId = null;
        this.currentTimeRange = '30m';
        this.historicalData = {};
        this.activeFilters = {
            source: 'all',
            board: 'all',
            location: 'all',
            environment: 'all',
            metric: 'all',
            deploymentTypes: new Set(['OUTDOOR', 'MIXED'])  // Map view deployment filter (multi-select)
        };

        // Region overlay state
        this.regionViewActive = false;
        this.regionLayer = null;
        this.pmtilesProtocol = null;   // PMTiles protocol handler
        this.regionBoundaries = null;  // Cached GeoJSON boundaries (legacy fallback)
        this.regionalData = {};        // Aggregated sensor data by region
        this.regionMetric = 'temperature';
        this.regionUnit = '';
        this.regionDeploymentTypes = new Set(['OUTDOOR', 'MIXED']);  // Default to outdoor + mixed sensors
        this.regionTimeWindow = '1h';          // Live time window: 30m, 1h, 2h, 4h, 24h
        this.regionHistoricalMode = false;     // Historical mode toggle
        this.regionHistoricalDate = null;      // Selected historical date
        this.regionHistoricalHour = 12;        // Selected hour (0-23)

        // Viewer tracking
        this.viewerId = localStorage.getItem('viewerId') || this.generateViewerId();
        localStorage.setItem('viewerId', this.viewerId);
        this.viewerCount = 0;

        this.init();
    }

    async init() {
        await this.initMap();
        this.setupTimeframeSelector();
        this.setupDarkMode();
        this.setupRefreshButton();
        this.setupHelpModal();
        this.setupFilters();
        this.setupCollapsibleSections();
        this.setupViewSwitching();
        this.setupDashboardTabs();
        this.setupTrendTimeframeSelector();
        this.setupRegionView();
        this.setupStats();
        this.setupViewerTracking();
        this.setupDetailsSidebar();  // Initialize details sidebar
        this.setupDashboardTimeRange();  // Dashboard time range selector
        this.loadSensors();
        this.loadAllSensorsForAging();  // Load all sensors for aging distribution
        this.loadLeaderboard();  // Load ADM2 leaderboard data
        this.loadEnvLeaderboard();  // Load environmental leaderboard data
        this.startAutoRefresh();
    }
    
    setupDarkMode() {
        const toggle = document.getElementById('darkModeToggle');
        const isDarkMode = localStorage.getItem('darkMode') === 'true';

        // SVG icons for dark mode toggle
        const sunIcon = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/>
            <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
            <line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/>
            <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
        </svg>`;
        const moonIcon = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
        </svg>`;

        // Sync both html and body elements (html is set early in head to prevent flash)
        if (isDarkMode) {
            document.documentElement.classList.add('dark-mode');
            document.body.classList.add('dark-mode');
            toggle.innerHTML = sunIcon;
        } else {
            toggle.innerHTML = moonIcon;
        }

        toggle.addEventListener('click', () => {
            document.documentElement.classList.toggle('dark-mode');
            document.body.classList.toggle('dark-mode');
            const isNowDark = document.body.classList.contains('dark-mode');
            localStorage.setItem('darkMode', isNowDark);
            toggle.innerHTML = isNowDark ? sunIcon : moonIcon;

            // Regenerate map markers to update icon colors for dark mode
            this.updateMap();
            // Reapply geocoder dark mode after toggle
            this.applyGeocoderDarkMode();
        });
    }

    generateViewerId() {
        return 'v_' + Math.random().toString(36).substring(2, 15) + Date.now().toString(36);
    }

    setupViewerTracking() {
        // Send initial heartbeat
        this.sendHeartbeat();

        // Send heartbeat every 30 seconds
        setInterval(() => this.sendHeartbeat(), 30000);
    }

    setupDetailsSidebar() {
        // Initialize the global sidebar instance
        detailsSidebar = new DetailsSidebar();

        // Initialize the room details sidebar
        roomDetailsSidebar = new RoomDetailsSidebar();

        // Initialize the sensor management panel
        sensorPanel = new SensorPanel();

        // Initialize the location manager
        locationManager = new LocationManager();

        // Initialize the new dashboard layout
        newDashboardLayout = new NewDashboardLayout();

        // Add click handlers to clickable widgets (legacy widgets)
        document.querySelectorAll('.epaper-widget.clickable').forEach(widget => {
            widget.addEventListener('click', () => {
                const metric = widget.dataset.metric;
                if (!metric) return;

                // Get current value from the widget
                let currentValue = '--';
                if (metric === 'temperature') {
                    currentValue = document.getElementById('avgTemp')?.textContent || '--';
                } else if (metric === 'humidity') {
                    currentValue = document.getElementById('avgHumidity')?.textContent || '--';
                } else if (metric === 'pressure') {
                    currentValue = document.getElementById('avgPressure')?.textContent || '--';
                } else if (metric === 'co2') {
                    currentValue = document.getElementById('avgCo2')?.textContent || '--';
                } else if (metric === 'pm2_5') {
                    currentValue = document.getElementById('avgPm25')?.textContent || '--';
                }

                // Get favorites list for API call
                const favorites = this.getFavorites();

                // Open sidebar with current data and device IDs
                detailsSidebar.open(metric, currentValue, { deviceIds: favorites });
            });
        });
    }

    setupDashboardTimeRange() {
        this.dashboardTimeRange = '24h';

        // Main time range buttons
        document.querySelectorAll('.dashboard-time-btn[data-range]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                // Don't handle the "More" button parent
                if (!e.target.dataset.range) return;

                // Update active state
                document.querySelectorAll('.dashboard-time-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');

                this.dashboardTimeRange = e.target.dataset.range;
                this.updateDashboardSparklines();
            });
        });

        // Dropdown time options
        document.querySelectorAll('.dashboard-time-dropdown button').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const range = e.target.dataset.range;
                if (!range) return;

                // Update active state on main buttons (remove active from all)
                document.querySelectorAll('.dashboard-time-btn').forEach(b => b.classList.remove('active'));

                this.dashboardTimeRange = range;
                this.updateDashboardSparklines();

                // Hide dropdown
                const dropdown = e.target.closest('.dashboard-time-dropdown');
                if (dropdown) dropdown.style.display = 'none';
            });
        });

        // Toggle dropdown visibility on "More" button click
        const moreContainer = document.querySelector('.dashboard-time-more');
        if (moreContainer) {
            const moreBtn = moreContainer.querySelector('.dashboard-time-btn');
            const dropdown = moreContainer.querySelector('.dashboard-time-dropdown');

            moreBtn?.addEventListener('click', () => {
                if (dropdown) {
                    dropdown.style.display = dropdown.style.display === 'block' ? 'none' : 'block';
                }
            });

            // Close dropdown when clicking outside
            document.addEventListener('click', (e) => {
                if (!moreContainer.contains(e.target) && dropdown) {
                    dropdown.style.display = 'none';
                }
            });
        }
    }

    updateDashboardSparklines() {
        // This will be connected to real data in a later phase
        // For now, update the timeframe label
        document.querySelectorAll('.epaper-sparkline-timeframe').forEach(el => {
            const rangeLabels = {
                '1h': 'Last 1 hour',
                '2h': 'Last 2 hours',
                '4h': 'Last 4 hours',
                '8h': 'Last 8 hours',
                '24h': 'Last 24 hours',
                '48h': 'Last 48 hours',
                '7d': 'Last 7 days',
                '30d': 'Last 30 days',
                '90d': 'Last 90 days',
                '1y': 'Last year',
                'all': 'All time'
            };
            el.textContent = rangeLabels[this.dashboardTimeRange] || 'Last 24 hours';
        });

        console.log('Dashboard time range changed to:', this.dashboardTimeRange);
        // TODO: Reload sparklines with new time range data
    }

    async sendHeartbeat() {
        try {
            const response = await fetch('/api/viewers/heartbeat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ viewerId: this.viewerId })
            });
            const data = await response.json();
            this.viewerCount = data.count;
            this.updateViewerDisplay();
        } catch (err) {
            console.warn('Failed to send viewer heartbeat:', err);
        }
    }

    updateViewerDisplay() {
        const display = document.getElementById('viewerCount');
        if (display) {
            display.textContent = this.viewerCount;
            display.title = `${this.viewerCount} viewer${this.viewerCount !== 1 ? 's' : ''} online in last 60 seconds`;
        }
    }

    setupRegionView() {
        const metricSelect = document.getElementById('regionMetricSelect');
        const sensorMetricFilter = document.getElementById('sensorMetricFilter');

        console.log('setupRegionView called', { metricSelect });

        if (!metricSelect) {
            console.error('Region metric select not found!');
            return;
        }

        // Set up sensor metric filter
        if (sensorMetricFilter) {
            sensorMetricFilter.addEventListener('change', (e) => {
                this.activeFilters.metric = e.target.value;
                this.updateMap();
            });
        }

        // Set up sensor deployment multi-select filter
        this.setupDeploymentMultiSelect();

        // Change metric for region coloring
        metricSelect.addEventListener('change', async (e) => {
            this.regionMetric = e.target.value;
            if (this.regionViewActive) {
                await this.refreshRegionData();
            }
        });

        // Set up region deployment multi-select filter
        this.setupRegionDeploymentMultiSelect();

        // Set up region time controls (window dropdown, historical toggle)
        this.setupRegionTimeControls();
    }

    setupDeploymentMultiSelect() {
        const dropdown = document.getElementById('deploymentFilterDropdown');
        const toggle = document.getElementById('deploymentFilterToggle');
        const optionsContainer = document.getElementById('deploymentFilterOptions');
        const selectAllCheckbox = document.getElementById('deploymentSelectAll');

        if (!dropdown || !toggle || !optionsContainer) return;

        // Track available deployment types (will be populated from sensor data)
        this.availableDeploymentTypes = new Set();

        // Toggle dropdown open/close
        toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('open');
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!dropdown.contains(e.target)) {
                dropdown.classList.remove('open');
            }
        });

        // Handle Select All checkbox
        selectAllCheckbox.addEventListener('change', () => {
            const isChecked = selectAllCheckbox.checked;
            const checkboxes = optionsContainer.querySelectorAll('input[type="checkbox"]:not(#deploymentSelectAll)');

            checkboxes.forEach(cb => {
                cb.checked = isChecked;
            });

            // Update the filter
            if (isChecked) {
                this.activeFilters.deploymentTypes = new Set(this.availableDeploymentTypes);
            } else {
                this.activeFilters.deploymentTypes = new Set();
            }

            this.updateDeploymentLabel();
            this.updateMap();
        });
    }

    setupRegionDeploymentMultiSelect() {
        const dropdown = document.getElementById('regionDeploymentDropdown');
        const toggle = document.getElementById('regionDeploymentToggle');
        const optionsContainer = document.getElementById('regionDeploymentOptions');
        const selectAllCheckbox = document.getElementById('regionDeploymentSelectAll');

        if (!dropdown || !toggle || !optionsContainer) return;

        // Track available deployment types for region view
        this.availableRegionDeploymentTypes = new Set();

        // Toggle dropdown open/close
        toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('open');
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!dropdown.contains(e.target)) {
                dropdown.classList.remove('open');
            }
        });

        // Handle Select All checkbox
        selectAllCheckbox.addEventListener('change', async () => {
            const isChecked = selectAllCheckbox.checked;
            const checkboxes = optionsContainer.querySelectorAll('input[type="checkbox"]:not(#regionDeploymentSelectAll)');

            checkboxes.forEach(cb => {
                cb.checked = isChecked;
            });

            // Update the filter
            if (isChecked) {
                this.regionDeploymentTypes = new Set(this.availableRegionDeploymentTypes);
            } else {
                this.regionDeploymentTypes = new Set();
            }

            this.updateRegionDeploymentLabel();
            if (this.regionViewActive) {
                await this.refreshRegionData();
            }
        });
    }

    populateRegionDeploymentTypes() {
        const optionsContainer = document.getElementById('regionDeploymentOptions');
        const selectAllCheckbox = document.getElementById('regionDeploymentSelectAll');
        if (!optionsContainer) return;

        // Collect unique deployment types from sensors
        const typeCounts = new Map();
        this.sensors.forEach(sensor => {
            const envType = this.getEnvironmentType(sensor);
            typeCounts.set(envType, (typeCounts.get(envType) || 0) + 1);
        });

        // Store available types
        this.availableRegionDeploymentTypes = new Set(typeCounts.keys());

        // Remove existing dynamic options (keep Select All)
        optionsContainer.querySelectorAll('.multi-select-option:not(.select-all)').forEach(el => el.remove());
        optionsContainer.querySelectorAll('.multi-select-divider').forEach(el => el.remove());

        // Sort types: OUTDOOR first, then MIXED, then alphabetically
        const sortOrder = ['OUTDOOR', 'MIXED', 'INDOOR', 'PORTABLE', 'MOBILE', 'DEVICE', 'UNKNOWN'];
        const sortedTypes = Array.from(typeCounts.keys()).sort((a, b) => {
            const aIdx = sortOrder.indexOf(a);
            const bIdx = sortOrder.indexOf(b);
            if (aIdx === -1 && bIdx === -1) return a.localeCompare(b);
            if (aIdx === -1) return 1;
            if (bIdx === -1) return -1;
            return aIdx - bIdx;
        });

        // Add divider after Select All
        const divider = document.createElement('div');
        divider.className = 'multi-select-divider';
        optionsContainer.appendChild(divider);

        // Add checkbox for each type
        sortedTypes.forEach(type => {
            const count = typeCounts.get(type);
            const isChecked = this.regionDeploymentTypes.has(type);
            const displayName = this.formatDeploymentTypeName(type);

            const label = document.createElement('label');
            label.className = 'multi-select-option';
            label.innerHTML = `
                <input type="checkbox" value="${type}" ${isChecked ? 'checked' : ''}>
                <span>${displayName} (${count})</span>
            `;

            const checkbox = label.querySelector('input');
            checkbox.addEventListener('change', async () => {
                if (checkbox.checked) {
                    this.regionDeploymentTypes.add(type);
                } else {
                    this.regionDeploymentTypes.delete(type);
                }
                this.updateRegionSelectAllState();
                this.updateRegionDeploymentLabel();
                if (this.regionViewActive) {
                    await this.refreshRegionData();
                }
            });

            optionsContainer.appendChild(label);
        });

        // Update Select All state based on current selection
        this.updateRegionSelectAllState();
        this.updateRegionDeploymentLabel();
    }

    updateRegionSelectAllState() {
        const selectAllCheckbox = document.getElementById('regionDeploymentSelectAll');
        if (!selectAllCheckbox) return;

        const allSelected = this.availableRegionDeploymentTypes.size > 0 &&
            Array.from(this.availableRegionDeploymentTypes).every(t => this.regionDeploymentTypes.has(t));
        const someSelected = this.regionDeploymentTypes.size > 0 && !allSelected;

        selectAllCheckbox.checked = allSelected;
        selectAllCheckbox.indeterminate = someSelected;
    }

    updateRegionDeploymentLabel() {
        const label = document.querySelector('#regionDeploymentToggle .multi-select-label');
        if (!label) return;

        const selected = this.regionDeploymentTypes;
        const total = this.availableRegionDeploymentTypes.size;

        if (selected.size === 0) {
            label.textContent = 'None';
        } else if (selected.size === total) {
            label.textContent = 'All Types';
        } else if (selected.size <= 2) {
            const names = Array.from(selected).map(t => this.formatDeploymentTypeName(t));
            label.textContent = names.join(', ');
        } else {
            label.textContent = `${selected.size} selected`;
        }
    }

    /**
     * Set up region time controls (window selector + historical mode)
     */
    setupRegionTimeControls() {
        const timeWindowSelect = document.getElementById('regionTimeWindow');
        const historicalToggle = document.getElementById('regionHistoricalToggle');
        const historicalControls = document.getElementById('historicalControls');
        const datePicker = document.getElementById('regionDatePicker');
        const hourSlider = document.getElementById('regionHourSlider');
        const hourLabel = document.getElementById('regionHourLabel');
        const controlsContainer = document.getElementById('mapOverlayControls');

        if (!timeWindowSelect) return;

        // Prevent map from capturing any events on the controls container
        if (controlsContainer && typeof L !== 'undefined') {
            L.DomEvent.disableClickPropagation(controlsContainer);
            L.DomEvent.disableScrollPropagation(controlsContainer);
        }

        // Get timezone abbreviation for display (e.g., "NZDT", "PST", "UTC")
        const tzAbbr = new Intl.DateTimeFormat('en', { timeZoneName: 'short' })
            .formatToParts(new Date())
            .find(p => p.type === 'timeZoneName')?.value || 'local';

        // Set up time window dropdown
        timeWindowSelect.addEventListener('change', async (e) => {
            this.regionTimeWindow = e.target.value;
            if (this.regionViewActive && !this.regionHistoricalMode) {
                await this.refreshRegionData();
            }
        });

        // Set up historical toggle
        if (historicalToggle && historicalControls) {
            historicalToggle.addEventListener('change', async (e) => {
                this.regionHistoricalMode = e.target.checked;
                const toggleLabel = historicalToggle.closest('.historical-toggle');

                if (this.regionHistoricalMode) {
                    historicalControls.classList.add('visible');
                    if (toggleLabel) toggleLabel.classList.add('active');

                    // Set default date to yesterday (guaranteed to have full day of data)
                    if (!this.regionHistoricalDate) {
                        const yesterday = new Date();
                        yesterday.setDate(yesterday.getDate() - 1);
                        // Store as YYYY-MM-DD string in local time
                        this.regionHistoricalDate = this.formatLocalDate(yesterday);
                        datePicker.value = this.regionHistoricalDate;
                        // Default to noon
                        this.regionHistoricalHour = 12;
                        hourSlider.value = 12;
                        hourLabel.textContent = `12:00 ${tzAbbr}`;
                    }

                    // Set date limits: max = today, min = 30 days ago (in local time)
                    const today = new Date();
                    const minDate = new Date(today);
                    minDate.setDate(minDate.getDate() - 30);
                    datePicker.max = this.formatLocalDate(today);
                    datePicker.min = this.formatLocalDate(minDate);

                    if (this.regionViewActive) {
                        await this.refreshRegionData();
                    }
                } else {
                    historicalControls.classList.remove('visible');
                    if (toggleLabel) toggleLabel.classList.remove('active');
                    if (this.regionViewActive) {
                        await this.refreshRegionData();
                    }
                }
            });
        }

        // Set up date picker
        if (datePicker) {
            datePicker.addEventListener('change', async (e) => {
                // Store as YYYY-MM-DD string directly (local time)
                this.regionHistoricalDate = e.target.value || null;
                if (this.regionViewActive && this.regionHistoricalMode) {
                    await this.refreshRegionData();
                }
            });
        }

        // Set up hour slider
        if (hourSlider && hourLabel) {
            hourSlider.addEventListener('input', (e) => {
                this.regionHistoricalHour = parseInt(e.target.value);
                hourLabel.textContent = `${this.regionHistoricalHour.toString().padStart(2, '0')}:00 ${tzAbbr}`;
            });

            // Set initial label with timezone
            hourLabel.textContent = `${this.regionHistoricalHour.toString().padStart(2, '0')}:00 ${tzAbbr}`;

            hourSlider.addEventListener('change', async () => {
                if (this.regionViewActive && this.regionHistoricalMode) {
                    await this.refreshRegionData();
                }
            });

            // Prevent map from capturing slider drag events
            const stopPropagation = (e) => e.stopPropagation();
            hourSlider.addEventListener('mousedown', stopPropagation);
            hourSlider.addEventListener('touchstart', stopPropagation);
            hourSlider.addEventListener('pointerdown', stopPropagation);
        }
    }

    /**
     * Format a Date object as YYYY-MM-DD in local time
     */
    formatLocalDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    /**
     * Get ISO timestamp for historical query, respecting local timezone
     * The slider hour represents local time, so we construct a local Date
     * and convert to ISO for the API (which expects UTC)
     */
    getRegionTimestamp() {
        if (!this.regionHistoricalDate) return null;

        // regionHistoricalDate is stored as "YYYY-MM-DD" string (local date)
        // regionHistoricalHour is 0-23 (local hour)
        const [year, month, day] = this.regionHistoricalDate.split('-').map(Number);

        // Create Date in local time
        const localDate = new Date(year, month - 1, day, this.regionHistoricalHour, 0, 0, 0);

        // Convert to ISO string (this converts to UTC)
        return localDate.toISOString();
    }

    populateDeploymentTypes() {
        const optionsContainer = document.getElementById('deploymentFilterOptions');
        const selectAllCheckbox = document.getElementById('deploymentSelectAll');
        if (!optionsContainer) return;

        // Collect unique deployment types from sensors
        const typeCounts = new Map();
        this.sensors.forEach(sensor => {
            const envType = this.getEnvironmentType(sensor);
            typeCounts.set(envType, (typeCounts.get(envType) || 0) + 1);
        });

        // Store available types
        this.availableDeploymentTypes = new Set(typeCounts.keys());

        // Remove existing dynamic options (keep Select All and divider)
        optionsContainer.querySelectorAll('.multi-select-option:not(.select-all)').forEach(el => el.remove());
        optionsContainer.querySelectorAll('.multi-select-divider').forEach(el => el.remove());

        // Sort types: OUTDOOR first, then MIXED, then alphabetically
        const sortOrder = ['OUTDOOR', 'MIXED', 'INDOOR', 'PORTABLE', 'MOBILE', 'DEVICE', 'UNKNOWN'];
        const sortedTypes = Array.from(typeCounts.keys()).sort((a, b) => {
            const aIdx = sortOrder.indexOf(a);
            const bIdx = sortOrder.indexOf(b);
            if (aIdx === -1 && bIdx === -1) return a.localeCompare(b);
            if (aIdx === -1) return 1;
            if (bIdx === -1) return -1;
            return aIdx - bIdx;
        });

        // Add divider after Select All
        const divider = document.createElement('div');
        divider.className = 'multi-select-divider';
        optionsContainer.appendChild(divider);

        // Add checkbox for each type
        sortedTypes.forEach(type => {
            const count = typeCounts.get(type);
            const isChecked = this.activeFilters.deploymentTypes.has(type);
            const displayName = this.formatDeploymentTypeName(type);

            const label = document.createElement('label');
            label.className = 'multi-select-option';
            label.innerHTML = `
                <input type="checkbox" value="${type}" ${isChecked ? 'checked' : ''}>
                <span>${displayName} (${count})</span>
            `;

            const checkbox = label.querySelector('input');
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    this.activeFilters.deploymentTypes.add(type);
                } else {
                    this.activeFilters.deploymentTypes.delete(type);
                }
                this.updateSelectAllState();
                this.updateDeploymentLabel();
                this.updateMap();
            });

            optionsContainer.appendChild(label);
        });

        // Update Select All state based on current selection
        this.updateSelectAllState();
        this.updateDeploymentLabel();
    }

    formatDeploymentTypeName(type) {
        const names = {
            'OUTDOOR': 'Outdoor',
            'INDOOR': 'Indoor',
            'MIXED': 'Mixed',
            'PORTABLE': 'Portable',
            'MOBILE': 'Mobile',
            'DEVICE': 'Device',
            'UNKNOWN': 'Unknown'
        };
        return names[type] || type.charAt(0).toUpperCase() + type.slice(1).toLowerCase();
    }

    updateSelectAllState() {
        const selectAllCheckbox = document.getElementById('deploymentSelectAll');
        if (!selectAllCheckbox) return;

        const allSelected = this.availableDeploymentTypes.size > 0 &&
            Array.from(this.availableDeploymentTypes).every(t => this.activeFilters.deploymentTypes.has(t));
        const someSelected = this.activeFilters.deploymentTypes.size > 0 && !allSelected;

        selectAllCheckbox.checked = allSelected;
        selectAllCheckbox.indeterminate = someSelected;
    }

    updateDeploymentLabel() {
        const label = document.querySelector('#deploymentFilterToggle .multi-select-label');
        if (!label) return;

        const selected = this.activeFilters.deploymentTypes;
        const total = this.availableDeploymentTypes.size;

        if (selected.size === 0) {
            label.textContent = 'None';
        } else if (selected.size === total) {
            label.textContent = 'All Types';
        } else if (selected.size <= 2) {
            const names = Array.from(selected).map(t => this.formatDeploymentTypeName(t));
            label.textContent = names.join(', ');
        } else {
            label.textContent = `${selected.size} selected`;
        }
    }

    // Region Overlay implementation per architecture doc Section 6.2
    async enableRegionView() {
        console.log('Enabling region view (architecture Section 6.2)...');

        // Hide marker clusters when region view is active
        if (this.markerCluster) {
            this.map.removeLayer(this.markerCluster);
        }

        // Load data FIRST, before creating layer, so colors are ready
        await this.refreshRegionData();

        console.log('Region data loaded before layer creation:', {
            dataCount: Object.keys(this.regionalData || {}).length,
            sampleKeys: Object.keys(this.regionalData || {}).slice(0, 3),
            zoom: this.map.getZoom()
        });

        // Create PMTiles boundary layer with dynamic coloring (data is now ready)
        this.createPMTilesLayer();

        // Setup legend
        this.setupRegionTooltip();

        // Listen for zoom changes only (data is global, panning doesn't need refetch)
        // Only refetch when admin level changes (zoom 0-1 → 2-4 → 5-7 → 8-10 → 11+)
        this._onZoomEnd = this._debounce(() => this.refreshRegionData(), 300);
        this.map.on('zoomend', this._onZoomEnd);

        // Add click handler to inspect regions
        this._onMapClick = async (e) => {
            await this.showRegionInfoPopup(e.latlng);
        };
        this.map.on('click', this._onMapClick);
    }

    // Show popup with region info when clicking on the map
    async showRegionInfoPopup(latlng) {
        const zoom = this.map.getZoom();
        const metric = this.regionMetric;
        const deploymentTypes = Array.from(this.regionDeploymentTypes).join(',');

        // Normalize longitude to -180 to 180 range (handles world wrap when zoomed out)
        let lng = latlng.lng;
        while (lng > 180) lng -= 360;
        while (lng < -180) lng += 360;

        // Build URL with time parameters
        let url = `/api/regions/at-point?lat=${latlng.lat}&lng=${lng}&zoom=${zoom}&metric=${metric}&deployment=${deploymentTypes}`;

        if (this.regionHistoricalMode && this.regionHistoricalDate) {
            const timestamp = this.getRegionTimestamp();
            url += `&timestamp=${encodeURIComponent(timestamp)}`;
        } else {
            url += `&range=${this.regionTimeWindow}`;
        }

        try {
            const response = await fetch(url);
            const data = await response.json();

            let content;
            if (!data.found) {
                content = `
                    <div style="min-width: 200px;">
                        <strong>No Region Data</strong><br>
                        <small style="color: #666;">
                            No region boundary at this location.<br>
                            Lat: ${latlng.lat.toFixed(4)}, Lng: ${latlng.lng.toFixed(4)}
                        </small>
                    </div>
                `;
            } else if (!data.data) {
                content = `
                    <div style="min-width: 200px;">
                        <strong>${data.name}</strong><br>
                        <small style="color: #888;">${data.region_id}</small><br><br>
                        <span style="color: #c00;">No sensor data</span><br>
                        <small>No sensors have reported ${metric} readings in this region.</small><br><br>
                        <small style="color: #666;">
                            ADM Level: ${data.admin_level}<br>
                            Click location: ${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}
                        </small>
                    </div>
                `;
            } else {
                const d = data.data;
                const devices = data.devices || [];

                // Build device list HTML - use lazy loading link instead of inline data
                const sensorCount = d.sensor_count || 0;
                const deviceListHtml = `
                    <hr style="margin: 8px 0; border: none; border-top: 1px solid #ddd;">
                    <div id="regionDeviceList" style="font-size: 11px;">
                        <a href="#" id="loadRegionDevices" style="color: #3498db; text-decoration: none;"
                           data-region="${data.region_id}"
                           data-metric="${metric}"
                           data-deployment="${data.deployment_filter}"
                           data-range="${data.time_window}"
                           data-unit="${data.unit}">
                            Show ${sensorCount} contributing sensor${sensorCount !== 1 ? 's' : ''}...
                        </a>
                    </div>
                `;

                // Format time window for display
                const timeWindowLabel = data.time_window === '30m' ? '30 min' :
                                        data.time_window === '1h' ? '1 hour' :
                                        data.time_window === '2h' ? '2 hours' :
                                        data.time_window === '4h' ? '4 hours' :
                                        data.time_window === '24h' ? '24 hours' : data.time_window;

                content = `
                    <div style="min-width: 220px; max-height: 350px; overflow-y: auto;">
                        <strong>${data.name}</strong><br>
                        <small style="color: #888;">${data.region_id}</small>
                        ${data.is_historical ? '<br><small style="color: #e67e22;">⏱ Historical view</small>' : ''}
                        <hr style="margin: 8px 0; border: none; border-top: 1px solid #ddd;">
                        <table style="width: 100%; font-size: 13px;">
                            <tr>
                                <td style="color: #666;">${timeWindowLabel} Average</td>
                                <td style="text-align: right; font-size: 16px;"><strong>${d.avg.toFixed(1)}${data.unit}</strong></td>
                            </tr>
                            <tr>
                                <td style="color: #666;">${timeWindowLabel} Min / Max</td>
                                <td style="text-align: right;">${d.min.toFixed(1)} / ${d.max.toFixed(1)}${data.unit}</td>
                            </tr>
                            <tr>
                                <td style="color: #666;">Sensors</td>
                                <td style="text-align: right;">${d.sensor_count}</td>
                            </tr>
                        </table>
                        ${deviceListHtml}
                        <hr style="margin: 8px 0; border: none; border-top: 1px solid #ddd;">
                        <small style="color: #666;">
                            ADM Level: ${data.admin_level}<br>
                            ${data.is_historical ? 'Historical query' : `Last updated: ${data.precomputed_at ? (() => { const d = new Date(data.precomputed_at); return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0'); })() : 'unknown'}`}
                        </small>
                    </div>
                `;
            }

            L.popup()
                .setLatLng(latlng)
                .setContent(content)
                .openOn(this.map);

            // Attach click handler for lazy-load sensors link
            setTimeout(() => {
                const loadLink = document.getElementById('loadRegionDevices');
                if (loadLink) {
                    loadLink.addEventListener('click', async (e) => {
                        e.preventDefault();
                        const container = document.getElementById('regionDeviceList');
                        const regionId = loadLink.dataset.region;
                        const metricParam = loadLink.dataset.metric;
                        const deploymentParam = loadLink.dataset.deployment;
                        const rangeParam = loadLink.dataset.range;
                        const unitParam = loadLink.dataset.unit;

                        container.innerHTML = '<span style="color: #888;">Loading sensors...</span>';

                        try {
                            const resp = await fetch(`/api/regions/devices?region=${regionId}&metric=${metricParam}&deployment=${deploymentParam}&range=${rangeParam}`);
                            const devData = await resp.json();

                            if (devData.devices && devData.devices.length > 0) {
                                const rows = devData.devices.filter(dev => dev && dev.device_id).map(dev => {
                                    const deviceId = dev.device_id || 'unknown';
                                    const name = dev.node_name || deviceId;
                                    const shortId = deviceId.length > 12 ? deviceId.slice(-8) : deviceId;
                                    const displayName = name !== deviceId ? name : shortId;
                                    const hwInfo = `${dev.board_model || 'Unknown'} / ${dev.sensor_model || 'Unknown'}`;
                                    return `<tr>
                                        <td style="padding: 2px 0; font-size: 12px;" title="${deviceId}">${displayName}<br><span style="color: #999; font-size: 10px;">${hwInfo}</span></td>
                                        <td style="padding: 2px 0; text-align: right; font-size: 12px; vertical-align: top;">${dev.avg_value.toFixed(1)}${unitParam}</td>
                                    </tr>`;
                                }).join('');

                                container.innerHTML = `
                                    <div style="color: #666; margin-bottom: 4px;">Contributing Sensors:</div>
                                    <table style="width: 100%;">${rows}</table>
                                `;
                            } else {
                                container.innerHTML = '<span style="color: #888;">No sensors found</span>';
                            }
                        } catch (err) {
                            container.innerHTML = `<span style="color: #c00;">Failed to load sensors</span>`;
                        }
                    });
                }
            }, 0);

        } catch (error) {
            console.error('Error fetching region info:', error);
            L.popup()
                .setLatLng(latlng)
                .setContent(`<div style="color: red;">Error loading region info<br><small>${error.message}</small></div>`)
                .openOn(this.map);
        }
    }

    capitalizeFirst(str) {
        return str.charAt(0).toUpperCase() + str.slice(1).replace(/_/g, ' ');
    }

    disableRegionView() {
        // Remove region layer
        if (this.regionLayer) {
            this.map.removeLayer(this.regionLayer);
            this.regionLayer = null;
        }

        // Keep the legend visible (it's now shown in both views)

        // Remove map event listeners
        if (this._onZoomEnd) {
            this.map.off('zoomend', this._onZoomEnd);
        }
        if (this._onMapClick) {
            this.map.off('click', this._onMapClick);
        }

        // Show marker clusters again
        if (this.markerCluster) {
            this.map.addLayer(this.markerCluster);
        }
    }

    createPMTilesLayer() {
        // Check required libraries
        if (typeof protomapsL === 'undefined') {
            console.error('protomaps-leaflet not loaded! Check script tags in index.html');
            return;
        }
        if (typeof pmtiles === 'undefined') {
            console.warn('pmtiles library not loaded separately - protomaps-leaflet bundles it');
        }

        console.log('Creating PMTiles layer...');

        const self = this;

        try {
            // Dynamic fill function that colors regions based on sensor data
            const dynamicFill = (zoom, feature) => {
                return self.getRegionColor(feature);
            };

            // Multi-layer config with dynamic colors
            const layerConfig = {
                url: '/regions.pmtiles',
                pane: 'regionPane',
                maxNativeZoom: 10,  // PMTiles has tiles up to zoom 10
                maxZoom: 19,        // Allow overzooming up to zoom 19
                paint_rules: [
                    // ADM0 (countries) at zoom 0-1
                    {
                        dataLayer: 'adm0',
                        symbolizer: new protomapsL.PolygonSymbolizer({
                            fill: dynamicFill,
                            stroke: '#333',
                            width: 2
                        }),
                        maxzoom: 1
                    },
                    // ADM1 (states/regions) at zoom 2-4
                    {
                        dataLayer: 'adm1',
                        symbolizer: new protomapsL.PolygonSymbolizer({
                            fill: dynamicFill,
                            stroke: '#444',
                            width: 1.5
                        }),
                        minzoom: 2,
                        maxzoom: 4
                    },
                    // ADM2 (districts) at zoom 5-7
                    {
                        dataLayer: 'adm2',
                        symbolizer: new protomapsL.PolygonSymbolizer({
                            fill: dynamicFill,
                            stroke: '#555',
                            width: 1
                        }),
                        minzoom: 5,
                        maxzoom: 7
                    },
                    // ADM3 (sub-districts) at zoom 8+ (will overzoom beyond tile maxzoom)
                    {
                        dataLayer: 'adm3',
                        symbolizer: new protomapsL.PolygonSymbolizer({
                            fill: dynamicFill,
                            stroke: '#666',
                            width: 0.8
                        }),
                        minzoom: 8
                    }
                ],
                label_rules: []
            };

            this.regionLayer = protomapsL.leafletLayer(layerConfig);
            this.regionLayer.addTo(this.map);
            console.log('PMTiles region layer added to map');

        } catch (err) {
            console.error('Error creating PMTiles layer:', err);
            console.error('  Stack:', err.stack);
        }
    }

    async refreshRegionData() {
        // Fetch pre-computed region data from server
        // Server pre-computes all regions in background, so we just need zoom (for ADM level), metric, deployment filter, and time params
        const zoom = this.map.getZoom();

        // Build request key - zoom, metric, deployment filter, and time settings
        // Map zoom to admin level for cache key (matches server logic)
        // Zoom 0-1: ADM0, Zoom 2-4: ADM1, Zoom 5-7: ADM2, Zoom 8+: ADM3
        const adminLevel = zoom <= 1 ? 0 : zoom <= 4 ? 1 : zoom <= 7 ? 2 : 3;
        const deploymentTypes = Array.from(this.regionDeploymentTypes).sort().join(',');

        // Include time parameters in request key
        let timeKey = this.regionTimeWindow;
        if (this.regionHistoricalMode && this.regionHistoricalDate) {
            timeKey = `hist:${this.getRegionTimestamp()}`;
        }
        const requestKey = `${adminLevel}:${this.regionMetric}:${deploymentTypes}:${timeKey}`;

        // Skip if already fetching this exact request
        if (this._pendingRegionRequest === requestKey) {
            return;
        }

        // Skip if we just fetched this exact data
        if (this._lastRegionRequest === requestKey) {
            return;
        }

        this._pendingRegionRequest = requestKey;

        try {
            // Build URL with time parameters
            let url = `/api/regions/data?zoom=${zoom}&metric=${this.regionMetric}&deployment=${deploymentTypes}`;

            if (this.regionHistoricalMode && this.regionHistoricalDate) {
                // Historical mode: specific timestamp
                const timestamp = this.getRegionTimestamp();
                url += `&timestamp=${encodeURIComponent(timestamp)}`;
            } else {
                // Live mode: use selected window
                url += `&range=${this.regionTimeWindow}`;
            }

            const response = await fetch(url);

            if (!response.ok) {
                throw new Error(`API error: ${response.status}`);
            }

            const data = await response.json();
            this.regionalData = data.regions || {};
            this.regionUnit = data.unit || '';

            console.log(`Loaded ${Object.keys(this.regionalData).length} regions for ${this.regionMetric} (ADM${data.admin_level})`);

            // Force layer redraw using rerenderTiles()
            // protomaps-leaflet caches rendered Canvas tiles, so we need to force a re-render
            if (this.regionLayer) {
                // Debug: log available methods once
                if (!this._loggedLayerMethods) {
                    console.log('RegionLayer methods:', Object.getOwnPropertyNames(Object.getPrototypeOf(this.regionLayer)));
                    this._loggedLayerMethods = true;
                }

                // Clear debug caches so we log features on next render
                this._debuggedFeatures = new Set();
                this._matchedRegions = new Set();
                this._fillDebugCount = 0;
                this._colorDebugCount = 0;

                // Use rerenderTiles() to redraw all visible tiles with new data
                if (this.regionLayer.rerenderTiles) {
                    console.log('Calling rerenderTiles() to update colors...');
                    this.regionLayer.rerenderTiles();
                } else {
                    // Fallback: remove and re-add layer to force re-render
                    console.log('rerenderTiles not available, removing and re-adding layer...');
                    this.map.removeLayer(this.regionLayer);
                    this.regionLayer.addTo(this.map);
                }
            }

            // Update legend
            this.updateRegionLegend();

            // Record successful request to avoid redundant fetches
            this._lastRegionRequest = requestKey;

        } catch (error) {
            console.error('Error loading region data:', error);
        } finally {
            this._pendingRegionRequest = null;
        }
    }

    getRegionColor(feature) {
        // Match PMTiles feature to server data using region_id
        // Both PMTiles and server now use the same format: {ISO3}_ADM{level}_{original_id}
        // e.g., NZL_ADM2_65097584

        const props = feature?.props || {};
        const regionId = props.region_id || '';
        const layerName = feature?.layerName || 'unknown';

        // Debug: Log data availability
        if (!this._colorDebugCount) this._colorDebugCount = 0;
        if (this._colorDebugCount < 3) {
            console.log('getRegionColor state:', {
                hasRegionalData: !!this.regionalData,
                regionDataKeys: Object.keys(this.regionalData || {}).length,
                regionId,
                match: !!(this.regionalData && this.regionalData[regionId])
            });
            this._colorDebugCount++;
        }

        // Debug: log first few features to understand what we're getting
        if (!this._debuggedFeatures) {
            this._debuggedFeatures = new Set();
        }
        if (this._debuggedFeatures.size < 5 && regionId && !this._debuggedFeatures.has(regionId)) {
            this._debuggedFeatures.add(regionId);
            console.log(`Feature from layer ${layerName}: region_id=${regionId}, props:`, props);
            console.log(`  Available data keys (first 5):`, Object.keys(this.regionalData || {}).slice(0, 5));
            console.log(`  Match found:`, !!(this.regionalData && this.regionalData[regionId]));
        }

        // No data loaded yet - show gray
        if (!this.regionalData || Object.keys(this.regionalData).length === 0) {
            return 'rgba(180, 180, 180, 0.4)';
        }

        // Direct region_id match (both PMTiles and server use same format)
        if (regionId && this.regionalData[regionId]) {
            const value = this.regionalData[regionId].avg;
            const color = this.getColorForRegionMetric(this.regionMetric, value);
            // Log matches (limited to avoid spam)
            if (!this._matchedRegions) this._matchedRegions = new Set();
            if (!this._matchedRegions.has(regionId)) {
                this._matchedRegions.add(regionId);
                console.log(`[MATCH] ${regionId} = ${value}${this.regionUnit} → ${color}`);
            }
            return color;
        }

        // No match - show gray (gap in coverage = no sensor data for this region)
        return 'rgba(180, 180, 180, 0.4)';
    }

    _debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    findRegionData(regionId, countryCode, regionName) {
        // The PMTiles uses ISO3 codes (e.g., "NZL"), server data uses ISO2-subdivision (e.g., "NZ-AUK")
        // Try multiple matching strategies

        // Strategy 1: Direct region_id match
        if (this.regionalData[regionId]) {
            return this.regionalData[regionId];
        }

        // Strategy 2: Try ISO2-subdivision format (e.g., "NZ-AUK")
        // Convert ISO3 to ISO2 for common countries
        const iso3ToIso2 = {
            'NZL': 'NZ', 'AUS': 'AU', 'USA': 'US', 'GBR': 'GB', 'CAN': 'CA',
            'DEU': 'DE', 'FRA': 'FR', 'ITA': 'IT', 'ESP': 'ES', 'JPN': 'JP',
            'CHN': 'CN', 'IND': 'IN', 'BRA': 'BR', 'MEX': 'MX', 'ARG': 'AR',
            'ZAF': 'ZA', 'NGA': 'NG', 'EGY': 'EG', 'KEN': 'KE', 'GHA': 'GH',
            'AFG': 'AF', 'PAK': 'PK', 'BGD': 'BD', 'IDN': 'ID', 'MYS': 'MY',
            'SGP': 'SG', 'THA': 'TH', 'VNM': 'VN', 'PHL': 'PH', 'KOR': 'KR'
        };

        const iso2 = iso3ToIso2[countryCode] || countryCode?.substring(0, 2);

        // Try various key formats
        for (const [key, data] of Object.entries(this.regionalData)) {
            // Match by country code and name similarity
            if (key.startsWith(iso2 + '-')) {
                // Check if subdivision name matches
                const subdivisionName = key.split('-')[1];
                if (regionName && (
                    regionName.toLowerCase().includes(subdivisionName.toLowerCase()) ||
                    subdivisionName.toLowerCase().includes(regionName.toLowerCase().substring(0, 3))
                )) {
                    return data;
                }
            }

            // Match by region name
            if (data.subdivision && regionName &&
                regionName.toLowerCase() === data.subdivision.toLowerCase()) {
                return data;
            }
        }

        return null;
    }

    updateRegionColors() {
        if (!this.regionLayer) return;

        // Use rerenderTiles() to redraw with new data
        if (this.regionLayer.rerenderTiles) {
            this.regionLayer.rerenderTiles();
        }

        // Update legend
        this.updateRegionLegend();
    }

    onRegionClick(e) {
        if (!this.regionViewActive) return;

        // Note: protomaps-leaflet doesn't have built-in feature querying
        // For now, we'll show a general tooltip. In a full implementation,
        // you'd use vector-tile feature querying or a separate GeoJSON layer for interactions
    }

    setupRegionTooltip() {
        // Add a legend showing the color scale for the current metric
        if (!this.regionLegend) {
            this.regionLegend = L.control({ position: 'bottomleft' });
            this.regionLegend.onAdd = () => {
                const div = L.DomUtil.create('div', 'region-legend');
                div.style.cssText = `
                    padding: 8px 10px;
                    background: var(--bg-secondary, white);
                    border-radius: 6px;
                    box-shadow: 0 2px 6px rgba(0,0,0,0.2);
                    font-size: 12px;
                `;
                return div;
            };
            this.regionLegend.addTo(this.map);
        }

        // Update the legend content
        this.updateRegionLegend();
    }

    updateRegionLegend() {
        const legendDiv = document.querySelector('.region-legend');
        if (!legendDiv) return;

        const metricLabels = {
            temperature: 'Temperature',
            humidity: 'Humidity',
            pressure: 'Pressure',
            co2: 'CO₂',
            pm2_5: 'PM2.5',
            pm10: 'PM10',
            voc_index: 'VOC Index',
            nox_index: 'NOx Index'
        };

        const label = metricLabels[this.regionMetric] || this.regionMetric;
        const unit = this.regionUnit || '';

        // Get color scale for current metric
        const colorStops = this.getColorScaleForMetric(this.regionMetric);

        // For temperature with many stops, show vertical BOM-style legend
        if (this.regionMetric === 'temperature' && colorStops.length > 10) {
            // Build rows from hot (top) to cold (bottom), showing all temperature values
            let rowsHtml = '';
            for (let i = colorStops.length - 2; i >= 0; i--) {
                const color = colorStops[i].color;
                const tempValue = colorStops[i].value;

                rowsHtml += `
                    <div style="display: flex; align-items: center; height: 11px;">
                        <div style="width: 20px; height: 11px; background: ${color}; flex-shrink: 0;"></div>
                        <span style="font-size: 10px; margin-left: 6px; white-space: nowrap;">${tempValue}°</span>
                    </div>
                `;
            }

            legendDiv.innerHTML = `
                <div style="font-weight: 600; font-size: 12px;">Temperature</div>
                <div style="font-size: 10px; color: #666; margin-bottom: 8px; cursor: help;" title="Bureau of Meteorology 2013 Extended Color Scale">(BOM 2013)</div>
                <div style="display: flex; flex-direction: column;">
                    ${rowsHtml}
                </div>
            `;
        } else {
            // Standard horizontal gradient legend for other metrics
            let gradientsHtml = colorStops.map((stop, i) => {
                const nextStop = colorStops[i + 1];
                if (!nextStop) return '';
                return `<div style="flex: 1; background: linear-gradient(to right, ${stop.color}, ${nextStop.color});"></div>`;
            }).join('');

            let labelsHtml = colorStops.map(stop =>
                `<span style="font-size: 10px;">${stop.value}${unit}</span>`
            ).join('');

            legendDiv.innerHTML = `
                <div style="font-weight: 600; margin-bottom: 6px;">${label}</div>
                <div style="display: flex; height: 12px; border-radius: 3px; overflow: hidden; margin-bottom: 4px;">
                    ${gradientsHtml}
                </div>
                <div style="display: flex; justify-content: space-between;">
                    ${labelsHtml}
                </div>
            `;
        }
    }

    getColorScaleForMetric(metric) {
        // Return color stops for the legend
        switch (metric) {
            case 'temperature':
                // Comprehensive temperature scale from -90°C to +60°C
                // Based on meteorological standards with BOM 2013 extension colors
                return [
                    { value: '-80', color: '#F2F2F2' },  // Terminal Cold - Ghost White
                    { value: '-70', color: '#9FAEB5' },  // Abyssal Cold - Steel Grey
                    { value: '-60', color: '#6A5ACD' },  // Deep Polar - Desaturated Violet
                    { value: '-50', color: '#4B0082' },  // Severe Polar - Deep Indigo
                    { value: '-40', color: '#00008B' },  // Extreme Cold - Dark Blue-Violet
                    { value: '-30', color: '#0047AB' },  // Very Cold - Medium Blue
                    { value: '-20', color: '#1E90FF' },  // Cold - Cobalt Blue
                    { value: '-10', color: '#00BFFF' },  // Deep Freeze - Cerulean
                    { value: '-5', color: '#87CEEB' },   // Freeze - Sky Blue
                    { value: '0', color: '#E0FFFF' },    // Frost - Pale Aqua
                    { value: '5', color: '#006400' },    // Thaw - Dark Green (0°C boundary)
                    { value: '10', color: '#228B22' },   // Cool - Forest Green
                    { value: '15', color: '#32CD32' },   // Mild - Kelly Green
                    { value: '20', color: '#ADFF2F' },   // Moderate - Chartreuse
                    { value: '25', color: '#FFFF00' },   // Warm - Yellow
                    { value: '30', color: '#FFD700' },   // Very Warm - Goldenrod
                    { value: '35', color: '#FFA500' },   // Hot - Orange
                    { value: '40', color: '#FF8C00' },   // Very Hot - Dark Orange
                    { value: '42', color: '#FF4500' },   // Extreme Heat I - Red-Orange
                    { value: '44', color: '#FF0000' },   // Extreme Heat II - Bright Red
                    { value: '46', color: '#8B0000' },   // Extreme Heat III - Dark Red
                    { value: '48', color: '#800000' },   // Critical Heat - Maroon
                    { value: '50', color: '#200000' },   // The Gap - Black/V. Dark Brown
                    { value: '52', color: '#993399' },   // BOM Extension I - Deep Purple
                    { value: '54', color: '#FF00FF' },   // BOM Extension II - Magenta
                    { value: '56', color: '#FF1493' },   // Hyper-Thermal - Deep Pink
                    { value: '58', color: '#FF69B4' },   // Projected - Hot Pink
                    { value: '60', color: '#FFC0CB' }    // Theoretical - Incandescent Pink/White
                ];
            case 'humidity':
                return [
                    { value: '<20', color: '#e74c3c' },
                    { value: '30', color: '#e67e22' },
                    { value: '50', color: '#2ecc71' },
                    { value: '70', color: '#3498db' },
                    { value: '>80', color: '#1e3a5f' }
                ];
            case 'pressure':
                return [
                    { value: '<990', color: '#e74c3c' },
                    { value: '1000', color: '#f39c12' },
                    { value: '1013', color: '#2ecc71' },
                    { value: '1025', color: '#3498db' },
                    { value: '>1035', color: '#1e3a5f' }
                ];
            case 'co2':
                return [
                    { value: '<400', color: '#2ecc71' },
                    { value: '600', color: '#f39c12' },
                    { value: '1000', color: '#e67e22' },
                    { value: '>1500', color: '#e74c3c' }
                ];
            case 'pm2_5':
            case 'pm10':
                return [
                    { value: '<12', color: '#2ecc71' },
                    { value: '35', color: '#f39c12' },
                    { value: '55', color: '#e67e22' },
                    { value: '>150', color: '#e74c3c' }
                ];
            default:
                return [
                    { value: 'Low', color: '#2ecc71' },
                    { value: 'Med', color: '#f39c12' },
                    { value: 'High', color: '#e74c3c' }
                ];
        }
    }

    hideRegionTooltip() {
        // Remove the legend control
        if (this.regionLegend) {
            this.map.removeControl(this.regionLegend);
            this.regionLegend = null;
        }
    }

    async loadRegionBoundaries() {
        try {
            const response = await fetch('/regions.geojson');
            if (!response.ok) {
                throw new Error(`Failed to load regions.geojson: ${response.status}`);
            }
            this.regionBoundaries = await response.json();
            console.log(`Loaded ${this.regionBoundaries.features.length} region boundaries`);
        } catch (error) {
            console.error('Error loading region boundaries:', error);
            this.regionBoundaries = null;
        }
    }

    createRegionLayer() {
        if (!this.regionBoundaries) {
            console.error('No region boundaries loaded');
            return;
        }

        // Build sensor data by polygon using point-in-polygon
        this.buildRegionMap();

        const self = this;

        this.regionLayer = L.geoJSON(this.regionBoundaries, {
            style: (feature) => {
                const featureIndex = self.regionBoundaries.features.indexOf(feature);
                const data = self.featureDataMap.get(featureIndex);

                if (data) {
                    const color = self.getColorForRegionMetric(self.regionMetric, data.avgValue);
                    return {
                        fillColor: color,
                        fillOpacity: 0.6,
                        color: color,
                        weight: 1.5,
                        opacity: 0.8
                    };
                }
                return {
                    fillColor: '#cccccc',
                    fillOpacity: 0.1,
                    color: '#999999',
                    weight: 0.5,
                    opacity: 0.3
                };
            },
            onEachFeature: (feature, layer) => {
                const featureIndex = self.regionBoundaries.features.indexOf(feature);
                const data = self.featureDataMap.get(featureIndex);
                if (data) {
                    layer.bindTooltip(self.createRegionTooltip(feature, data), {
                        sticky: true,
                        className: 'region-tooltip'
                    });
                }
            }
        }).addTo(this.map);

        console.log('Region layer added to map');
    }

    buildRegionMap() {
        // Map feature index to aggregated sensor data using point-in-polygon
        this.featureDataMap = new Map();

        const sensorsWithLocation = this.sensors.filter(s => s.latitude && s.longitude);
        console.log(`Matching ${sensorsWithLocation.length} sensors to ${this.regionBoundaries.features.length} regions...`);

        // For each feature, find sensors that fall within it
        this.regionBoundaries.features.forEach((feature, index) => {
            const sensorsInRegion = sensorsWithLocation.filter(sensor => {
                return this.pointInPolygon([sensor.longitude, sensor.latitude], feature.geometry);
            });

            if (sensorsInRegion.length > 0) {
                // Aggregate readings for sensors in this region
                const readings = sensorsInRegion
                    .map(s => s.readings?.[this.regionMetric]?.value)
                    .filter(v => v !== undefined && v !== null);

                if (readings.length > 0) {
                    this.featureDataMap.set(index, {
                        avgValue: readings.reduce((a, b) => a + b, 0) / readings.length,
                        minValue: Math.min(...readings),
                        maxValue: Math.max(...readings),
                        sensorCount: sensorsInRegion.length
                    });
                }
            }
        });

        console.log(`Found sensor data in ${this.featureDataMap.size} regions`);
    }

    pointInPolygon(point, geometry) {
        if (geometry.type === 'Polygon') {
            return this.pointInPolygonRing(point, geometry.coordinates[0]);
        } else if (geometry.type === 'MultiPolygon') {
            return geometry.coordinates.some(polygon =>
                this.pointInPolygonRing(point, polygon[0])
            );
        }
        return false;
    }

    pointInPolygonRing(point, ring) {
        const [x, y] = point;
        let inside = false;

        for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
            const [xi, yi] = ring[i];
            const [xj, yj] = ring[j];

            if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
                inside = !inside;
            }
        }

        return inside;
    }

    updateRegionLayer() {
        if (!this.regionViewActive || !this.regionLayer) return;

        // Rebuild sensor map with new metric
        this.buildRegionMap();

        // Update styles
        const self = this;
        this.regionLayer.eachLayer((layer) => {
            if (layer.feature) {
                const featureIndex = self.regionBoundaries.features.indexOf(layer.feature);
                const data = self.featureDataMap.get(featureIndex);

                if (data) {
                    const color = self.getColorForRegionMetric(self.regionMetric, data.avgValue);
                    layer.setStyle({
                        fillColor: color,
                        fillOpacity: 0.6,
                        color: color,
                        weight: 1.5,
                        opacity: 0.8
                    });

                    layer.unbindTooltip();
                    layer.bindTooltip(self.createRegionTooltip(layer.feature, data), {
                        sticky: true,
                        className: 'region-tooltip'
                    });
                }
            }
        });

        // Update the legend
        this.updateRegionLegend();
    }

    getColorForRegionMetric(metric, value) {
        // Color scales matching legend display
        // Legend shows upper bounds, so use <= to include boundary values
        switch (metric) {
            case 'temperature':
                // Comprehensive temperature scale from -90°C to +60°C
                // Based on meteorological standards with BOM 2013 extension colors
                if (value <= -80) return '#F2F2F2';  // <= -80: Terminal Cold - Ghost White
                if (value <= -70) return '#9FAEB5';  // -80 to -70: Abyssal Cold - Steel Grey
                if (value <= -60) return '#6A5ACD';  // -70 to -60: Deep Polar - Desaturated Violet
                if (value <= -50) return '#4B0082';  // -60 to -50: Severe Polar - Deep Indigo
                if (value <= -40) return '#00008B';  // -50 to -40: Extreme Cold - Dark Blue-Violet
                if (value <= -30) return '#0047AB';  // -40 to -30: Very Cold - Medium Blue
                if (value <= -20) return '#1E90FF';  // -30 to -20: Cold - Cobalt Blue
                if (value <= -10) return '#00BFFF';  // -20 to -10: Deep Freeze - Cerulean
                if (value <= -5) return '#87CEEB';   // -10 to -5: Freeze - Sky Blue
                if (value <= 0) return '#E0FFFF';    // -5 to 0: Frost - Pale Aqua
                if (value <= 5) return '#006400';    // 0 to 5: Thaw - Dark Teal/Green
                if (value <= 10) return '#228B22';   // 5 to 10: Cool - Forest Green
                if (value <= 15) return '#32CD32';   // 10 to 15: Mild - Kelly Green
                if (value <= 20) return '#ADFF2F';   // 15 to 20: Moderate - Chartreuse
                if (value <= 25) return '#FFFF00';   // 20 to 25: Warm - Yellow
                if (value <= 30) return '#FFD700';   // 25 to 30: Very Warm - Goldenrod
                if (value <= 35) return '#FFA500';   // 30 to 35: Hot - Orange
                if (value <= 40) return '#FF8C00';   // 35 to 40: Very Hot - Dark Orange
                if (value <= 42) return '#FF4500';   // 40 to 42: Extreme Heat I - Red-Orange
                if (value <= 44) return '#FF0000';   // 42 to 44: Extreme Heat II - Bright Red
                if (value <= 46) return '#8B0000';   // 44 to 46: Extreme Heat III - Dark Red
                if (value <= 48) return '#800000';   // 46 to 48: Critical Heat - Maroon
                if (value <= 50) return '#200000';   // 48 to 50: The Gap - Black/V. Dark Brown
                if (value <= 52) return '#993399';   // 50 to 52: BOM Extension I - Deep Purple
                if (value <= 54) return '#FF00FF';   // 52 to 54: BOM Extension II - Magenta
                if (value <= 56) return '#FF1493';   // 54 to 56: Hyper-Thermal - Deep Pink
                if (value <= 58) return '#FF69B4';   // 56 to 58: Projected - Hot Pink
                return '#FFC0CB';                    // > 58: Theoretical - Incandescent Pink/White

            case 'humidity':
                if (value <= 20) return '#e74c3c';  // Very dry - red
                if (value <= 30) return '#e67e22';  // Dry - orange
                if (value <= 60) return '#2ecc71';  // Comfortable - green
                if (value <= 70) return '#f39c12';  // Humid - yellow
                if (value <= 80) return '#3498db';  // Very humid - blue
                return '#1e3a5f';                    // Saturated - dark blue

            case 'pressure':
                const pressureHPa = value < 200 ? value * 10 : value;
                if (pressureHPa <= 980) return '#e74c3c';  // Very low (stormy)
                if (pressureHPa <= 1000) return '#f39c12'; // Low (rain likely)
                if (pressureHPa <= 1020) return '#2ecc71'; // Normal
                if (pressureHPa <= 1040) return '#5dade2'; // High (fair)
                return '#3498db';                           // Very high

            case 'co2':
                if (value <= 400) return '#2ecc71';  // Outdoor levels - green
                if (value <= 600) return '#58d68d';  // Excellent - light green
                if (value <= 1000) return '#f39c12'; // Acceptable - yellow
                if (value <= 1500) return '#e67e22'; // Poor - orange
                return '#e74c3c';                     // Bad - red

            case 'pm2_5':
                if (value <= 12) return '#2ecc71';  // Good - green
                if (value <= 35) return '#f39c12';  // Moderate - yellow
                if (value <= 55) return '#e67e22';  // Unhealthy for sensitive
                if (value <= 150) return '#e74c3c'; // Unhealthy - red
                return '#8e44ad';                    // Very unhealthy - purple

            default:
                return '#3D7A7A';
        }
    }

    createRegionTooltip(feature, regionData) {
        const subdivisionName = feature.properties?.name || 'Unknown';

        const metricLabels = {
            temperature: 'Temperature',
            humidity: 'Humidity',
            pressure: 'Pressure',
            co2: 'CO₂',
            pm2_5: 'PM2.5',
            pm10: 'PM10',
            voc_index: 'VOC Index',
            nox_index: 'NOx Index'
        };

        const metricUnits = {
            temperature: '°C',
            humidity: '%',
            pressure: 'hPa',
            co2: 'ppm',
            pm2_5: 'µg/m³',
            pm10: 'µg/m³'
        };

        const label = metricLabels[this.regionMetric] || this.regionMetric;
        const unit = metricUnits[this.regionMetric] || '';

        const avgValue = regionData.avgValue?.toFixed(1) || 'N/A';
        const minValue = regionData.minValue?.toFixed(1) || 'N/A';
        const maxValue = regionData.maxValue?.toFixed(1) || 'N/A';
        const sensorCount = regionData.sensorCount || 0;

        return `
            <div class="region-tooltip-content">
                <strong>${subdivisionName}</strong>
                <hr style="margin: 4px 0; border-color: rgba(255,255,255,0.3);">
                <div style="font-size: 12px;">
                    <strong>${label}:</strong> ${avgValue}${unit}<br>
                    <span style="opacity: 0.8;">Min: ${minValue}${unit} | Max: ${maxValue}${unit}</span><br>
                    <span style="opacity: 0.7;">${sensorCount} sensor${sensorCount !== 1 ? 's' : ''}</span>
                </div>
            </div>
        `;
    }

    applyGeocoderDarkMode() {
        const isDarkMode = document.body.classList.contains('dark-mode');

        // Find all geocoder elements with multiple selector variations
        const geocoderControl = document.querySelector('.leaflet-control-geocoder');
        const geocoderForm = document.querySelector('.leaflet-control-geocoder-form');
        const geocoderExpanded = document.querySelector('.leaflet-control-geocoder.leaflet-control-geocoder-expanded');

        // Try to find all inputs within the geocoder - be very thorough
        const allInputs = document.querySelectorAll(
            '.leaflet-control-geocoder input, ' +
            '.leaflet-control-geocoder-form input, ' +
            '.leaflet-control-geocoder input[type="text"]'
        );

        const geocoderButton = document.querySelector('.leaflet-control-geocoder-icon');

        if (isDarkMode) {
            // Dark mode styling - apply to all elements
            [geocoderControl, geocoderForm, geocoderExpanded].forEach(el => {
                if (el) {
                    el.style.setProperty('background-color', '#2d2d2d', 'important');
                    el.style.setProperty('background', '#2d2d2d', 'important');
                    el.style.setProperty('border-color', '#444', 'important');
                }
            });

            // Style ALL inputs found - be very aggressive
            allInputs.forEach(input => {
                input.style.setProperty('background-color', '#2d2d2d', 'important');
                input.style.setProperty('background', '#2d2d2d', 'important');
                input.style.setProperty('color', '#e0e0e0', 'important');
                input.style.setProperty('border-color', '#444', 'important');
                input.style.setProperty('border', '1px solid #444', 'important');
            });

            if (geocoderButton) {
                geocoderButton.style.setProperty('filter', 'invert(1)', 'important');
            }
        } else {
            // Light mode - remove inline styles to use default
            [geocoderControl, geocoderForm, geocoderExpanded].forEach(el => {
                if (el) {
                    el.style.removeProperty('background-color');
                    el.style.removeProperty('background');
                    el.style.removeProperty('border-color');
                }
            });

            allInputs.forEach(input => {
                input.style.removeProperty('background-color');
                input.style.removeProperty('background');
                input.style.removeProperty('color');
                input.style.removeProperty('border-color');
                input.style.removeProperty('border');
            });

            if (geocoderButton) {
                geocoderButton.style.removeProperty('filter');
            }
        }
    }

    setupRefreshButton() {
        const refreshBtn = document.getElementById('refreshButton');
        if (!refreshBtn) return;

        refreshBtn.addEventListener('click', async () => {
            // Visual feedback
            refreshBtn.style.opacity = '0.5';
            refreshBtn.disabled = true;

            try {
                // Load sensors
                await this.loadSensors();

                // Also refresh region layer if active
                if (this.regionViewActive) {
                    await this.loadRegionalData();
                    this.updateRegionLayer();
                }
            } finally {
                refreshBtn.style.opacity = '0.8';
                refreshBtn.disabled = false;
            }
        });
    }

    setupHelpModal() {
        const helpBtn = document.getElementById('helpButton');
        const modal = document.getElementById('helpModal');
        const closeBtn = document.getElementById('closeHelp');

        if (!helpBtn || !modal || !closeBtn) return;

        // Open modal
        helpBtn.addEventListener('click', () => {
            modal.classList.add('show');
        });

        // Close modal
        closeBtn.addEventListener('click', () => {
            modal.classList.remove('show');
        });

        // Close on background click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.classList.remove('show');
            }
        });

        // Close on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && modal.classList.contains('show')) {
                modal.classList.remove('show');
            }
        });
    }

    setupTimeframeSelector() {
        const buttons = document.querySelectorAll('.timeframe-btn');

        buttons.forEach(btn => {
            btn.addEventListener('click', async (e) => {
                this.currentTimeRange = e.target.dataset.range;

                // Update active state
                buttons.forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');

                // Update the section header to show current selection
                const timeFilterValue = document.getElementById('timeFilterValue');
                if (timeFilterValue) {
                    timeFilterValue.textContent = `: ${e.target.textContent}`;
                }

                // Reload sensors from API with new time range
                await this.loadSensors();

                // Update region layer if active
                if (this.regionViewActive) {
                    await this.loadRegionalData();
                    this.updateRegionLayer();
                }
            });
        });
    }

    setupFilters() {
        // Handle accordion clicks on filter labels
        document.querySelectorAll('.filter-label').forEach(label => {
            label.addEventListener('click', (e) => {
                const filterName = e.currentTarget.dataset.filter;
                const options = document.querySelector(`.filter-options[data-filter="${filterName}"]`);
                
                // Toggle collapsed state
                e.currentTarget.classList.toggle('collapsed');
                options.classList.toggle('collapsed');
            });
        });
        
        // Handle filter button clicks
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('filter-btn')) {
                const filterType = e.target.dataset.filterType;
                const filterValue = e.target.dataset.filterValue;
                
                // Update active state for this filter group
                const filterGroup = e.target.parentElement;
                filterGroup.querySelectorAll('.filter-btn').forEach(btn => {
                    btn.classList.remove('active');
                });
                e.target.classList.add('active');
                
                // Update active filters
                this.activeFilters[filterType] = filterValue;
                
                // Update summary text
                this.updateFilterSummary(filterType, filterValue);
                
                // Auto-collapse after selection
                const label = document.querySelector(`.filter-label[data-filter="${filterType}"]`);
                const options = filterGroup;
                label.classList.add('collapsed');
                options.classList.add('collapsed');

                // Update dependent filters based on what changed
                if (filterType === 'timeframe' || filterType === 'board' || filterType === 'location' || filterType === 'environment') {
                    // When timeframe, board, location, or environment changes, update source options
                    this.updateSourceFilters();
                }
                if (filterType === 'source' || filterType === 'timeframe' || filterType === 'location' || filterType === 'environment') {
                    // When source, timeframe, location, or environment changes, update board options
                    this.updateBoardFilters();
                }
                if (filterType === 'source' || filterType === 'timeframe' || filterType === 'board' || filterType === 'environment') {
                    // When source, timeframe, board, or environment changes, update location options
                    this.updateLocationFilters();
                }
                if (filterType === 'source' || filterType === 'timeframe' || filterType === 'board' || filterType === 'location') {
                    // When source, timeframe, board, or location changes, update environment options
                    this.updateEnvironmentFilters();
                }

                // Refresh display
                this.updateSidebar();
                this.updateMap();
            }
        });
    }

    setupCollapsibleSections() {
        // Load saved collapse states from localStorage
        const savedStates = JSON.parse(localStorage.getItem('sidebarSectionStates') || '{}');

        document.querySelectorAll('.section-header').forEach(header => {
            const sectionName = header.dataset.section;
            const content = header.nextElementSibling;

            // Apply saved state (default to expanded)
            if (savedStates[sectionName] === true) {
                header.classList.add('collapsed');
                content.classList.add('collapsed');
            }

            header.addEventListener('click', () => {
                header.classList.toggle('collapsed');
                content.classList.toggle('collapsed');

                // Save state to localStorage
                const states = JSON.parse(localStorage.getItem('sidebarSectionStates') || '{}');
                states[sectionName] = header.classList.contains('collapsed');
                localStorage.setItem('sidebarSectionStates', JSON.stringify(states));
            });
        });
    }

    updateFilterSummary(filterType, filterValue) {
        const summaryMap = {
            'source': 'sourceSummary',
            'board': 'boardSummary',
            'location': 'locationSummary',
            'environment': 'environmentSummary'
        };
        const summaryId = summaryMap[filterType];
        const summary = document.getElementById(summaryId);
        if (summary) {
            const displayValue = filterValue === 'all' ? 'All' : filterValue;
            summary.textContent = `: ${displayValue}`;
        }
    }
    
    updateSourceFilters() {
        // Collect unique data sources and count sensors for each from time-filtered sensors
        const sourceCounts = new Map();
        const filteredSensors = this.getFilteredSensorsExcluding(['source']);

        filteredSensors.forEach(sensor => {
            let dataSource = null;

            // Check sensor-level data_source first (from ClickHouse)
            if (sensor.data_source) {
                dataSource = sensor.data_source;
            }

            // Fallback: check reading-level data_source
            if (!dataSource) {
                const readings = sensor.readings || {};
                for (const reading of Object.values(readings)) {
                    if (reading.raw) {
                        if (reading.raw.data_source) {
                            dataSource = reading.raw.data_source;
                            break;
                        }
                        if (reading.raw.rawMqttPayload && reading.raw.rawMqttPayload.data_source) {
                            dataSource = reading.raw.rawMqttPayload.data_source;
                            break;
                        }
                    }
                }
            }

            // Fallback: detect by device ID pattern
            if (!dataSource) {
                if (sensor.deviceId.startsWith('!') || sensor.deviceId.startsWith('meshtastic_')) {
                    dataSource = 'MESHTASTIC';
                } else if (sensor.deviceId.includes('_')) {
                    dataSource = 'WESENSE';
                } else {
                    dataSource = 'UNKNOWN';
                }
            }
            sourceCounts.set(dataSource, (sourceCounts.get(dataSource) || 0) + 1);
        });

        const sources = Array.from(sourceCounts.keys());

        // Update source filter buttons
        const sourceFilters = document.getElementById('sourceFilters');

        // Remove all except 'All' button
        sourceFilters.querySelectorAll('.filter-btn:not([data-filter-value="all"])').forEach(btn => btn.remove());

        // Sort sources alphabetically by display name
        const sortedSources = sources.sort((a, b) => {
            const nameA = this.formatDataSource(a) || a;
            const nameB = this.formatDataSource(b) || b;
            return nameA.localeCompare(nameB);
        });

        // Add button for each unique source with count
        sortedSources.forEach(source => {
            const count = sourceCounts.get(source);
            const displayName = this.formatDataSource(source) || source;
            const btn = document.createElement('button');
            btn.className = 'filter-btn';
            btn.dataset.filterType = 'source';
            btn.dataset.filterValue = source;
            btn.textContent = `${displayName} (${count})`;
            sourceFilters.appendChild(btn);
        });
    }

    updateBoardFilters() {
        // Collect unique board types and count sensors for each from time-filtered sensors
        const boardCounts = new Map();
        const filteredSensors = this.getFilteredSensorsExcluding(['board']);

        filteredSensors.forEach(sensor => {
            const deviceType = this.getDeviceType(sensor);
            const board = deviceType.board || 'UNKNOWN';
            boardCounts.set(board, (boardCounts.get(board) || 0) + 1);
        });

        const boardTypes = Array.from(boardCounts.keys());

        // Update board filter buttons
        const boardFilters = document.getElementById('boardFilters');
        const allBtn = boardFilters.querySelector('[data-filter-value="all"]');

        // Remove all except 'All' button
        boardFilters.querySelectorAll('.filter-btn:not([data-filter-value="all"])').forEach(btn => btn.remove());

        // Sort board types - put UNKNOWN first, then alphabetically
        const sortedBoards = boardTypes.sort((a, b) => {
            if (a === 'UNKNOWN') return -1;
            if (b === 'UNKNOWN') return 1;
            return a.localeCompare(b);
        });

        // Add button for each unique board type with count
        sortedBoards.forEach(board => {
            const count = boardCounts.get(board);
            const btn = document.createElement('button');
            btn.className = 'filter-btn';
            btn.dataset.filterType = 'board';
            btn.dataset.filterValue = board;
            btn.textContent = `${board} (${count})`;
            boardFilters.appendChild(btn);
        });
    }
    
    updateLocationFilters() {
        // Collect unique locations and count sensors for each from time-filtered sensors
        const locationCounts = new Map();
        const filteredSensors = this.getFilteredSensorsExcluding(['location']);
        
        filteredSensors.forEach(sensor => {
            const location = this.getLocationString(sensor);
            locationCounts.set(location, (locationCounts.get(location) || 0) + 1);
        });
        
        const locations = Array.from(locationCounts.keys());
        
        // Update location filter buttons
        const locationFilters = document.getElementById('locationFilters');
        
        // Remove all except 'All' button
        locationFilters.querySelectorAll('.filter-btn:not([data-filter-value="all"])').forEach(btn => btn.remove());
        
        // Sort locations - put 'Unknown' and 'Pending geocoding' first, then alphabetically
        const sortedLocations = locations.sort((a, b) => {
            if (a === 'Unknown' || a === 'Pending geocoding') return -1;
            if (b === 'Unknown' || b === 'Pending geocoding') return 1;
            return a.localeCompare(b);
        });
        
        // Add button for each unique location with count
        sortedLocations.forEach(location => {
            const count = locationCounts.get(location);
            const btn = document.createElement('button');
            btn.className = 'filter-btn';
            btn.dataset.filterType = 'location';
            btn.dataset.filterValue = location;
            btn.textContent = `${location} (${count})`;
            locationFilters.appendChild(btn);
        });
    }

    updateEnvironmentFilters() {
        // Collect unique deployment types and count sensors for each from time-filtered sensors
        const envCounts = new Map();
        const filteredSensors = this.getFilteredSensorsExcluding(['environment']);

        filteredSensors.forEach(sensor => {
            const envType = this.getEnvironmentType(sensor);
            envCounts.set(envType, (envCounts.get(envType) || 0) + 1);
        });

        const envTypes = Array.from(envCounts.keys());

        // Update environment filter buttons
        const envFilters = document.getElementById('environmentFilters');

        // Remove all except 'All' button
        envFilters.querySelectorAll('.filter-btn:not([data-filter-value="all"])').forEach(btn => btn.remove());

        // Sort environment types - Outdoor first (default), then Indoor, Mixed, Portable
        const sortOrder = ['OUTDOOR', 'INDOOR', 'MIXED', 'PORTABLE'];
        const sortedEnvTypes = envTypes.sort((a, b) => {
            const aIdx = sortOrder.indexOf(a);
            const bIdx = sortOrder.indexOf(b);
            if (aIdx === -1 && bIdx === -1) return a.localeCompare(b);
            if (aIdx === -1) return 1;
            if (bIdx === -1) return -1;
            return aIdx - bIdx;
        });

        // Add button for each unique environment type with count
        sortedEnvTypes.forEach(envType => {
            const count = envCounts.get(envType);
            const btn = document.createElement('button');
            btn.className = 'filter-btn';
            btn.dataset.filterType = 'environment';
            btn.dataset.filterValue = envType;
            // Display friendly names
            const displayName = this.formatEnvironmentName(envType);
            btn.textContent = `${displayName} (${count})`;
            envFilters.appendChild(btn);
        });
    }

    getEnvironmentType(sensor) {
        // Get deployment_type from sensor data
        let envType = sensor.deployment_type;

        // Normalize the value
        if (envType) {
            envType = envType.toUpperCase().trim();
            // Handle DEPLOYMENT_UNKNOWN prefix from protobuf
            if (envType === 'DEPLOYMENT_UNKNOWN') {
                return 'UNKNOWN';
            }
            if (envType === '') {
                return 'UNKNOWN';
            }
            return envType;
        }

        // Fallback for empty/null/undefined deployment_type
        return 'UNKNOWN';
    }

    formatEnvironmentName(envType) {
        // Convert INDOOR -> Indoor, OUTDOOR -> Outdoor, etc.
        const names = {
            'INDOOR': 'Indoor',
            'OUTDOOR': 'Outdoor',
            'MIXED': 'Mixed',
            'PORTABLE': 'Portable',
            'MOBILE': 'Mobile',
            'DEVICE': 'Device',
            'UNKNOWN': 'Unknown'
        };
        return names[envType] || envType;
    }

    getLocationString(sensor) {
        const hasLocation = sensor.latitude && sensor.longitude;
        if (!hasLocation) return 'Unknown';

        // Build location string: "Subdivision, Country" or "Locality, City, Country"
        const parts = [];

        // Try new ClickHouse format first (subdivision, country)
        if (sensor.subdivision && sensor.subdivision !== 'unknown') {
            parts.push(this.formatLocationName(sensor.subdivision));
        }
        if (sensor.country && sensor.country !== 'unknown') {
            parts.push(sensor.country.toUpperCase());
        }

        // Fallback to old InfluxDB format (locality, city, country)
        if (parts.length === 0) {
            if (sensor.locality) parts.push(sensor.locality);
            if (sensor.city) parts.push(sensor.city);
            if (sensor.country) parts.push(sensor.country);
        }

        if (parts.length > 0) {
            return parts.join(', ');
        } else {
            return 'Pending geocoding';
        }
    }

    formatLocationName(name) {
        // Convert "subcarpathian-voivodeship" to "Subcarpathian Voivodeship"
        if (!name) return name;
        return name
            .split('-')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }
    
    getFilteredSensorsExcluding(excludeFilters = []) {
        // Same as getFilteredSensors but excludes specified filters (for updating filter lists)
        // Note: Time filtering is already done by the backend API (ClickHouse query)

        return this.sensors.filter(sensor => {
            // Source filter
            if (!excludeFilters.includes('source') && this.activeFilters.source !== 'all') {
                let dataSource = null;

                // Check sensor-level data_source first (from ClickHouse)
                if (sensor.data_source) {
                    dataSource = sensor.data_source;
                }

                // Fallback: check reading-level data_source
                if (!dataSource) {
                    const readings = sensor.readings || {};
                    for (const reading of Object.values(readings)) {
                        if (reading.raw) {
                            if (reading.raw.data_source) {
                                dataSource = reading.raw.data_source;
                                break;
                            }
                            if (reading.raw.rawMqttPayload && reading.raw.rawMqttPayload.data_source) {
                                dataSource = reading.raw.rawMqttPayload.data_source;
                                break;
                            }
                        }
                    }
                }

                // Fallback: detect by device ID pattern
                if (!dataSource) {
                    if (sensor.deviceId.startsWith('!') || sensor.deviceId.startsWith('meshtastic_')) {
                        dataSource = 'MESHTASTIC';
                    } else if (sensor.deviceId.includes('_')) {
                        dataSource = 'WESENSE';
                    } else {
                        dataSource = 'UNKNOWN';
                    }
                }
                if (dataSource !== this.activeFilters.source) {
                    return false;
                }
            }
            
            // Board filter
            if (!excludeFilters.includes('board') && this.activeFilters.board !== 'all') {
                const deviceType = this.getDeviceType(sensor);
                if (this.activeFilters.board === 'UNKNOWN') {
                    if (deviceType.board !== null && deviceType.board !== undefined) {
                        return false;
                    }
                } else {
                    if (deviceType.board !== this.activeFilters.board) {
                        return false;
                    }
                }
            }
            
            // Location filter
            if (!excludeFilters.includes('location') && this.activeFilters.location !== 'all') {
                const location = this.getLocationString(sensor);
                if (location !== this.activeFilters.location) {
                    return false;
                }
            }

            // Environment filter
            if (!excludeFilters.includes('environment') && this.activeFilters.environment !== 'all') {
                const envType = this.getEnvironmentType(sensor);
                if (envType !== this.activeFilters.environment) {
                    return false;
                }
            }

            // Metric filter - only show sensors that have data for the selected metric
            if (!excludeFilters.includes('metric') && this.activeFilters.metric !== 'all') {
                const readings = sensor.readings || {};
                const metricKey = this.activeFilters.metric;
                // Map filter values to actual reading keys
                const readingKey = metricKey === 'pm2_5' ? 'pm2_5' :
                                   metricKey === 'pm10' ? 'pm10' :
                                   metricKey === 'voc' ? 'voc_index' :
                                   metricKey === 'co2' ? 'co2' :
                                   metricKey;
                if (!readings[readingKey] || readings[readingKey].value == null) {
                    return false;
                }
            }

            // Map view deployment filter (multi-select)
            if (!excludeFilters.includes('deploymentTypes') && this.activeFilters.deploymentTypes.size > 0) {
                const envType = this.getEnvironmentType(sensor);
                if (!this.activeFilters.deploymentTypes.has(envType)) {
                    return false;
                }
            }

            return true;
        });
    }

    getFilteredSensors() {
        // Note: Time filtering is already done by the backend API (ClickHouse query)
        // The sensors array only contains sensors within the selected time range
        // We only apply the additional filters (source, board, location, environment) here

        return this.sensors.filter(sensor => {
            // Source filter
            if (this.activeFilters.source !== 'all') {
                let dataSource = null;

                // Check sensor-level data_source first (from ClickHouse)
                if (sensor.data_source) {
                    dataSource = sensor.data_source;
                }

                // Fallback: check reading-level data_source
                if (!dataSource) {
                    const readings = sensor.readings || {};
                    for (const reading of Object.values(readings)) {
                        if (reading.raw) {
                            if (reading.raw.data_source) {
                                dataSource = reading.raw.data_source;
                                break;
                            }
                            if (reading.raw.rawMqttPayload && reading.raw.rawMqttPayload.data_source) {
                                dataSource = reading.raw.rawMqttPayload.data_source;
                                break;
                            }
                        }
                    }
                }

                // Fallback: detect by device ID pattern
                if (!dataSource) {
                    if (sensor.deviceId.startsWith('!') || sensor.deviceId.startsWith('meshtastic_')) {
                        dataSource = 'MESHTASTIC';
                    } else if (sensor.deviceId.includes('_')) {
                        dataSource = 'WESENSE';
                    } else {
                        dataSource = 'UNKNOWN';
                    }
                }
                if (dataSource !== this.activeFilters.source) {
                    return false;
                }
            }
            
            // Board filter
            if (this.activeFilters.board !== 'all') {
                const deviceType = this.getDeviceType(sensor);
                if (this.activeFilters.board === 'UNKNOWN') {
                    // Filter for sensors without board info (null or undefined)
                    if (deviceType.board !== null && deviceType.board !== undefined) {
                        return false;
                    }
                } else {
                    // Filter for specific board type
                    if (deviceType.board !== this.activeFilters.board) {
                        return false;
                    }
                }
            }
            
            // Location filter
            if (this.activeFilters.location !== 'all') {
                const location = this.getLocationString(sensor);
                if (location !== this.activeFilters.location) {
                    return false;
                }
            }

            // Environment filter
            if (this.activeFilters.environment !== 'all') {
                const envType = this.getEnvironmentType(sensor);
                if (envType !== this.activeFilters.environment) {
                    return false;
                }
            }

            // Metric filter - only show sensors that have data for the selected metric
            if (this.activeFilters.metric !== 'all') {
                const readings = sensor.readings || {};
                const metricKey = this.activeFilters.metric;
                // Map filter values to actual reading keys
                const readingKey = metricKey === 'pm2_5' ? 'pm2_5' :
                                   metricKey === 'pm10' ? 'pm10' :
                                   metricKey === 'voc' ? 'voc_index' :
                                   metricKey === 'co2' ? 'co2' :
                                   metricKey;
                if (!readings[readingKey] || readings[readingKey].value == null) {
                    return false;
                }
            }

            // Map view deployment filter (multi-select)
            if (this.activeFilters.deploymentTypes.size > 0) {
                const envType = this.getEnvironmentType(sensor);
                if (!this.activeFilters.deploymentTypes.has(envType)) {
                    return false;
                }
            }

            return true;
        });
    }

    getTimeRangeMs(range) {
        const ranges = {
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000,
            '7d': 7 * 24 * 60 * 60 * 1000,
            '30d': 30 * 24 * 60 * 60 * 1000
        };
        return ranges[range] || ranges['30m'];
    }
    
    getLastSeenTime(sensor) {
        // Check lastUpdated first
        if (sensor.lastUpdated) {
            return sensor.lastUpdated;
        }
        // Fall back to latest reading timestamp
        const readings = sensor.readings || {};
        const timestamps = Object.values(readings)
            .map(r => r.timestamp)
            .filter(t => t)
            .sort((a, b) => new Date(b) - new Date(a));
        return timestamps[0] || null;
    }

    async loadHistoricalData() {
        try {
            document.getElementById('historyStatus').textContent = `Loading ${this.currentTimeRange} history...`;
            const response = await fetch(`/api/history/average?range=${this.currentTimeRange}`);
            const data = await response.json();
            this.historicalData = data.averageData || {};
            const timeframeDisplay = this.currentTimeRange === '30m' ? '30 minutes' :
                                    this.currentTimeRange === '1h' ? '1 hour' :
                                    this.currentTimeRange === '2h' ? '2 hours' :
                                    this.currentTimeRange === '4h' ? '4 hours' :
                                    this.currentTimeRange === '8h' ? '8 hours' :
                                    this.currentTimeRange === '24h' ? '24 hours' :
                                    this.currentTimeRange === '7d' ? '7 days' :
                                    this.currentTimeRange === '30d' ? '30 days' : this.currentTimeRange;
            document.getElementById('historyStatus').textContent = `Last heard within ${timeframeDisplay}`;
            
            // Refresh sidebar with new historical data
            this.updateSidebar();
            
            // If a sensor is selected, update its popup
            if (this.selectedSensorId) {
                const selectedSensor = this.sensors.find(s => s.deviceId === this.selectedSensorId);
                if (selectedSensor && this.markers.has(this.selectedSensorId)) {
                    const marker = this.markers.get(this.selectedSensorId);
                    marker.setPopupContent(this.createPopupContent(selectedSensor));
                    setTimeout(() => this.drawSparklines(selectedSensor), 100);
                }
            }
        } catch (error) {
            console.error('Error loading historical data:', error);
            document.getElementById('historyStatus').textContent = 'Real-time MQTT only';
        }
    }

    async detectVisitorLocation() {
        try {
            const response = await fetch('/api/location');
            if (!response.ok) return null;

            const data = await response.json();
            if (data && data.lat && data.lng) {
                console.log(`Detected location: ${data.city || 'Unknown'}, ${data.country || 'Unknown'}`);
                return [data.lat, data.lng];
            }
        } catch (error) {
            console.log('Geolocation detection failed, using default location');
        }
        return null;
    }

    async initMap() {
        // Try to detect visitor's location, fall back to configured default
        let mapCenter = await this.detectVisitorLocation();
        if (!mapCenter) {
            mapCenter = [
                parseFloat(this.getMapConfig('MAP_CENTER_LAT') || '-36.848'),
                parseFloat(this.getMapConfig('MAP_CENTER_LNG') || '174.763')
            ];
        }
        const zoomLevel = parseInt(this.getMapConfig('MAP_ZOOM_LEVEL') || '4');

        this.map = L.map('map', {
            maxBounds: [[-90, -180], [90, 180]],
            maxBoundsViscosity: 1.0
        }).setView(mapCenter, zoomLevel);

        // Create a custom pane for region overlay so it renders above base tiles
        // Default tilePane is z-index 200, we use 250 to be above base tiles but below markers
        this.map.createPane('regionPane');
        this.map.getPane('regionPane').style.zIndex = 250;

        // Define base layers for street and satellite views
        // Street layer (CartoDB Voyager - uses OSM data with local language labels)
        const streetLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
            maxZoom: 19,
            noWrap: true
        });

        const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: '&copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
            maxZoom: 19,
            noWrap: true
        });

        // Labels overlay for satellite view (roads, place names, boundaries)
        const labelsLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}', {
            attribution: '&copy; Esri',
            maxZoom: 19,
            noWrap: true
        });

        // Create a layer group for hybrid view (satellite + labels)
        const hybridLayer = L.layerGroup([satelliteLayer, labelsLayer]);

        // Layer name to layer object mapping
        const baseMaps = {
            "Street": streetLayer,
            "Satellite": satelliteLayer,
            "Hybrid": hybridLayer
        };

        // Load saved layer preference or default to Hybrid
        const savedLayer = localStorage.getItem('preferredMapLayer') || 'Hybrid';
        const defaultLayer = baseMaps[savedLayer] || hybridLayer;

        defaultLayer.addTo(this.map);
        this.tileLayer = defaultLayer;

        // Set initial layer class for dark mode filtering
        const mapElement = document.getElementById('map');
        if (savedLayer === 'Street') {
            mapElement.classList.add('street-layer');
        }

        // Create layer control for switching between views
        L.control.layers(baseMaps).addTo(this.map);

        // Add heatmap toggle control (below layer control)
        this.createHeatmapToggleControl();

        // Add temperature legend (visible in both heatmap and sensor views)
        this.setupRegionTooltip();

        // Add zoom level display control
        const ZoomDisplay = L.Control.extend({
            options: { position: 'topleft' },
            onAdd: function(map) {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control zoom-display');
                container.style.cssText = 'background: white; color: black; padding: 4px 8px; font-size: 12px; font-weight: bold; min-width: 24px; text-align: center;';
                container.innerHTML = map.getZoom();
                map.on('zoomend', () => {
                    container.innerHTML = map.getZoom();
                });
                return container;
            }
        });
        new ZoomDisplay().addTo(this.map);

        // Listen for layer changes and update CSS class for dark mode filtering
        this.map.on('baselayerchange', (e) => {
            // Save layer preference
            localStorage.setItem('preferredMapLayer', e.name);

            // Street layer needs CSS filtering for dark mode
            if (e.name === 'Street') {
                mapElement.classList.add('street-layer');
            } else {
                mapElement.classList.remove('street-layer');
            }

            // Regenerate map markers to update icon colors for current layer
            this.updateMap();
        });

        // Add geocoder (place search) control
        const geocoder = L.Control.geocoder({
            defaultMarkGeocode: false,
            placeholder: 'Search for a place...',
            errorMessage: 'No results found',
            collapsed: true,
            position: 'topleft'
        }).on('markgeocode', (e) => {
            const bbox = e.geocode.bbox;
            const center = e.geocode.center;

            // Zoom to the location
            if (bbox) {
                this.map.fitBounds([
                    [bbox.getSouth(), bbox.getWest()],
                    [bbox.getNorth(), bbox.getEast()]
                ]);
            } else {
                this.map.setView(center, 13);
            }
        }).addTo(this.map);

        // Apply dark mode styles to geocoder input after a short delay to override inline styles
        setTimeout(() => {
            this.applyGeocoderDarkMode();
        }, 500);

        // Setup MutationObserver to watch for geocoder input field creation
        const geocoderControl = document.querySelector('.leaflet-control-geocoder');
        if (geocoderControl) {
            // Watch for changes to the geocoder control (input field added dynamically)
            // Only watch for child elements being added, NOT style changes (to avoid infinite loop)
            const observer = new MutationObserver(() => {
                this.applyGeocoderDarkMode();
            });

            observer.observe(geocoderControl, {
                childList: true,
                subtree: true
            });

            // Also apply when clicked
            geocoderControl.addEventListener('click', () => {
                setTimeout(() => this.applyGeocoderDarkMode(), 10);
                setTimeout(() => this.applyGeocoderDarkMode(), 100);
            });

            // Store observer reference
            this.geocoderObserver = observer;
        }

        // Apply when user interacts with geocoder
        document.addEventListener('focusin', (e) => {
            if (e.target && e.target.closest('.leaflet-control-geocoder')) {
                this.applyGeocoderDarkMode();
            }
        });

        // Store geocoder reference for later styling
        this.geocoder = geocoder;

        // Create marker cluster group with zoom disabled on click
        // maxClusterRadius: smaller = less clustering, more individual markers visible
        // Use zoom-dependent radius: more clustering at low zoom, less at high zoom
        this.markerCluster = L.markerClusterGroup({
            zoomToBoundsOnClick: false,
            maxClusterRadius: (zoom) => {
                if (zoom <= 2) return 80;  // World view - cluster heavily
                if (zoom <= 4) return 50;  // Continental - moderate clustering
                if (zoom <= 6) return 25;  // Regional - light clustering
                return 1;                   // Local - effectively no clustering
            }
        });

        // Add custom click handler to spiderfy without zooming
        this.markerCluster.on('clusterclick', (e) => {
            e.layer.spiderfy();
        });

        this.map.addLayer(this.markerCluster);

        // Safari-specific workaround: force tile redraw after initialization
        // Safari has known issues with Leaflet tile rendering
        // See: https://github.com/Leaflet/Leaflet/issues/5685
        const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
        if (isSafari) {
            console.log('Safari detected - applying tile rendering workaround');
            // Multiple invalidateSize calls at different intervals
            setTimeout(() => {
                this.map.invalidateSize();
                if (this.tileLayer && this.tileLayer.redraw) {
                    this.tileLayer.redraw();
                }
            }, 100);
            setTimeout(() => {
                this.map.invalidateSize();
                // Force a tiny pan to trigger tile reload
                this.map.panBy([1, 0], { animate: false });
                this.map.panBy([-1, 0], { animate: false });
            }, 500);
            setTimeout(() => this.map.invalidateSize(), 1000);
        }
    }

    createHeatmapToggleControl() {
        const self = this;

        // SVG icons for toggle states
        const gridIcon = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>
        </svg>`;
        const mapIcon = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/>
        </svg>`;
        const loadingIcon = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="12" y1="2" x2="12" y2="6"/><line x1="12" y1="18" x2="12" y2="22"/><line x1="4.93" y1="4.93" x2="7.76" y2="7.76"/><line x1="16.24" y1="16.24" x2="19.07" y2="19.07"/><line x1="2" y1="12" x2="6" y2="12"/><line x1="18" y1="12" x2="22" y2="12"/><line x1="4.93" y1="19.07" x2="7.76" y2="16.24"/><line x1="16.24" y1="7.76" x2="19.07" y2="4.93"/>
        </svg>`;

        const HeatmapToggle = L.Control.extend({
            options: { position: 'topright' },
            onAdd: function(map) {
                const container = L.DomUtil.create('div', 'leaflet-bar leaflet-control heatmap-toggle-control');
                const button = L.DomUtil.create('button', '', container);
                button.innerHTML = gridIcon;
                button.title = 'Toggle region heatmap view';
                button.id = 'heatmapToggleBtn';

                // Prevent map clicks when clicking the control
                L.DomEvent.disableClickPropagation(container);

                button.addEventListener('click', async () => {
                    const mapOverlayControls = document.getElementById('mapOverlayControls');
                    const sensorOverlayControls = document.getElementById('sensorOverlayControls');

                    // Store current map position to restore after toggle
                    const currentCenter = self.map.getCenter();
                    const currentZoom = self.map.getZoom();

                    button.innerHTML = loadingIcon;
                    button.classList.add('loading');

                    self.regionViewActive = !self.regionViewActive;
                    button.classList.toggle('active', self.regionViewActive);

                    // Show region controls when region view is active, sensor controls when not
                    if (mapOverlayControls) {
                        mapOverlayControls.classList.toggle('active', self.regionViewActive);
                    }
                    if (sensorOverlayControls) {
                        sensorOverlayControls.classList.toggle('active', !self.regionViewActive);
                    }

                    try {
                        if (self.regionViewActive) {
                            await self.enableRegionView();
                            button.innerHTML = mapIcon; // Show map pin when heatmap active
                        } else {
                            self.disableRegionView();
                            button.innerHTML = gridIcon; // Show grid when showing sensors
                        }
                    } catch (error) {
                        console.error('Error toggling region view:', error);
                        button.innerHTML = gridIcon;
                    }

                    // Restore map position (layer changes can sometimes shift the view)
                    self.map.setView(currentCenter, currentZoom, { animate: false });

                    button.classList.remove('loading');
                });

                return container;
            }
        });

        this.heatmapToggleControl = new HeatmapToggle();
        this.heatmapToggleControl.addTo(this.map);
    }

    getMapConfig(key) {
        // In a real app, these would come from the server
        const defaults = {
            'MAP_CENTER_LAT': '-36.848',
            'MAP_CENTER_LNG': '174.763',
            'MAP_ZOOM_LEVEL': '10',
            'MAP_LANGUAGE': 'auto',  // 'auto', 'en', or 'local'
        };
        return defaults[key];
    }

    async loadSensors() {
        try {
            // Respect the selected time range - only show sensors with data in that period
            const response = await fetch(`/api/sensors?range=${this.currentTimeRange}`);
            const data = await response.json();
            this.sensors = data.sensors || [];
            this.swarmStats = data.swarm_stats || null;  // Store swarm statistics
            this.updateSourceFilters();
            this.updateBoardFilters();
            this.updateLocationFilters();
            this.updateEnvironmentFilters();
            this.populateDeploymentTypes();  // Populate map view deployment filter
            this.populateRegionDeploymentTypes();  // Populate tiles view deployment filter
            this.updateMap();
            this.updateSidebar();
            this.updateStats();

            // Update dashboard if it's currently visible
            if (document.getElementById('dashboardView').classList.contains('active')) {
                this.renderDashboard();
            }

            // Load historical data (updates the "Last heard within X" message)
            await this.loadHistoricalData();
        } catch (error) {
            console.error('Error loading sensors:', error);
        }
    }

    async loadAllSensorsForAging() {
        // Load all sensors (30d) for the aging distribution chart
        try {
            const response = await fetch('/api/sensors?range=30d');
            const data = await response.json();
            this.allSensorsForAging = data.sensors || [];
            this.updateStats();  // Re-render stats with full data
        } catch (error) {
            console.error('Error loading sensors for aging chart:', error);
        }
    }

    async loadLeaderboard() {
        // Load town leaderboards at ADM2 level from the API
        // Fixed to 24h for stability (leaderboard shouldn't fluctuate with short time ranges)
        try {
            const response = await fetch(`/api/leaderboard?limit=5`);
            const data = await response.json();
            this.leaderboardData = {
                byNodes: data.byNodes || [],
                bySensors: data.bySensors || [],
                byTypes: data.byTypes || []
            };
            this.totalRegionsCount = data.total_regions || 0;
            this.updateLeaderboard();
            // Update regions count in stats
            const regionsCountEl = document.getElementById('regionsCount');
            if (regionsCountEl) {
                regionsCountEl.textContent = this.totalRegionsCount;
            }
        } catch (error) {
            console.error('Error loading leaderboard:', error);
        }
    }

    async loadEnvLeaderboard() {
        // Load environmental leaderboards (30-day data)
        try {
            const response = await fetch('/api/env-leaderboard');
            const data = await response.json();
            this.envLeaderboardData = {
                outdoorAir: data.outdoorAir || [],
                indoorAir: data.indoorAir || [],
                bestWeather: data.bestWeather || [],
                mostStable: data.mostStable || [],
                hottest: data.hottest || []
            };
            this.updateEnvLeaderboard();
        } catch (error) {
            console.error('Error loading environmental leaderboard:', error);
        }
    }

    updateEnvLeaderboard() {
        const container = document.getElementById('envLeaderboardList');
        if (!container) return;

        const { outdoorAir, indoorAir, bestWeather, mostStable, hottest } = this.envLeaderboardData;

        // Check if we have any data
        if (outdoorAir.length === 0 && indoorAir.length === 0 && bestWeather.length === 0 &&
            mostStable.length === 0 && hottest.length === 0) {
            container.innerHTML = '<div class="leaderboard-empty">Insufficient data</div>';
            return;
        }

        // Helper to build air quality section with score + details
        const buildAirSection = (title, tooltip, regions, detailsFormatter) => {
            if (!regions || regions.length === 0) return '';

            let itemsHtml = '';
            regions.forEach((region, index) => {
                const formattedRegion = this.formatLocationName(region.name);
                const countryCode = region.country_code ? region.country_code.substring(0, 2) : '';
                const details = detailsFormatter(region);
                const scoreDisplay = region.score !== null ? region.score : '-';
                itemsHtml += `
                    <div class="leaderboard-item" data-lat="${region.lat}" data-lon="${region.lon}" style="cursor: pointer;" title="${details}">
                        <span class="leaderboard-rank">#${index + 1}</span>
                        <span class="leaderboard-region">${formattedRegion}<span style="opacity: 0.6; margin-left: 3px;">(${countryCode})</span></span>
                        <span class="leaderboard-count" style="font-weight: 600;">${scoreDisplay}</span>
                    </div>
                `;
            });

            return `
                <div class="leaderboard-section">
                    <div class="leaderboard-section-header" title="${tooltip}" style="cursor: help;">${title} <span style="opacity: 0.5; font-size: 10px;">ⓘ</span></div>
                    <div class="leaderboard-list">${itemsHtml}</div>
                </div>
            `;
        };

        // Helper for simple value sections
        const buildSection = (title, regions, valueFormatter) => {
            if (!regions || regions.length === 0) return '';

            let itemsHtml = '';
            regions.forEach((region, index) => {
                const formattedRegion = this.formatLocationName(region.name);
                const countryCode = region.country_code ? region.country_code.substring(0, 2) : '';
                const valueDisplay = valueFormatter(region);
                itemsHtml += `
                    <div class="leaderboard-item" data-lat="${region.lat}" data-lon="${region.lon}" style="cursor: pointer;">
                        <span class="leaderboard-rank">#${index + 1}</span>
                        <span class="leaderboard-region">${formattedRegion}<span style="opacity: 0.6; margin-left: 3px;">(${countryCode})</span></span>
                        <span class="leaderboard-count">${valueDisplay}</span>
                    </div>
                `;
            });

            return `
                <div class="leaderboard-section">
                    <div class="leaderboard-section-header">${title}</div>
                    <div class="leaderboard-list">${itemsHtml}</div>
                </div>
            `;
        };

        let html = '';

        // Outdoor Air Quality (PM2.5 | PM10 | NOx → Score)
        const outdoorTooltip = 'Score 0-100 (higher=cleaner). Based on WHO guidelines: PM2.5 <15µg/m³, PM10 <45µg/m³, NOx index <100. Hover items for details.';
        html += buildAirSection('Outdoor Air Quality', outdoorTooltip, outdoorAir, r => {
            const parts = [];
            if (r.pm25) parts.push(`PM2.5: ${r.pm25}µg/m³`);
            if (r.pm10) parts.push(`PM10: ${r.pm10}µg/m³`);
            if (r.nox) parts.push(`NOx: ${r.nox}`);
            return parts.join(' | ') + ` → Score: ${r.score}`;
        });

        // Indoor Air Quality (VOC | CO2 → Score)
        const indoorTooltip = 'Score 0-100 (higher=cleaner). VOC index <100 = good, CO2 <700ppm = good ventilation. Hover items for details.';
        html += buildAirSection('Indoor Air Quality', indoorTooltip, indoorAir, r => {
            const parts = [];
            if (r.voc) parts.push(`VOC: ${r.voc}`);
            if (r.co2) parts.push(`CO2: ${r.co2}ppm`);
            return parts.join(' | ') + ` → Score: ${r.score}`;
        });

        // Best Weather (show temp)
        html += buildSection('Best Weather', bestWeather, r => `${r.value}°C`);

        // Most Stable Climate (show variance)
        html += buildSection('Most Stable', mostStable, r => `±${r.variance}°C`);

        // Hottest Towns
        html += buildSection('Hottest', hottest, r => `${r.value}°C`);

        container.innerHTML = html || '<div class="leaderboard-empty">No data available</div>';

        // Add click handlers for navigation
        container.querySelectorAll('.leaderboard-item[data-lat]').forEach(item => {
            item.addEventListener('click', () => {
                const lat = parseFloat(item.dataset.lat);
                const lon = parseFloat(item.dataset.lon);
                if (!isNaN(lat) && !isNaN(lon)) {
                    this.map.setView([lat, lon], 10);
                }
            });
        });
    }

    updateMap() {
        const filteredSensors = this.getFilteredSensors();
        
        // Clear existing markers
        this.markerCluster.clearLayers();
        this.markers.clear();
        
        filteredSensors.forEach(sensor => {
            if (!sensor.latitude || !sensor.longitude) return;

            const key = sensor.deviceId;
            
            if (this.markers.has(key)) {
                // Update existing marker
                const marker = this.markers.get(key);
                marker.setLatLng([sensor.latitude, sensor.longitude]);
            } else {
                // Create new marker with custom SVG icon
                const icon = this.createSensorIcon(sensor);
                const marker = L.marker([sensor.latitude, sensor.longitude], { icon });

                // Add hover tooltip explaining the icon
                const tooltipContent = `
                    <div style="font-size: 11px; line-height: 1.4;">
                        <strong>${sensor.deviceId || 'Sensor'}</strong><br>
                        <span style="font-size: 10px; opacity: 0.8;">
                            Top Left: Temp | Top Right: Humidity<br>
                            Bottom Left: Pressure | Bottom Right: CO₂
                        </span>
                    </div>
                `;
                marker.bindTooltip(tooltipContent, {
                    direction: 'top',
                    offset: [0, -20],
                    opacity: 0.95
                });

                const popup = L.popup({ maxWidth: 320, maxHeight: 400 }).setContent(this.createPopupContent(sensor));
                marker.bindPopup(popup);
                marker.on('click', () => {
                    this.selectSensor(sensor.deviceId);
                    // Draw sparklines when popup opens
                    setTimeout(() => this.drawSparklines(sensor), 100);
                });
                this.markerCluster.addLayer(marker);
                this.markers.set(key, marker);
            }
        });
    }

    updateSidebar() {
        const container = document.getElementById('sensorsList');

        if (this.sensors.length === 0) {
            container.innerHTML = '<div class="empty"><div class="empty-icon">X</div><p>No sensors detected</p></div>';
            return;
        }

        const filteredSensors = this.getFilteredSensors();
        
        container.innerHTML = filteredSensors
            .sort((a, b) => {
                // Sort by most recent update first - check both lastUpdated and reading timestamps
                const getLatestTime = (sensor) => {
                    if (sensor.lastUpdated) {
                        return new Date(sensor.lastUpdated).getTime();
                    }
                    // Fall back to latest reading timestamp
                    const readings = sensor.readings || {};
                    const timestamps = Object.values(readings)
                        .map(r => r.timestamp)
                        .filter(t => t)
                        .map(t => new Date(t).getTime());
                    return timestamps.length > 0 ? Math.max(...timestamps) : 0;
                };
                return getLatestTime(b) - getLatestTime(a);
            })
            .map(sensor => this.createSensorItem(sensor))
            .join('');

        // Add click handlers
        document.querySelectorAll('.sensor-item').forEach(item => {
            item.addEventListener('click', (e) => {
                const deviceId = e.currentTarget.dataset.deviceId;
                this.selectSensor(deviceId, true); // Enable zoom when clicking from sidebar
            });
        });
    }

    getDisplayReadings(sensor) {
        // If viewing historical data, use averages from InfluxDB
        if (this.currentTimeRange !== '24h' || Object.keys(this.historicalData).length > 0) {
            const historyForSensor = this.historicalData[sensor.deviceId] || {};
            if (Object.keys(historyForSensor).length > 0) {
                return historyForSensor;
            }
        }
        // Otherwise use real-time MQTT data
        return sensor.readings || {};
    }

    getSensorTypeSortOrder(sensorType) {
        // Return sort priority: Temperature, Humidity, Pressure, CO2, NOx, VOC, Particles
        const type = sensorType.toLowerCase();

        if (type.includes('temperature') || type.includes('temp')) return 1;
        if (type.includes('humidity')) return 2;
        if (type.includes('pressure')) return 3;
        if (type.includes('co2')) return 4;
        if (type.includes('nox')) return 5;
        if (type.includes('voc')) return 6;
        if (type.includes('pm') || type.includes('particle')) return 7;

        // Unknown sensor types go to the end
        return 999;
    }

    sortSensorReadings(readings) {
        // Sort sensor readings by the specified order
        return Object.entries(readings).sort((a, b) => {
            const orderA = this.getSensorTypeSortOrder(a[0]);
            const orderB = this.getSensorTypeSortOrder(b[0]);
            return orderA - orderB;
        });
    }

    createSensorItem(sensor) {
        const hasLocation = sensor.latitude && sensor.longitude;
        const displayReadings = this.getDisplayReadings(sensor);
        const deviceType = this.getDeviceType(sensor);
        
        // Get the most recent timestamp from any reading
        let lastSeenTime = sensor.lastUpdated;
        if (!lastSeenTime && displayReadings) {
            const timestamps = Object.values(displayReadings)
                .map(r => r.timestamp)
                .filter(t => t)
                .sort((a, b) => new Date(b) - new Date(a));
            lastSeenTime = timestamps[0] || null;
        }
        
        const readingsHtml = this.sortSensorReadings(displayReadings)
            .map(([type, data]) => {
                const rawValue = data.value !== undefined ? data.value : data;
                const value = this.formatValue(rawValue, type);
                const unit = data.unit || this.getUnitForSensorType(type);
                const label = data.type === 'average' ? ` (avg)` : '';
                const trend = data.trend ? this.getTrendArrow(data.trend) : '';
                return `
                    <div class="reading">
                        <span class="reading-type">${this.formatSensorType(type)}${label}:</span>
                        <span class="reading-value">${value} ${unit} ${trend}</span>
                    </div>
                `;
            })
            .join('');

        // Format location string using shared method
        let locationText = hasLocation ? this.getLocationString(sensor) : 'No location';
        
        return `
            <div class="sensor-item ${this.selectedSensorId === sensor.deviceId ? 'active' : ''}" data-device-id="${sensor.deviceId}">
                <div class="sensor-name">${sensor.name}</div>
                <div class="sensor-meta" style="font-size: 11px; color: #999; margin-bottom: 2px;">
                    ${locationText}
                </div>
                <div class="sensor-meta" style="font-size: 10px; color: #888; margin-bottom: 2px;">
                    ${deviceType.type || 'Unknown'}${deviceType.board ? ' - ' + deviceType.board : ' - Unknown'}
                </div>
                <div class="sensor-meta" style="font-size: 9px; color: #aaa; margin-bottom: 4px;">
                    ${lastSeenTime ? this.getTimeAgo(lastSeenTime) : 'No data'}
                </div>
                <div class="sensor-readings">
                    ${readingsHtml || '<div style="color: #ccc; font-size: 11px;">No readings yet</div>'}
                </div>
            </div>
        `;
    }

    createPopupContent(sensor) {
        const div = document.createElement('div');
        div.className = 'info-window';

        // Use sensor name as title, fallback to deviceId if name is missing or same as ID
        const displayName = sensor.name || sensor.deviceId;
        let html = `<div class="info-window-title">
            ${displayName}
            <div style="font-size: 9px; color: #888; font-weight: normal; margin-top: 2px;">ID: ${sensor.deviceId}</div>
        </div>`;
        html += `<div class="info-window-content" style="display: flex; flex-direction: column; gap: 0;">`;

        // Readings section - larger and bold names, at top
        const displayReadings = this.getDisplayReadings(sensor);
        if (Object.keys(displayReadings).length > 0) {
            html += `<div style="display: grid; gap: 2px; font-size: 12px; margin: 0 0 10px 0; padding: 0; line-height: 1.3;">`;
            this.sortSensorReadings(displayReadings).forEach(([type, data], idx) => {
                const rawValue = data.value !== undefined ? data.value : data;
                const value = this.formatValue(rawValue, type);
                const unit = data.unit || this.getUnitForSensorType(type);
                const trend = data.trend ? ` ${this.getTrendArrow(data.trend)}` : '';
                const chartId = `mini-chart-${sensor.deviceId}-${idx}`;
                html += `<div style="display: flex; align-items: center; gap: 8px; margin: 0; padding: 2px 0;">
                    <div style="flex: 1;"><strong>${this.formatSensorType(type)}:</strong> ${value}${unit}${trend}</div>
                    <canvas id="${chartId}" width="60" height="20" style="width: 60px; height: 20px;"></canvas>
                </div>`;
            });
            html += `</div>`;
        }

        // Device info section - smaller, at bottom, tight spacing
        html += `<div style="font-size: 10px; border-top: 1px solid #eee; padding-top: 6px; line-height: 1.2;">`;

        // Location
        const location = this.getLocationString(sensor);
        html += `<p style="margin: 0;"><strong>Location:</strong> ${location}</p>`;

        // Deployment type if available
        if (sensor.deployment_type) {
            const envDisplay = sensor.deployment_type.charAt(0).toUpperCase() + sensor.deployment_type.slice(1).toLowerCase();
            const sourceLabel = sensor.deployment_type_source === 'manual' ? ' (manual)' :
                               sensor.deployment_type_source === 'inferred' ? ' (auto)' : '';
            html += `<p style="margin: 0;"><strong>Deployment:</strong> ${envDisplay}${sourceLabel}</p>`;
        }

        // Node info and documentation link
        if (sensor.node_info || sensor.node_info_url) {
            if (sensor.node_info) {
                html += `<p style="margin: 0;"><strong>Setup:</strong> ${sensor.node_info}</p>`;
            }
            if (sensor.node_info_url) {
                html += `<p style="margin: 0;"><a href="${sensor.node_info_url}" target="_blank" rel="noopener" style="color: #60a5fa; text-decoration: none;">View sensor documentation</a></p>`;
            }
        }

        // Get data source and board from sensor top-level or from getDeviceType
        let dataSource = sensor.data_source;
        let boardModel = sensor.board_model;

        // If not available at top level, try getDeviceType
        if (!dataSource || !boardModel) {
            const deviceType = this.getDeviceType(sensor);
            dataSource = dataSource || deviceType.type || 'Unknown';
            boardModel = boardModel || deviceType.board || 'Unknown';
        }

        // Format the data source for display
        const formattedSource = this.formatDataSource(dataSource) || dataSource;

        html += `<p style="margin: 0;"><strong>Source:</strong> ${formattedSource}</p>`;
        if (boardModel && boardModel !== 'Unknown') {
            html += `<p style="margin: 0;"><strong>Board:</strong> ${boardModel}</p>`;
        }

        // Show last updated time
        if (sensor.lastUpdated) {
            const timeAgo = this.getTimeAgo(sensor.lastUpdated);
            html += `<p style="margin: 0; color: #888;"><strong>Updated:</strong> ${timeAgo}</p>`;
        }

        html += `</div>`;
        html += `</div>`;
        div.innerHTML = html;
        return div;
    }

    selectSensor(deviceId, shouldZoom = false) {
        this.selectedSensorId = deviceId;
        const sensor = this.sensors.find(s => s.deviceId === deviceId);

        // Zoom to sensor location when clicked from sidebar
        if (shouldZoom && sensor && sensor.latitude && sensor.longitude) {
            this.map.setView([sensor.latitude, sensor.longitude], 15);

            // Open the marker popup if it exists
            const marker = this.markers.get(deviceId);
            if (marker) {
                marker.openPopup();
            }
        }

        this.updateSidebar();
    }

    updateStats() {
        const now = Date.now();
        const filteredSensors = this.getFilteredSensors();
        const activeSensors = filteredSensors.length;

        // Calculate newest reading time
        let newestTimestamp = null;
        filteredSensors.forEach(sensor => {
            const lastSeen = this.getLastSeenTime(sensor);
            if (lastSeen) {
                const ts = new Date(lastSeen).getTime();
                if (!newestTimestamp || ts > newestTimestamp) {
                    newestTimestamp = ts;
                }
            }
        });

        let newestReadingText = '-';
        if (newestTimestamp) {
            const ageMs = now - newestTimestamp;
            if (ageMs < 60 * 1000) {
                newestReadingText = `${Math.floor(ageMs / 1000)}s ago`;
            } else if (ageMs < 60 * 60 * 1000) {
                newestReadingText = `${Math.floor(ageMs / (60 * 1000))}m ago`;
            } else if (ageMs < 24 * 60 * 60 * 1000) {
                newestReadingText = `${Math.floor(ageMs / (60 * 60 * 1000))}h ago`;
            } else {
                newestReadingText = `${Math.floor(ageMs / (24 * 60 * 60 * 1000))}d ago`;
            }
        }

        // Count unique countries from filtered sensors
        const uniqueCountries = new Set();

        filteredSensors.forEach(sensor => {
            if (sensor.country && sensor.country !== 'unknown') {
                uniqueCountries.add(sensor.country);
            }
        });

        const countriesCount = uniqueCountries.size;

        // Update DOM
        document.getElementById('activeSensors').textContent = activeSensors;
        document.getElementById('newestReading').textContent = newestReadingText;
        document.getElementById('regionsCount').textContent = this.totalRegionsCount;
        document.getElementById('countriesCount').textContent = `${countriesCount}/195`;

        // Update aging distribution chart
        this.updateAgingChart();
    }

    updateLeaderboard() {
        const leaderboardList = document.getElementById('leaderboardList');
        if (!leaderboardList) return;

        const { byNodes, bySensors, byTypes } = this.leaderboardData;

        // Check if we have any data
        if ((!byNodes || byNodes.length === 0) &&
            (!bySensors || bySensors.length === 0) &&
            (!byTypes || byTypes.length === 0)) {
            leaderboardList.innerHTML = '<div class="leaderboard-empty">No data yet</div>';
            return;
        }

        const rankClasses = ['gold', 'silver', 'bronze', '', ''];

        // Helper to build a section
        const buildSection = (title, regions, countKey) => {
            if (!regions || regions.length === 0) return '';

            let itemsHtml = '';
            regions.forEach((region, index) => {
                const rankClass = rankClasses[index] || '';
                const formattedRegion = this.formatLocationName(region.name);
                const countryDisplay = region.country_code ? ` (${region.country_code.substring(0, 2)})` : '';
                const fullName = `${formattedRegion}${countryDisplay}`;
                const count = region[countKey];
                itemsHtml += `
                    <div class="leaderboard-item" data-lat="${region.lat}" data-lon="${region.lon}" style="cursor: pointer;">
                        <span class="leaderboard-rank ${rankClass}">#${index + 1}</span>
                        <span class="leaderboard-region" title="Click to view ${fullName}">${formattedRegion}<span style="opacity: 0.6; margin-left: 4px;">${countryDisplay}</span></span>
                        <span class="leaderboard-count">${count}</span>
                    </div>
                `;
            });

            return `
                <div class="leaderboard-section">
                    <div class="leaderboard-section-header">${title}</div>
                    <div class="leaderboard-list">${itemsHtml}</div>
                </div>
            `;
        };

        const html =
            buildSection('Most Sensor Nodes', byNodes, 'sensor_count') +
            buildSection('Most Sensors', bySensors, 'sensor_count') +
            buildSection('Most Sensor Types', byTypes, 'type_count');

        leaderboardList.innerHTML = html;

        // Add click handlers for navigation
        leaderboardList.querySelectorAll('.leaderboard-item[data-lat]').forEach(item => {
            item.addEventListener('click', () => {
                const lat = parseFloat(item.dataset.lat);
                const lon = parseFloat(item.dataset.lon);
                if (!isNaN(lat) && !isNaN(lon)) {
                    this.map.setView([lat, lon], 10);  // Zoom level 10 for town view
                }
            });
        });
    }

    updateAgingChart() {
        // Use allSensorsForAging (30d data) for the distribution chart
        if (!this.allSensorsForAging || this.allSensorsForAging.length === 0) {
            return;
        }

        // Define age buckets - each shows sensors in that specific range
        const timeframes = [
            { label: '30m', range: '30m', minMs: 0, maxMs: 30 * 60 * 1000 },
            { label: '1h', range: '1h', minMs: 30 * 60 * 1000, maxMs: 60 * 60 * 1000 },
            { label: '2h', range: '2h', minMs: 60 * 60 * 1000, maxMs: 2 * 60 * 60 * 1000 },
            { label: '4h', range: '4h', minMs: 2 * 60 * 60 * 1000, maxMs: 4 * 60 * 60 * 1000 },
            { label: '8h', range: '8h', minMs: 4 * 60 * 60 * 1000, maxMs: 8 * 60 * 60 * 1000 },
            { label: '24h', range: '24h', minMs: 8 * 60 * 60 * 1000, maxMs: 24 * 60 * 60 * 1000 },
            { label: '7d', range: '7d', minMs: 24 * 60 * 60 * 1000, maxMs: 7 * 24 * 60 * 60 * 1000 },
            { label: '30d+', range: '30d', minMs: 7 * 24 * 60 * 60 * 1000, maxMs: Infinity }
        ];

        const now = Date.now();
        const counts = timeframes.map(tf => {
            const count = this.allSensorsForAging.filter(sensor => {
                const lastSeenTime = this.getLastSeenTime(sensor);
                if (!lastSeenTime) return false;

                const age = now - new Date(lastSeenTime).getTime();
                // Count sensors whose age falls within this bucket's range
                return age > tf.minMs && age <= tf.maxMs;
            }).length;
            return { ...tf, count };
        });

        // Find max count for scaling
        const maxCount = Math.max(...counts.map(c => c.count), 1);

        // Render bars
        const barsContainer = document.getElementById('agingBars');
        if (!barsContainer) return;

        let html = '';
        counts.forEach(item => {
            // Skip rendering bars with 0 count
            if (item.count === 0) {
                return;
            }

            const widthPercent = (item.count / maxCount) * 100;
            const isActive = this.currentTimeRange === item.range;
            const barClass = isActive ? 'active' : '';

            html += `
                <div class="aging-bar-item ${barClass}">
                    <div class="aging-bar-label">${item.label}</div>
                    <div class="aging-bar-container">
                        <div class="aging-bar-fill" style="width: ${widthPercent}%">
                            <span class="aging-bar-count">${item.count}</span>
                        </div>
                    </div>
                </div>
            `;
        });

        barsContainer.innerHTML = html;
    }

    startAutoRefresh() {
        // Refresh every 60 seconds
        this.refreshInterval = setInterval(() => this.loadSensors(), 60000);
        // Refresh aging data every 5 minutes (less frequent since it's 30d data)
        this.agingRefreshInterval = setInterval(() => this.loadAllSensorsForAging(), 300000);
        // Refresh leaderboard every 5 minutes (24h data, doesn't change frequently)
        this.leaderboardRefreshInterval = setInterval(() => this.loadLeaderboard(), 300000);
    }

    stopAutoRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
        if (this.agingRefreshInterval) {
            clearInterval(this.agingRefreshInterval);
        }
        if (this.leaderboardRefreshInterval) {
            clearInterval(this.leaderboardRefreshInterval);
        }
    }

    createSensorIcon(sensor) {
        const size = 60;  // Increased from 50
        const metrics = this.extractSensorMetrics(sensor);
        const health = this.calculateAirQualityHealth(metrics);
        
        // Create segmented ring SVG
        const svg = this.createSegmentedRingSVG(size, metrics, health);
        
        return L.divIcon({
            className: 'custom-sensor-marker',
            html: svg,
            iconSize: [size, size],
            iconAnchor: [size / 2, size / 2],
            popupAnchor: [0, -size / 2]
        });
    }
    
    extractSensorMetrics(sensor) {
        const readings = sensor.readings || {};
        const metrics = {
            pm25: null,
            co2: null,
            temp: null,
            humidity: null,
            pressure: null,
            voc: null,
            lastUpdate: sensor.lastUpdated
        };

        // Extract values from readings
        Object.entries(readings).forEach(([type, data]) => {
            const value = data.value !== undefined ? data.value : data;
            if (type.includes('pm2_5') || type.includes('pm25')) {
                metrics.pm25 = value;
            } else if (type.includes('co2')) {
                metrics.co2 = value;
            } else if (type.includes('temperature')) {
                metrics.temp = value;
            } else if (type.includes('humidity')) {
                metrics.humidity = value;
            } else if (type.includes('pressure')) {
                metrics.pressure = value;
            } else if (type.includes('voc_index')) {
                metrics.voc = value;
            }
        });

        return metrics;
    }
    
    calculateAirQualityHealth(metrics) {
        let score = 100;
        let issues = [];
        let hasAirQualityData = false;
        
        // PM2.5 assessment (WHO guidelines)
        if (metrics.pm25 !== null) {
            hasAirQualityData = true;
            if (metrics.pm25 > 35) {
                score -= 40;
                issues.push('high PM2.5');
            } else if (metrics.pm25 > 12) {
                score -= 20;
                issues.push('moderate PM2.5');
            }
        }
        
        // CO2 assessment
        if (metrics.co2 !== null) {
            hasAirQualityData = true;
            if (metrics.co2 > 1500) {
                score -= 30;
                issues.push('very high CO2');
            } else if (metrics.co2 > 1000) {
                score -= 15;
                issues.push('high CO2');
            } else if (metrics.co2 > 800) {
                score -= 5;
                issues.push('elevated CO2');
            }
        }
        
        // VOC assessment
        if (metrics.voc !== null) {
            hasAirQualityData = true;
            if (metrics.voc > 250) {
                score -= 20;
                issues.push('poor VOC');
            } else if (metrics.voc > 150) {
                score -= 10;
                issues.push('moderate VOC');
            }
        }
        
        // Check data freshness
        const isStale = this.isDataStale(metrics.lastUpdate);
        
        // Determine leaf colour and type based on air quality
        let leafType, leafColor;
        if (!hasAirQualityData) {
            // Grey horizontal leaf for sensors without air quality data
            leafType = 'neutral';
            leafColor = '#999999';
        } else if (score >= 80) {
            // Fresh green rising leaf - excellent air quality
            leafType = 'rising';
            leafColor = '#2ecc71';
        } else if (score >= 60) {
            // Yellow-brown falling leaf - moderate air quality
            leafType = 'falling';
            leafColor = '#e8b14d';
        } else if (score >= 40) {
            // Brown falling leaf - poor air quality
            leafType = 'falling';
            leafColor = '#a67c52';
        } else {
            // Dark brown falling leaf - unhealthy air quality
            leafType = 'falling';
            leafColor = '#654321';
        }
        
        return {
            score: Math.max(0, score),
            level: score >= 80 ? 'good' : score >= 60 ? 'moderate' : score >= 40 ? 'poor' : 'unhealthy',
            color: leafColor,
            leafType: leafType,
            leafColor: leafColor,
            isStale: isStale,
            hasAirQualityData: hasAirQualityData,
            issues: issues
        };
    }
    
    isDataStale(timestamp) {
        if (!timestamp) return true;
        const now = new Date();
        const then = new Date(timestamp);
        const diffHours = (now - then) / (1000 * 60 * 60);
        return diffHours > 1;
    }
    
    createSegmentedRingSVG(size, metrics, health) {
        const center = size / 2;
        const outerRadius = size / 2 - 2;
        const innerRadius = outerRadius - 6;
        const segments = [];

        // Define 4 segments: Temp (top-left), Humidity (top-right), CO2 (bottom-right), Pressure (bottom-left)
        const segmentData = [
            { metric: 'temp', value: metrics.temp, startAngle: -90, endAngle: 0, label: 'T' },         // Top Left (9-12 o'clock)
            { metric: 'humidity', value: metrics.humidity, startAngle: 0, endAngle: 90, label: 'H' },  // Top Right (12-3 o'clock)
            { metric: 'co2', value: metrics.co2, startAngle: 90, endAngle: 180, label: 'CO₂' },        // Bottom Right (3-6 o'clock)
            { metric: 'pressure', value: metrics.pressure, startAngle: 180, endAngle: 270, label: 'P' } // Bottom Left (6-9 o'clock)
        ];

        // Generate arc paths for each segment
        segmentData.forEach(seg => {
            if (seg.value !== null) {
                const color = this.getColorForMetric(seg.metric, seg.value);
                const path = this.createArcPath(center, center, innerRadius, outerRadius, seg.startAngle, seg.endAngle);
                segments.push(`<path d="${path}" fill="${color}" opacity="0.9"/>`);
            } else {
                // Grey segment for missing data
                const path = this.createArcPath(center, center, innerRadius, outerRadius, seg.startAngle, seg.endAngle);
                segments.push(`<path d="${path}" fill="#cccccc" opacity="0.3"/>`);
            }
        });

        // Center circle with health indicator
        const glowColor = health.isStale ? '#999999' : health.color;
        const leafSVG = this.createLeafSVG(center, center, health.leafType, health.leafColor);

        // Check if dark mode is active and which layer is shown
        const isDarkMode = document.body.classList.contains('dark-mode');
        const isStreetLayer = document.getElementById('map')?.classList.contains('street-layer');

        // For street layer in dark mode, use white (gets inverted to black by CSS filter)
        // For satellite/hybrid in dark mode, use black directly (no CSS filter)
        // For light mode, use white
        let centerFill;
        if (isDarkMode && isStreetLayer) {
            centerFill = 'white'; // Will be inverted to black by CSS filter
        } else if (isDarkMode) {
            centerFill = '#1a1a1a'; // Black for satellite/hybrid
        } else {
            centerFill = 'white'; // Light mode
        }

        return `
            <svg width="${size}" height="${size}" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <filter id="glow-${health.level}" x="-50%" y="-50%" width="200%" height="200%">
                        <feGaussianBlur stdDeviation="2" result="coloredBlur"/>
                        <feMerge>
                            <feMergeNode in="coloredBlur"/>
                            <feMergeNode in="SourceGraphic"/>
                        </feMerge>
                    </filter>
                </defs>
                ${segments.join('')}
                <circle cx="${center}" cy="${center}" r="${innerRadius - 1}" fill="${centerFill}" stroke="${glowColor}" stroke-width="2" filter="url(#glow-${health.level})"/>
                ${leafSVG}
            </svg>
        `;
    }
    
    createLeafSVG(cx, cy, leafType, color) {
        const darkerColor = this.adjustColorBrightness(color, -20);
        
        if (leafType === 'neutral') {
            // Neutral horizontal leaf - no air quality data
            return `
                <g transform="translate(${cx - 14}, ${cy - 7}) rotate(0)">
                    <!-- Stem -->
                    <line x1="0" y1="7" x2="5" y2="7" stroke="${darkerColor}" stroke-width="2" stroke-linecap="round"/>
                    <!-- Horizontal leaf body -->
                    <path d="M 5 7 C 8 4, 12 3, 16 3 C 20 3, 24 4, 27 7 C 24 10, 20 11, 16 11 C 12 11, 8 10, 5 7 Z" 
                          fill="${color}" stroke="${darkerColor}" stroke-width="1.2"/>
                    <!-- Center vein -->
                    <path d="M 7 7 L 25 7" stroke="${darkerColor}" stroke-width="1.2" opacity="0.6" fill="none"/>
                    <!-- Side veins -->
                    <path d="M 12 7 Q 11 5 10 4" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 12 7 Q 11 9 10 10" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 20 7 Q 21 5 22 4" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 20 7 Q 21 9 22 10" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                </g>
            `;
        } else if (leafType === 'rising') {
            // Rising leaf - pointing upward to the right (good air quality)
            const angle = -60 - 90;  // -150 total to point upward-right
            return `
                <g transform="rotate(${angle} ${cx} ${cy}) translate(${cx - 12}, ${cy - 14})">
                    <!-- Stem -->
                    <line x1="12" y1="28" x2="12" y2="22" stroke="${darkerColor}" stroke-width="2" stroke-linecap="round"/>
                    <!-- Leaf body with natural asymmetric curves -->
                    <path d="M 12 2 C 7 4, 4 8, 3 14 C 2 18, 4 24, 8 27 C 10 28, 11 28, 12 28 C 13 28, 14 28, 16 27 C 20 24, 22 18, 21 14 C 20 8, 17 4, 12 2 Z" 
                          fill="${color}" stroke="${darkerColor}" stroke-width="1.2"/>
                    <!-- Center vein with slight curve -->
                    <path d="M 12 4 Q 11.5 14 12 26" stroke="${darkerColor}" stroke-width="1.2" opacity="0.6" fill="none"/>
                    <!-- Side veins -->
                    <path d="M 12 9 Q 8 12 6 15" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 12 9 Q 16 12 18 15" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 12 16 Q 8 18 6 21" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                    <path d="M 12 16 Q 16 18 18 21" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45" fill="none"/>
                </g>
            `;
        } else {
            // Falling leaf - right side lower than left (declining air quality)
            const angle = 30;  // Positive angle to droop downward-right
            return `
                <g transform="rotate(${angle} ${cx} ${cy}) translate(${cx - 13}, ${cy - 13})">
                    <!-- Stem -->
                    <line x1="13" y1="28" x2="13" y2="22" stroke="${darkerColor}" stroke-width="2" stroke-linecap="round"/>
                    <!-- Drooping maple-style leaf with serrated edges -->
                    <path d="M 13 2 L 17 9 L 21 12 L 18 15 L 20 23 L 13 20 L 6 23 L 8 15 L 5 12 L 9 9 Z" 
                          fill="${color}" stroke="${darkerColor}" stroke-width="1.2"/>
                    <!-- Center vein with slight bend -->
                    <path d="M 13 4 Q 12.5 12 13 21" stroke="${darkerColor}" stroke-width="1.2" opacity="0.6" fill="none"/>
                    <!-- Side veins -->
                    <line x1="13" y1="9" x2="9" y2="12" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45"/>
                    <line x1="13" y1="9" x2="17" y2="12" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45"/>
                    <line x1="13" y1="15" x2="9" y2="17" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45"/>
                    <line x1="13" y1="15" x2="17" y2="17" stroke="${darkerColor}" stroke-width="0.9" opacity="0.45"/>
                </g>
            `;
        }
    }
    
    createArcPath(cx, cy, innerRadius, outerRadius, startAngle, endAngle) {
        const toRadians = (angle) => (angle - 90) * Math.PI / 180;
        
        const startAngleRad = toRadians(startAngle);
        const endAngleRad = toRadians(endAngle);
        
        const x1 = cx + outerRadius * Math.cos(startAngleRad);
        const y1 = cy + outerRadius * Math.sin(startAngleRad);
        const x2 = cx + outerRadius * Math.cos(endAngleRad);
        const y2 = cy + outerRadius * Math.sin(endAngleRad);
        const x3 = cx + innerRadius * Math.cos(endAngleRad);
        const y3 = cy + innerRadius * Math.sin(endAngleRad);
        const x4 = cx + innerRadius * Math.cos(startAngleRad);
        const y4 = cy + innerRadius * Math.sin(startAngleRad);
        
        const largeArc = endAngle - startAngle > 180 ? 1 : 0;
        
        return `
            M ${x1} ${y1}
            A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${x2} ${y2}
            L ${x3} ${y3}
            A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${x4} ${y4}
            Z
        `;
    }
    
    getColorForMetric(metric, value) {
        switch(metric) {
            case 'pm25':
                if (value < 12) return '#2ecc71'; // Good
                if (value < 35) return '#f39c12'; // Moderate
                if (value < 55) return '#e67e22'; // Unhealthy for sensitive
                return '#e74c3c'; // Unhealthy
                
            case 'co2':
                if (value < 600) return '#2ecc71'; // Excellent
                if (value < 1000) return '#f39c12'; // Acceptable
                if (value < 1500) return '#e67e22'; // Poor
                return '#e74c3c'; // Bad
                
            case 'temp':
                // Use BOM 2013 temperature scale (same as region heatmap)
                return this.getColorForRegionMetric('temperature', value);
                
            case 'humidity':
                if (value < 30) return '#e67e22'; // Too dry
                if (value < 60) return '#2ecc71'; // Comfortable
                if (value < 70) return '#f39c12'; // Humid
                return '#e74c3c'; // Very humid

            case 'pressure':
                // Handle both kPa and hPa readings
                // If value < 200, assume it's in kPa and convert to hPa
                const pressureHPa = value < 200 ? value * 10 : value;

                if (pressureHPa < 980) return '#e74c3c'; // Very low (stormy)
                if (pressureHPa < 1000) return '#f39c12'; // Low (rain likely)
                if (pressureHPa < 1020) return '#2ecc71'; // Normal
                if (pressureHPa < 1040) return '#5dade2'; // High (fair)
                return '#3498db'; // Very high (very dry)

            default:
                return '#3D7A7A';
        }
    }

    adjustColorBrightness(color, amount) {
        // Convert hex to RGB, adjust brightness, convert back
        const num = parseInt(color.replace('#', ''), 16);
        const r = Math.max(0, Math.min(255, (num >> 16) + amount));
        const g = Math.max(0, Math.min(255, ((num >> 8) & 0x00FF) + amount));
        const b = Math.max(0, Math.min(255, (num & 0x0000FF) + amount));
        return '#' + ((r << 16) | (g << 8) | b).toString(16).padStart(6, '0');
    }
    
    getColorForSensor(sensor) {
        // Legacy function - kept for compatibility
        const metrics = this.extractSensorMetrics(sensor);
        const health = this.calculateAirQualityHealth(metrics);
        return health.color;
    }

    formatSensorType(type) {
        return type
            .replace(/_/g, ' ')
            .replace(/sht4x|bme680|sgp41|pms5003|scd4x|bmp280/i, '')
            .trim()
            .split(' ')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }
    
    getTrendArrow(trend) {
        const arrows = {
            'up': '↑',
            'down': '↓',
            'flat': '→'
        };
        return arrows[trend] || '';
    }
    
    getDeviceType(sensor) {
        // Check sensor-level properties first (from ClickHouse)
        if (sensor.data_source) {
            const type = this.formatDataSource(sensor.data_source);
            const board = sensor.board_model || null;
            return { type, board };
        }

        // Fallback: check readings for raw metadata (legacy InfluxDB format)
        const readings = sensor.readings || {};

        // First pass: look for readings with board_model set (prefer complete metadata)
        for (const reading of Object.values(readings)) {
            if (reading.raw && Object.keys(reading.raw).length > 0) {
                const raw = reading.raw;

                // Prefer Meshtastic readings with board_model
                if ((raw.data_source === 'MESHTASTIC' || raw.data_source === 'MESHTASTIC_PUBLIC' || raw.data_source === 'MESHTASTIC_COMMUNITY') && raw.board_model) {
                    return {
                        type: this.formatDataSource(raw.data_source),
                        board: raw.board_model
                    };
                }

                // Prefer SkyTrace Homebrew with board_model
                if ((raw.board_manufacturer || raw.firmware_version) && raw.board_model) {
                    return {
                        type: 'SkyTrace Homebrew',
                        board: raw.board_model
                    };
                }
            }
        }

        // Second pass: fallback to readings without board_model
        for (const reading of Object.values(readings)) {
            if (reading.raw && Object.keys(reading.raw).length > 0) {
                const raw = reading.raw;

                if (raw.data_source === 'MESHTASTIC' || raw.data_source === 'MESHTASTIC_PUBLIC' || raw.data_source === 'MESHTASTIC_COMMUNITY') {
                    return {
                        type: this.formatDataSource(raw.data_source),
                        board: null
                    };
                }

                if (raw.board_manufacturer || raw.firmware_version) {
                    return {
                        type: 'SkyTrace Homebrew',
                        board: null
                    };
                }
            }
        }

        // Fallback: detect by device ID pattern
        if (sensor.deviceId && (sensor.deviceId.startsWith('!') || sensor.deviceId.startsWith('meshtastic_'))) {
            return { type: 'Meshtastic', board: null };
        }

        return { type: null, board: null };
    }

    formatDataSource(source) {
        // Convert "MESHTASTIC" to "Meshtastic", "WESENSE" to "WeSense"
        if (!source) return null;
        const map = {
            'MESHTASTIC': 'Meshtastic',
            'MESHTASTIC_PUBLIC': 'Meshtastic Public',
            'MESHTASTIC_COMMUNITY': 'Meshtastic Community',
            'WESENSE': 'WeSense',
            'meshtastic-public': 'Meshtastic Public',
            'meshtastic-community': 'Meshtastic Community'
        };
        return map[source] || source;
    }
    
    drawSparklines(sensor) {
        const displayReadings = this.getDisplayReadings(sensor);
        this.sortSensorReadings(displayReadings).forEach(([type, data], idx) => {
            if (!data.sparklineData || data.sparklineData.length < 2) return;
            
            const chartId = `mini-chart-${sensor.deviceId}-${idx}`;
            const canvas = document.getElementById(chartId);
            if (!canvas) return;
            
            // Destroy existing chart if any
            if (canvas.chart) {
                canvas.chart.destroy();
            }
            
            canvas.width = 60;
            canvas.height = 20;
            
            canvas.chart = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: data.sparklineData.map((_, i) => i),
                    datasets: [{
                        data: data.sparklineData,
                        borderColor: this.getSparklineColor(data.trend),
                        backgroundColor: 'rgba(0,0,0,0)',
                        borderWidth: 1,
                        pointRadius: 0,
                        tension: 0.3,
                        fill: false
                    }]
                },
                options: {
                    responsive: false,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        x: { display: false },
                        y: { display: false }
                    }
                }
            });
        });
    }
    
    getSparklineColor(trend) {
        const colors = {
            'up': '#e74c3c',
            'down': '#3498db',
            'flat': '#95a5a6'
        };
        return colors[trend] || '#3D7A7A';
    }
    
    // =========================================================================
    // Stats Tab
    // =========================================================================

    setupStats() {
        // Debug toggle
        const toggle = document.getElementById('statsDebugToggle');
        const section = document.getElementById('statsDebugSection');
        if (toggle && section) {
            toggle.addEventListener('click', () => {
                toggle.classList.toggle('expanded');
                section.classList.toggle('expanded');
            });
        }

        // Refresh button
        const refreshBtn = document.getElementById('statsRefreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.loadStats());
        }
    }

    async loadStats() {
        const refreshBtn = document.getElementById('statsRefreshBtn');
        if (refreshBtn) refreshBtn.classList.add('loading');

        try {
            const [overview, orbitdb, zenoh, nodes, trust] = await Promise.allSettled([
                fetch('/api/stats/overview').then(r => r.json()),
                fetch('/api/stats/orbitdb').then(r => r.json()),
                fetch('/api/stats/zenoh').then(r => r.json()),
                fetch('/api/stats/nodes').then(r => r.json()),
                fetch('/api/stats/trust').then(r => r.json()),
            ]);

            const ov = overview.status === 'fulfilled' ? overview.value : {};
            const ob = orbitdb.status === 'fulfilled' ? orbitdb.value : {};
            const ze = zenoh.status === 'fulfilled' ? zenoh.value : {};
            const no = nodes.status === 'fulfilled' ? nodes.value : {};
            const tr = trust.status === 'fulfilled' ? trust.value : {};

            this.renderUserStats(ov, ob, ze);
            this.renderDebugStats(ob, ze, no, tr, ov);
        } catch (err) {
            console.error('Failed to load stats:', err);
        } finally {
            if (refreshBtn) refreshBtn.classList.remove('loading');
        }
    }

    renderUserStats(overview, orbitdb, zenoh) {
        // Hero: P2P Peers
        const peersEl = document.getElementById('statsPeers');
        const peersSubEl = document.getElementById('statsPeersSub');
        if (orbitdb && orbitdb.peer_count != null) {
            peersEl.textContent = orbitdb.peer_count;
            peersSubEl.textContent = 'connected';
        } else if (orbitdb && orbitdb.status === 'not_configured') {
            peersEl.textContent = '--';
            peersSubEl.textContent = 'not configured';
        } else {
            peersEl.textContent = '--';
            peersSubEl.textContent = 'offline';
        }

        // Hero: Devices Online
        const devicesEl = document.getElementById('statsDevices');
        const devicesSubEl = document.getElementById('statsDevicesSub');
        if (overview.active_devices_24h != null) {
            devicesEl.textContent = this.formatLargeNumber(overview.active_devices_24h);
            devicesSubEl.textContent = `${overview.active_devices_1h || 0} in last hour`;
        }

        // Hero: Readings/min
        const rateEl = document.getElementById('statsRate');
        const rateSubEl = document.getElementById('statsRateSub');
        if (overview.readings_per_minute != null) {
            rateEl.textContent = overview.readings_per_minute;
            rateSubEl.textContent = `${this.formatLargeNumber(overview.readings_last_24h || 0)} in 24h`;
        }

        // Hero: Coverage
        const coverageEl = document.getElementById('statsCoverage');
        const coverageSubEl = document.getElementById('statsCoverageSub');
        if (overview.coverage) {
            coverageEl.textContent = `${overview.coverage.countries}/${overview.coverage.regions}`;
            coverageSubEl.textContent = 'countries / regions';
        }

        // Data Sources
        const sourcesEl = document.getElementById('statsDataSources');
        if (overview.data_sources && Object.keys(overview.data_sources).length > 0) {
            const sourceNames = {
                'WESENSE': 'WeSense WiFi',
                'MESHTASTIC_PUBLIC': 'Meshtastic Public',
                'MESHTASTIC_COMMUNITY': 'Meshtastic Community',
                'HOME_ASSISTANT': 'Home Assistant',
                'TTN': 'TTN LoRaWAN'
            };
            const sourceDotClass = {
                'WESENSE': 'wesense',
                'MESHTASTIC_PUBLIC': 'meshtastic-public',
                'MESHTASTIC_COMMUNITY': 'meshtastic-community',
                'HOME_ASSISTANT': 'homeassistant',
                'TTN': 'ttn'
            };
            sourcesEl.innerHTML = Object.entries(overview.data_sources)
                .map(([key, count]) => `
                    <div class="stats-source-row">
                        <span class="stats-source-name">
                            <span class="source-dot ${sourceDotClass[key] || 'default'}"></span>
                            ${sourceNames[key] || key}
                        </span>
                        <span class="stats-source-count">${count}</span>
                    </div>
                `).join('');
        } else {
            sourcesEl.innerHTML = '<div class="stats-empty">No data</div>';
        }

        // Health indicators
        this.setHealthIndicator('healthClickhouse', overview.clickhouse_connected ? 'healthy' : 'offline');

        if (zenoh && zenoh.status === 'not_configured') {
            this.setHealthIndicator('healthZenoh', 'unknown');
        } else if (zenoh && zenoh.status) {
            this.setHealthIndicator('healthZenoh', zenoh.status === 'ok' || zenoh.connected ? 'healthy' : 'degraded');
        } else {
            this.setHealthIndicator('healthZenoh', 'offline');
        }

        if (orbitdb && orbitdb.status === 'not_configured') {
            this.setHealthIndicator('healthOrbitdb', 'unknown');
        } else if (orbitdb && orbitdb.peer_count != null) {
            this.setHealthIndicator('healthOrbitdb', 'healthy');
        } else {
            this.setHealthIndicator('healthOrbitdb', 'offline');
        }

        // MQTT — infer from whether we have real-time data (readings in last hour)
        if (overview.active_devices_1h > 0) {
            this.setHealthIndicator('healthMqtt', 'healthy');
        } else if (overview.active_devices_24h > 0) {
            this.setHealthIndicator('healthMqtt', 'degraded');
        } else {
            this.setHealthIndicator('healthMqtt', 'unknown');
        }

        // Coverage Details
        const coverageDetailsEl = document.getElementById('statsCoverageDetails');
        if (overview.coverage) {
            coverageDetailsEl.innerHTML = `
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Countries</span>
                    <span class="stats-detail-value">${overview.coverage.countries}</span>
                </div>
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Regions</span>
                    <span class="stats-detail-value">${overview.coverage.regions}</span>
                </div>
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Sensor Types</span>
                    <span class="stats-detail-value">${overview.reading_types_active || '--'}</span>
                </div>
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Total Readings</span>
                    <span class="stats-detail-value">${this.formatLargeNumber(overview.total_readings_all_time || 0)}</span>
                </div>
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Latest Reading</span>
                    <span class="stats-detail-value">${overview.latest_reading ? this.getTimeAgo(overview.latest_reading) : '--'}</span>
                </div>
            `;
        }

        // Live
        const liveEl = document.getElementById('statsLive');
        liveEl.innerHTML = `
            <div class="stats-detail-row">
                <span class="stats-detail-label">Map Viewers</span>
                <span class="stats-detail-value">${overview.active_viewers != null ? overview.active_viewers : '--'}</span>
            </div>
            <div class="stats-detail-row">
                <span class="stats-detail-label">Active Devices (1h)</span>
                <span class="stats-detail-value">${overview.active_devices_1h != null ? overview.active_devices_1h : '--'}</span>
            </div>
            <div class="stats-detail-row">
                <span class="stats-detail-label">Total Devices</span>
                <span class="stats-detail-value">${overview.total_devices != null ? this.formatLargeNumber(overview.total_devices) : '--'}</span>
            </div>
        `;
    }

    renderDebugStats(orbitdb, zenoh, nodes, trust, overview) {
        // P2P Network
        const p2pEl = document.getElementById('debugP2P');
        if (orbitdb && orbitdb.peer_count != null) {
            const peers = orbitdb.peers || [];
            const peerId = orbitdb.libp2p_peer_id || '';
            const addrs = orbitdb.addresses || [];
            p2pEl.innerHTML = `
                ${peerId ? `<div class="stats-mono-row stats-mono">Peer ID: ${this.statsEscapeHtml(peerId)}</div>` : ''}
                ${addrs.length > 0 ? `<div class="stats-mono-row stats-mono">Listen: ${addrs.map(a => this.statsEscapeHtml(a)).join('<br>')}</div>` : ''}
                <div class="stats-mono-row stats-mono">Connected Peers: ${peers.length}${peers.length > 0 ? '<br>' + peers.map(p => this.statsEscapeHtml(p)).join('<br>') : ''}</div>
            `;
        } else {
            p2pEl.innerHTML = '<div class="stats-empty">OrbitDB not connected</div>';
        }

        // OrbitDB Databases
        const dbEl = document.getElementById('debugOrbitDBDatabases');
        if (orbitdb && orbitdb.db_sizes) {
            const dbNames = Object.keys(orbitdb.db_sizes);
            if (dbNames.length > 0) {
                dbEl.innerHTML = dbNames.map(name => {
                    const count = orbitdb.db_sizes[name];
                    const addr = orbitdb.db_addresses?.[name] || '';
                    return `<div class="stats-mono-row stats-mono"><strong>${this.statsEscapeHtml(name)}</strong>: ${count} docs${addr ? `<br><span style="color:var(--text-muted)">${this.statsEscapeHtml(addr)}</span>` : ''}</div>`;
                }).join('');
            } else {
                dbEl.innerHTML = '<div class="stats-empty">No databases</div>';
            }
        } else {
            dbEl.innerHTML = '<div class="stats-empty">OrbitDB not connected</div>';
        }

        // Gossipsub Topics
        const gossipEl = document.getElementById('debugGossipsub');
        if (orbitdb && orbitdb.gossipsub_topics) {
            const topics = Object.entries(orbitdb.gossipsub_topics);
            if (topics.length > 0) {
                gossipEl.innerHTML = topics.map(([topic, subscribers]) => {
                    const count = Array.isArray(subscribers) ? subscribers.length : subscribers;
                    return `<div class="stats-mono-row stats-mono">${this.statsEscapeHtml(topic)}: ${count} subscriber${count !== 1 ? 's' : ''}</div>`;
                }).join('');
            } else {
                gossipEl.innerHTML = '<div class="stats-empty">No topics</div>';
            }
        } else {
            gossipEl.innerHTML = '<div class="stats-empty">OrbitDB not connected</div>';
        }

        // Trust List — shape: { keys: { ingester_id: { version: { status, added, public_key } } } }
        const trustEl = document.getElementById('debugTrust');
        const trustKeys = trust?.keys || {};
        const trustEntries = Object.entries(trustKeys);
        if (trustEntries.length > 0) {
            trustEl.innerHTML = trustEntries.map(([ingesterId, versions]) => {
                // Get the latest version's status
                const latestVersion = Object.values(versions).pop();
                const status = latestVersion?.status === 'active' ? 'active' : 'revoked';
                return `<div class="stats-mono-row stats-mono"><span class="stats-trust-status ${status}"></span>${this.statsEscapeHtml(ingesterId)}</div>`;
            }).join('');
        } else {
            trustEl.innerHTML = '<div class="stats-empty">No trust entries</div>';
        }

        // Registered Nodes
        const nodesEl = document.getElementById('debugNodes');
        const nodeList = nodes?.nodes || [];
        if (nodeList.length > 0) {
            nodesEl.innerHTML = nodeList.map(node => {
                const id = node.ingester_id || node._id || 'unknown';
                const endpoint = node.zenoh_endpoint || '';
                return `<div class="stats-mono-row stats-mono">${this.statsEscapeHtml(id)}${endpoint ? ' <span style="color:var(--text-muted)">(' + this.statsEscapeHtml(endpoint) + ')</span>' : ''}</div>`;
            }).join('');
        } else {
            nodesEl.innerHTML = '<div class="stats-empty">No registered nodes</div>';
        }

        // Background Tasks
        const tasksEl = document.getElementById('debugTasks');
        if (overview.precompute_status) {
            const ps = overview.precompute_status;
            tasksEl.innerHTML = `
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Last Region Refresh</span>
                    <span class="stats-detail-value">${ps.last_refresh ? this.getTimeAgo(ps.last_refresh) : 'never'}</span>
                </div>
                <div class="stats-detail-row">
                    <span class="stats-detail-label">Refresh In Progress</span>
                    <span class="stats-detail-value">${ps.refresh_in_progress ? 'Yes' : 'No'}</span>
                </div>
            `;
        } else {
            tasksEl.innerHTML = '<div class="stats-empty">No data</div>';
        }
    }

    setHealthIndicator(elementId, status) {
        const el = document.getElementById(elementId);
        if (!el) return;
        el.className = `health-dot ${status}`;
    }

    formatLargeNumber(num) {
        if (num == null || isNaN(num)) return '0';
        num = Number(num);
        if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
        if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
        return String(num);
    }

    statsEscapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    getTimeAgo(timestamp) {
        const now = new Date();
        const then = new Date(timestamp);
        const diffMs = now - then;
        const diffMins = Math.floor(diffMs / 60000);
        
        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        
        const diffHours = Math.floor(diffMins / 60);
        if (diffHours < 24) return `${diffHours}h ago`;
        
        const diffDays = Math.floor(diffHours / 24);
        return `${diffDays}d ago`;
    }

    getUnitForSensorType(type) {
        // Check for direct match first
        const units = {
            'temperature': '°C',
            'humidity': '%',
            'pressure': 'hPa',
            'co2': 'ppm',
            'pm1_0': 'µg/m³',
            'pm2_5': 'µg/m³',
            'pm10': 'µg/m³',
            'particles_0_3um': '/L',
            'particles_0_5um': '/L',
            'particles_1_0um': '/L',
            'particles_2_5um': '/L',
            'particles_5_0um': '/L',
            'particles_10um': '/L',
            'voc_raw': '',
            'voc_index': '',
            'nox_raw': '',
            'nox_index': '',
            'altitude': 'm',
            'voltage': 'V',
            'battery_level': '%',
        };

        if (units[type]) return units[type];

        // Check if type contains any known sensor keywords
        if (type.includes('temperature')) return '°C';
        if (type.includes('humidity')) return '%';
        if (type.includes('pressure')) return 'hPa';
        if (type.includes('co2')) return 'ppm';
        if (type.includes('pm')) return 'µg/m³';
        if (type.includes('particles')) return '/L';
        if (type.includes('voc') || type.includes('nox')) return '';
        if (type.includes('altitude')) return 'm';
        if (type.includes('voltage')) return 'V';
        if (type.includes('battery')) return '%';

        return '';
    }

    formatValue(value, type) {
        if (value === null || value === undefined) return '--';

        // Determine decimal places based on sensor type
        const decimals = {
            'temperature': 1,
            'humidity': 0,
            'pressure': 0,
            'co2': 0,
            'pm1_0': 1,
            'pm2_5': 1,
            'pm10': 1,
            'voc_index': 0,
            'voc_raw': 0,
            'nox_index': 0,
            'nox_raw': 0,
            'altitude': 0,
            'voltage': 2,
            'battery_level': 0,
            'particles_0_3um': 0,
            'particles_0_5um': 0,
            'particles_1_0um': 0,
            'particles_2_5um': 0,
            'particles_5_0um': 0,
            'particles_10um': 0,
        };

        // Find matching decimal places
        let dp = 1; // default
        if (decimals[type] !== undefined) {
            dp = decimals[type];
        } else {
            // Check partial matches
            const lowerType = type.toLowerCase();
            if (lowerType.includes('temperature')) dp = 1;
            else if (lowerType.includes('humidity')) dp = 0;
            else if (lowerType.includes('pressure')) dp = 0;
            else if (lowerType.includes('co2')) dp = 0;
            else if (lowerType.includes('pm')) dp = 1;
            else if (lowerType.includes('voc') || lowerType.includes('nox')) dp = 0;
            else if (lowerType.includes('voltage')) dp = 2;
            else if (lowerType.includes('altitude')) dp = 0;
        }

        return Number(value).toFixed(dp);
    }

    formatTime(timestamp) {
        const date = new Date(timestamp);
        const now = new Date();
        const diff = now - date;
        
        const minutes = Math.floor(diff / 60000);
        if (minutes < 1) return 'now';
        if (minutes < 60) return `${minutes}m ago`;
        
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;

        const days = Math.floor(hours / 24);
        return `${days}d ago`;
    }

    // Dashboard Methods
    setupViewSwitching() {
        const navTabs = document.querySelectorAll('.nav-tab');
        navTabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const view = tab.dataset.view;
                this.switchView(view);

                // Update active tab
                navTabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
            });
        });
    }

    switchView(view) {
        const mapView = document.getElementById('mapView');
        const dashboardView = document.getElementById('dashboardView');

        // Clear stats auto-refresh when leaving stats view
        if (this._statsInterval) {
            clearInterval(this._statsInterval);
            this._statsInterval = null;
        }

        if (view === 'map') {
            mapView.classList.add('active');
            dashboardView.classList.remove('active');
            // Invalidate map size when switching back to ensure proper display
            setTimeout(() => this.map.invalidateSize(), 100);
        } else {
            // Dashboard views: dashboard, stats
            mapView.classList.remove('active');
            dashboardView.classList.add('active');

            // Show corresponding dash-view
            document.querySelectorAll('.dash-view').forEach(v => v.classList.remove('active'));
            if (view === 'dashboard') {
                document.getElementById('mySensorsView').classList.add('active');
                this.renderMySensors();
            } else if (view === 'stats') {
                document.getElementById('statsView').classList.add('active');
                this.loadStats();
                // Auto-refresh every 30 seconds while stats tab is active
                this._statsInterval = setInterval(() => this.loadStats(), 30000);
            }
        }
    }

    setupDashboardTabs() {
        // Back to Dashboard button from Overview
        document.getElementById('backToDashboardBtn')?.addEventListener('click', () => {
            document.querySelectorAll('.dash-view').forEach(v => v.classList.remove('active'));
            document.getElementById('mySensorsView')?.classList.add('active');
            this.renderMySensors();
        });
    }

    renderDashboard() {
        // Render based on the active nav tab
        const activeTab = document.querySelector('.nav-tab.active');
        if (activeTab) {
            const view = activeTab.dataset.view;
            if (view === 'dashboard') {
                this.renderMySensors();
            }
        } else {
            // Default to Dashboard
            this.renderMySensors();
        }
    }

    refreshDashboard() {
        // Refresh dashboard data when location changes
        // renderMySensors will re-fetch data and update newDashboardLayout
        this.renderMySensors();
    }

    async renderOverview() {
        const sensorsGrid = document.getElementById('overviewGrid');
        const sensorCount = document.getElementById('overviewSensorCount');
        const searchInput = document.getElementById('deviceSearchInput');

        if (!sensorsGrid) {
            console.error('Overview grid element not found');
            return;
        }

        // Fetch all devices (not time-limited)
        if (!this.allDevices || this.allDevices.length === 0) {
            sensorsGrid.innerHTML = '<div class="loading">Loading all devices...</div>';
            try {
                const response = await fetch('/api/devices');
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                this.allDevices = data.devices || [];
            } catch (error) {
                console.error('Failed to fetch devices:', error);
                sensorsGrid.innerHTML = `<div class="loading">Failed to load devices: ${error.message}</div>`;
                return;
            }
        }

        // Setup search handler (only once)
        if (!this.overviewSearchSetup) {
            this.overviewSearchSetup = true;
            searchInput.addEventListener('input', () => this.filterOverviewDevices());
        }

        this.filterOverviewDevices();
    }

    filterOverviewDevices() {
        const sensorsGrid = document.getElementById('overviewGrid');
        const sensorCount = document.getElementById('overviewSensorCount');
        const searchInput = document.getElementById('deviceSearchInput');
        const searchTerm = (searchInput?.value || '').toLowerCase().trim();

        let devices = this.allDevices || [];

        // Apply search filter
        if (searchTerm) {
            devices = devices.filter(device => {
                const name = (device.name || '').toLowerCase();
                const deviceId = (device.deviceId || '').toLowerCase();
                const country = (device.geo_country || '').toLowerCase();
                const subdivision = (device.geo_subdivision || '').toLowerCase();
                const dataSource = (device.data_source || '').toLowerCase();
                return name.includes(searchTerm) ||
                       deviceId.includes(searchTerm) ||
                       country.includes(searchTerm) ||
                       subdivision.includes(searchTerm) ||
                       dataSource.includes(searchTerm);
            });
        }

        sensorCount.textContent = `${devices.length} device${devices.length !== 1 ? 's' : ''}${searchTerm ? ' found' : ' total'}`;

        if (devices.length === 0) {
            sensorsGrid.innerHTML = `<div class="loading">${searchTerm ? 'No devices match your search' : 'No devices found'}</div>`;
            return;
        }

        // Limit display to prevent performance issues
        const displayDevices = devices.slice(0, 100);
        const hasMore = devices.length > 100;

        sensorsGrid.innerHTML = displayDevices.map(device => this.createDeviceCard(device)).join('') +
            (hasMore ? `<div class="loading">Showing 100 of ${devices.length} devices. Use search to find specific devices.</div>` : '');

        // Attach star button event listeners
        this.attachStarListeners();
    }

    createDeviceCard(device) {
        const name = device.name || device.deviceId;
        const lastSeen = device.last_seen ? new Date(device.last_seen) : null;
        const lastSeenStr = lastSeen ? this.formatLastSeen(lastSeen) : 'Unknown';
        const isRecent = lastSeen && (Date.now() - lastSeen.getTime()) < 24 * 60 * 60 * 1000;

        const isFavorited = this.isFavorited(device.deviceId);
        const starIcon = `<svg width="16" height="16" viewBox="0 0 24 24" fill="${isFavorited ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>`;

        const locationStr = [device.geo_subdivision, device.geo_country]
            .filter(Boolean)
            .join(', ') || 'Unknown location';

        // Deployment type badge
        const deployType = (device.deployment_type || '').toUpperCase();
        const deployClass = deployType ? deployType.toLowerCase() : 'unknown';
        const deployLabel = deployType || 'UNKNOWN';
        const deployIcon = this.getDeploymentIcon(deployClass);

        // Source badge
        const sourceType = (device.deployment_type_source || '').toLowerCase();
        let sourceBadge = '';
        if (sourceType === 'manual') {
            sourceBadge = '<span class="source-badge manual" title="Manually configured"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg></span>';
        } else if (sourceType === 'inferred') {
            sourceBadge = '<span class="source-badge inferred" title="Auto-classified">?</span>';
        } else if (deployType) {
            sourceBadge = '<span class="source-badge unknown-source" title="Source unknown">~</span>';
        }

        // Warning badge
        let warningBadge = '';
        const warnings = this.checkDeploymentWarnings(device);
        if (warnings.length > 0) {
            const warningIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2L1 21h22L12 2zm0 3.99L19.53 19H4.47L12 5.99zM11 10v4h2v-4h-2zm0 6v2h2v-2h-2z"/></svg>';
            warningBadge = `<span class="warning-badge" title="${warnings.join('; ')}">${warningIcon}</span>`;
        }

        return `
            <div class="sensor-card ${!isRecent ? 'inactive' : ''}">
                <div class="card-header">
                    <div class="card-title">
                        <h3>${name}</h3>
                        <span class="device-id">${device.deviceId}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <button class="star-btn ${isFavorited ? 'favorited' : ''}" data-device-id="${device.deviceId}" title="${isFavorited ? 'Remove from favorites' : 'Add to favorites'}">${starIcon}</button>
                        <div class="card-status ${isRecent ? 'active' : 'inactive'}"></div>
                    </div>
                </div>
                <div class="sensor-badges" style="margin: 8px 0;">
                    <span class="deployment-badge ${deployClass}">${deployIcon}${deployLabel}</span>
                    ${sourceBadge}
                    ${warningBadge}
                </div>
                <div class="device-meta">
                    <div class="meta-item"><strong>Source:</strong> ${device.data_source || 'Unknown'}</div>
                    <div class="meta-item"><strong>Location:</strong> ${locationStr}</div>
                    <div class="meta-item"><strong>Board:</strong> ${device.board_model || 'Unknown'}</div>
                    <div class="meta-item"><strong>Readings:</strong> ${device.reading_count?.toLocaleString() || 0}</div>
                    ${device.node_info ? `<div class="meta-item"><strong>Setup:</strong> ${device.node_info}</div>` : ''}
                    ${device.node_info_url ? `<div class="meta-item"><a href="${device.node_info_url}" target="_blank" rel="noopener" style="color: #60a5fa;">View documentation</a></div>` : ''}
                </div>
                <div class="card-footer">
                    <div class="last-update ${!isRecent ? 'stale' : ''}">Last seen: ${lastSeenStr}</div>
                </div>
            </div>
        `;
    }

    formatLastSeen(date) {
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return 'Just now';
        if (diffMins < 60) return `${diffMins} min${diffMins !== 1 ? 's' : ''} ago`;
        if (diffHours < 24) return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
        if (diffDays < 7) return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
        return date.toLocaleDateString();
    }

    createSensorCard(sensor, showStar = false) {
        const readings = sensor.readings || {};
        const location = sensor.name || sensor.location || sensor.locationName || sensor.deviceId;
        const lastSeen = this.getLastSeenTime(sensor);
        const lastUpdate = lastSeen ? (() => { const d = new Date(lastSeen); return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0'); })() : 'Unknown';

        // Determine overall health icon
        const healthIcon = this.getHealthIcon(sensor);

        // Extract values from reading objects
        const temp = readings.temperature?.value;
        const humidity = readings.humidity?.value;
        const co2 = readings.co2?.value;
        const pm25 = readings['pm2.5']?.value;
        const voc = readings.voc_index?.value;
        const nox = readings.nox_index?.value;
        const pressure = readings.pressure?.value;

        // Check if this sensor is favorited
        const isFavorited = showStar && this.isFavorited(sensor.deviceId);
        const starIcon = `<svg width="16" height="16" viewBox="0 0 24 24" fill="${isFavorited ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>`;

        return `
            <div class="sensor-card">
                <div class="card-header">
                    <div class="card-title">
                        <h3>${location}</h3>
                        <span class="device-id">${sensor.deviceId}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        ${showStar ? `<button class="star-btn ${isFavorited ? 'favorited' : ''}" data-device-id="${sensor.deviceId}" title="${isFavorited ? 'Remove from favorites' : 'Add to favorites'}">${starIcon}</button>` : ''}
                        <div class="card-status">${healthIcon}</div>
                    </div>
                </div>

                <div class="readings-grid">
                    ${this.createReadingItem('Temperature', temp, '°C', 'temperature')}
                    ${this.createReadingItem('Humidity', humidity, '%', 'humidity')}
                    ${this.createReadingItem('CO₂', co2, 'ppm', 'co2')}
                    ${this.createReadingItem('PM2.5', pm25, 'µg/m³', 'pm2.5')}
                    ${this.createReadingItem('VOC', voc, 'index', 'voc')}
                    ${this.createReadingItem('NOx', nox, 'index', 'nox')}
                    ${this.createReadingItem('Pressure', pressure, 'hPa', 'pressure')}
                </div>

                <div class="card-footer">
                    <div class="calibration-status ${this.isFullyCalibrated(sensor) ? 'calibrated' : ''}">
                        ${this.getCalibrationStatus(sensor)}
                    </div>
                    <div class="last-update">${lastUpdate}</div>
                </div>
            </div>
        `;
    }

    createReadingItem(label, value, unit, type) {
        if (value === undefined || value === null) {
            return ''; // Don't show if no data
        }

        const formattedValue = typeof value === 'number' ?
            (type === 'co2' || type === 'voc' || type === 'nox' ? value.toFixed(0) : value.toFixed(1)) :
            value;

        const qualityBadge = this.getQualityBadge(type, value);

        return `
            <div class="reading-item">
                <div class="reading-label">${label}</div>
                <div class="reading-value">
                    ${formattedValue}<span class="reading-unit"> ${unit}</span>
                </div>
                ${qualityBadge}
            </div>
        `;
    }

    getQualityBadge(type, value) {
        const level = this.getAirQualityLevel(type, value);
        if (!level) return '';

        return `<div class="quality-badge ${level.class}">${level.text}</div>`;
    }

    getAirQualityLevel(type, value) {
        if (value === undefined || value === null) return null;

        switch(type) {
            case 'co2':
                if (value < 600) return { class: 'excellent', text: 'Excellent' };
                if (value < 800) return { class: 'good', text: 'Good' };
                if (value < 1000) return { class: 'moderate', text: 'Moderate' };
                if (value < 1500) return { class: 'poor', text: 'Poor' };
                return { class: 'unhealthy', text: 'Unhealthy' };

            case 'pm2.5':
                if (value < 12) return { class: 'excellent', text: 'Excellent' };
                if (value < 35) return { class: 'good', text: 'Good' };
                if (value < 55) return { class: 'moderate', text: 'Moderate' };
                if (value < 150) return { class: 'poor', text: 'Poor' };
                return { class: 'unhealthy', text: 'Unhealthy' };

            case 'voc':
            case 'nox':
                if (value < 100) return { class: 'excellent', text: 'Excellent' };
                if (value < 150) return { class: 'good', text: 'Good' };
                if (value < 200) return { class: 'moderate', text: 'Moderate' };
                if (value < 300) return { class: 'poor', text: 'Poor' };
                return { class: 'unhealthy', text: 'Unhealthy' };

            default:
                return null;
        }
    }

    getHealthIcon(sensor) {
        const readings = sensor.readings || {};

        // Check CO2 levels for overall health - return CSS class instead of emoji
        const co2 = readings.co2?.value;
        if (co2) {
            if (co2 < 600) return 'excellent';
            if (co2 < 1000) return 'good';
            if (co2 < 1500) return 'moderate';
            return 'poor';
        }

        // Check PM2.5 if no CO2
        const pm25 = readings['pm2.5']?.value;
        if (pm25) {
            if (pm25 < 12) return 'excellent';
            if (pm25 < 35) return 'good';
            if (pm25 < 55) return 'moderate';
            return 'poor';
        }

        return 'unknown';
    }

    getCalibrationStatus(sensor) {
        // Check if sensor has calibration info
        const calibration = sensor.calibration || {};

        if (calibration.status === 'calibrated' || this.isFullyCalibrated(sensor)) {
            return 'Calibrated';
        }

        if (calibration.status) {
            // Format calibration status
            const status = calibration.status.replace(/_/g, ' ');
            return status;
        }

        return 'Status unknown';
    }

    isFullyCalibrated(sensor) {
        // Simple check - can be expanded based on actual calibration data structure
        const calibration = sensor.calibration || {};
        return calibration.status === 'calibrated';
    }

    isSensorActive(sensor) {
        const lastSeen = this.getLastSeenTime(sensor);
        if (!lastSeen) return false;

        const now = new Date();
        const lastSeenDate = new Date(lastSeen);
        const diffMs = now - lastSeenDate;
        const timeRange = this.getTimeRangeMs(this.currentTimeRange);

        return diffMs <= timeRange;
    }

    // ============= FAVORITES MANAGEMENT =============

    getFavorites() {
        try {
            const favorites = localStorage.getItem('sensorFavorites');
            return favorites ? JSON.parse(favorites) : [];
        } catch (e) {
            console.error('Error loading favorites:', e);
            return [];
        }
    }

    saveFavorites(favorites) {
        try {
            localStorage.setItem('sensorFavorites', JSON.stringify(favorites));
        } catch (e) {
            console.error('Error saving favorites:', e);
        }
    }

    isFavorited(deviceId) {
        const favorites = this.getFavorites();
        return favorites.includes(deviceId);
    }

    toggleFavorite(deviceId) {
        let favorites = this.getFavorites();
        const index = favorites.indexOf(deviceId);

        if (index > -1) {
            favorites.splice(index, 1);
        } else {
            favorites.push(deviceId);
        }

        this.saveFavorites(favorites);
        return favorites.includes(deviceId);
    }

    attachStarListeners() {
        const starButtons = document.querySelectorAll('.star-btn');
        starButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const deviceId = btn.dataset.deviceId;
                const isFavorited = this.toggleFavorite(deviceId);

                // Update button appearance
                btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="${isFavorited ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>`;
                btn.classList.toggle('favorited', isFavorited);
                btn.title = isFavorited ? 'Remove from favorites' : 'Add to favorites';

                // Update My Sensors if currently viewing it
                const mySensorsView = document.getElementById('mySensorsView');
                if (mySensorsView && mySensorsView.classList.contains('active')) {
                    this.renderMySensors();
                }
            });
        });
    }

    // ============= MY SENSORS WIDGET DASHBOARD =============

    // SVG Icons for e-paper style dashboard
    getSvgIcons() {
        return {
            temperature: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M16 4v16m0 0a6 6 0 1 0 0 8 6 6 0 0 0 0-8z"/>
                <circle cx="16" cy="24" r="3" fill="currentColor"/>
                <path d="M12 4h8M12 8h8M12 12h8"/>
            </svg>`,
            humidity: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M16 4c-6 8-10 12-10 18a10 10 0 0 0 20 0c0-6-4-10-10-18z"/>
                <path d="M12 20a4 4 0 0 0 4 4" stroke-linecap="round"/>
            </svg>`,
            pressure: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="16" cy="16" r="12"/>
                <path d="M16 8v8l4 4"/>
                <path d="M8 16h2M22 16h2M16 8v2"/>
            </svg>`,
            co2: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <ellipse cx="16" cy="16" rx="10" ry="8"/>
                <path d="M10 16a3 3 0 0 1 3-3h0a3 3 0 0 1 3 3v0a3 3 0 0 1-3 3h0a3 3 0 0 1-3-3z"/>
                <circle cx="20" cy="13" r="1.5" fill="currentColor"/>
                <circle cx="22" cy="18" r="1" fill="currentColor"/>
            </svg>`,
            pm25: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="10" cy="12" r="3"/>
                <circle cx="20" cy="10" r="2"/>
                <circle cx="16" cy="20" r="4"/>
                <circle cx="24" cy="18" r="2"/>
                <circle cx="8" cy="22" r="2"/>
                <path d="M4 28h24" stroke-dasharray="2 2"/>
            </svg>`,
            voc: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 24V12l4 6 4-6v12"/>
                <circle cx="16" cy="8" r="3"/>
                <path d="M8 28h16"/>
            </svg>`,
            chart: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M4 24l6-8 6 4 8-12"/>
                <path d="M4 28h24"/>
                <path d="M4 4v24"/>
            </svg>`,
            status: `<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="4" y="8" width="24" height="16" rx="2"/>
                <circle cx="10" cy="16" r="2" fill="currentColor"/>
                <path d="M16 12v8M20 14v4"/>
            </svg>`
        };
    }

    initDashboardIcons() {
        const icons = this.getSvgIcons();
        const iconMappings = {
            'tempIcon': icons.temperature,
            'humidityIcon': icons.humidity,
            'pressureIcon': icons.pressure,
            'co2Icon': icons.co2,
            'pm25Icon': icons.pm25,
            'vocIcon': icons.voc,
            'chartIcon': icons.chart,
            'statusIcon': icons.status
        };

        Object.entries(iconMappings).forEach(([id, svg]) => {
            const element = document.getElementById(id);
            if (element) {
                element.innerHTML = svg;
            }
        });
    }

    // Update "Help your area" swarm prompt based on sensor swarm status
    updateSwarmPrompt(favoritedSensors) {
        const promptContainer = document.getElementById('swarmPrompt');
        if (!promptContainer) return;

        if (!favoritedSensors || favoritedSensors.length === 0) {
            promptContainer.style.display = 'none';
            return;
        }

        // Analyze swarm status of favorited sensors
        const swarmSizes = favoritedSensors.map(s => s.swarm_size || 0);
        const avgSwarmSize = Math.round(swarmSizes.reduce((a, b) => a + b, 0) / swarmSizes.length);
        const minSwarmSize = Math.min(...swarmSizes);
        const maxSwarmSize = Math.max(...swarmSizes);

        // Count sensors by status
        const statusCounts = { shield: 0, swarm: 0, super_swarm: 0 };
        favoritedSensors.forEach(s => {
            const status = s.swarm_status || 'shield';
            statusCounts[status]++;
        });

        // Determine the prompt message
        let promptHtml = '';
        let promptClass = '';

        if (maxSwarmSize >= 7) {
            // Super Swarm - show success message
            promptClass = 'swarm-prompt-success';
            promptHtml = `
                <span class="swarm-prompt-icon">⭐</span>
                <span class="swarm-prompt-text">
                    <strong>Super Swarm!</strong> Your area has ${maxSwarmSize} sensors - peer verification is highly reliable.
                </span>
            `;
        } else if (maxSwarmSize >= 5) {
            // Swarm - encourage growth
            const needed = 7 - maxSwarmSize;
            promptClass = 'swarm-prompt-info';
            promptHtml = `
                <span class="swarm-prompt-icon">🐝</span>
                <span class="swarm-prompt-text">
                    <strong>Peer Verified!</strong> Your area has ${maxSwarmSize} sensors. Add ${needed} more for Super Swarm status!
                </span>
            `;
        } else if (maxSwarmSize >= 3) {
            // Close to swarm - strong encouragement
            const needed = 5 - maxSwarmSize;
            promptClass = 'swarm-prompt-warning';
            promptHtml = `
                <span class="swarm-prompt-icon">🛡️</span>
                <span class="swarm-prompt-text">
                    <strong>Almost there!</strong> ${needed} more sensor${needed > 1 ? 's' : ''} needed in your ~10km area for peer verification.
                </span>
                <a href="#" class="swarm-prompt-link" onclick="window.open('https://github.com/wesense-earth', '_blank'); return false;">Learn more</a>
            `;
        } else if (maxSwarmSize >= 1) {
            // Solo sensor - education
            const needed = 5 - maxSwarmSize;
            promptClass = 'swarm-prompt-default';
            promptHtml = `
                <span class="swarm-prompt-icon">🛡️</span>
                <span class="swarm-prompt-text">
                    ${needed} more sensor${needed > 1 ? 's' : ''} needed within ~10km for peer verification - enables outlier detection.
                </span>
            `;
        } else {
            // No swarm data
            promptContainer.style.display = 'none';
            return;
        }

        promptContainer.className = `swarm-prompt ${promptClass}`;
        promptContainer.innerHTML = promptHtml;
        promptContainer.style.display = 'flex';
    }

    async renderMySensors() {
        const favorites = this.getFavorites();
        const loadingMsg = document.getElementById('dashboardLoading');
        const noFavoritesMsg = document.getElementById('noFavoritesMessage');
        const widgetDashboard = document.getElementById('widgetDashboard');

        // Initialize SVG icons
        this.initDashboardIcons();

        if (favorites.length === 0) {
            if (loadingMsg) loadingMsg.style.display = 'none';
            noFavoritesMsg.style.display = 'block';
            widgetDashboard.style.display = 'none';
            if (newDashboardLayout) newDashboardLayout.hide();
            // Show location selector in empty state if multiple locations exist
            if (locationManager) locationManager.updateEmptyStateLocationVisibility();
            return;
        }

        // Fetch all-time data for favorited sensors (no time limit)
        let favoritedSensors = [];
        try {
            // Use "all" to get all historical data without time limit
            const response = await fetch('/api/sensors?range=all');
            if (response.ok) {
                const data = await response.json();
                const allSensors = data.sensors || [];
                // Don't filter by isSensorActive - show all favorited sensors
                favoritedSensors = allSensors.filter(s => favorites.includes(s.deviceId));
            }
        } catch (error) {
            console.error('Failed to fetch sensor data:', error);
            // Fallback to cached data
            const allSensors = this.getFilteredSensors();
            favoritedSensors = allSensors.filter(s => favorites.includes(s.deviceId));
        }

        if (favoritedSensors.length === 0) {
            if (loadingMsg) loadingMsg.style.display = 'none';
            noFavoritesMsg.style.display = 'block';
            widgetDashboard.style.display = 'none';
            if (newDashboardLayout) newDashboardLayout.hide();
            // Show location selector in empty state if multiple locations exist
            if (locationManager) locationManager.updateEmptyStateLocationVisibility();
            return;
        }

        // Hide loading and no favorites messages
        if (loadingMsg) loadingMsg.style.display = 'none';
        noFavoritesMsg.style.display = 'none';

        // Calculate aggregates
        const aggregates = this.calculateAggregates(favoritedSensors);

        // Update "Help your area" swarm prompt
        this.updateSwarmPrompt(favoritedSensors);

        // Get outdoor-only device IDs for weather metrics and historical trends
        const outdoorDeviceIds = favoritedSensors
            .filter(s => {
                const deployType = (s.deployment_type || '').toLowerCase();
                return deployType === 'outdoor' || deployType === 'mixed';
            })
            .map(s => s.deviceId);

        // Update the new dashboard layout (if active)
        if (newDashboardLayout && newDashboardLayout.isActive) {
            newDashboardLayout.show(favorites, aggregates, outdoorDeviceIds);
            widgetDashboard.style.display = 'none'; // Hide legacy widgets
        } else {
            widgetDashboard.style.display = 'grid';

            // Update widgets with new e-paper style
            this.updateEpaperWidget('temp', aggregates.temperature, 'temperature');
            this.updateEpaperWidget('humidity', aggregates.humidity, 'humidity');
            this.updateEpaperWidget('pressure', aggregates.pressure, 'pressure');
            this.updateEpaperWidget('co2', aggregates.co2, 'co2', 'co2');
            this.updateEpaperWidget('pm25', aggregates.pm25, 'pm2_5', 'pm2.5');
            this.updateEpaperWidget('voc', aggregates.voc, 'voc_index', 'voc');

            // Render sparklines (always shows last 24h of data)
            this.renderSparklines(favoritedSensors);

            // Update trend charts - use selected time range, not main dashboard data
            this.fetchAndUpdateTrends();

            // Update sensor status grid
            this.updateEpaperSensorStatus(favoritedSensors);

            // Fetch and display comparison badges ("vs yesterday")
            this.fetchAndUpdateComparisonBadges(favorites);
        }
    }

    updateEpaperWidget(widgetId, data, readingType, qualityType = null) {
        const avgElement = document.getElementById(`avg${widgetId.charAt(0).toUpperCase() + widgetId.slice(1)}`);
        const highElement = document.getElementById(`high${widgetId.charAt(0).toUpperCase() + widgetId.slice(1)}`);
        const lowElement = document.getElementById(`low${widgetId.charAt(0).toUpperCase() + widgetId.slice(1)}`);
        const countElement = document.getElementById(`${widgetId}SensorCount`);
        const qualityElement = document.getElementById(`${widgetId}Quality`);
        const trendElement = document.getElementById(`${widgetId}Trend`);

        if (data.avg != null) {
            if (avgElement) avgElement.textContent = this.formatValue(data.avg, readingType);
            if (highElement) highElement.textContent = this.formatValue(data.high, readingType);
            if (lowElement) lowElement.textContent = this.formatValue(data.low, readingType);
            if (countElement) countElement.textContent = `${data.count} sensor${data.count !== 1 ? 's' : ''}`;

            // Update quality indicator for air quality metrics (e-paper style)
            if (qualityElement && qualityType) {
                const quality = this.getAirQualityLevel(qualityType, data.avg);
                if (quality) {
                    qualityElement.className = `epaper-quality ${quality.class}`;
                    qualityElement.innerHTML = `<span class="epaper-quality-dot"></span><span>${quality.text}</span>`;
                } else {
                    qualityElement.className = 'epaper-quality';
                    qualityElement.innerHTML = '<span>--</span>';
                }
            }

            // Update pressure trend indicator
            if (trendElement && widgetId === 'pressure') {
                // For now show steady - in future, calculate from sparkline data
                const trend = this.calculateTrend(data.values || []);
                trendElement.className = `epaper-trend ${trend.class}`;
                trendElement.innerHTML = `<span class="epaper-trend-arrow">${trend.arrow}</span><span>${trend.text}</span>`;
            }
        } else {
            if (avgElement) avgElement.textContent = '--';
            if (highElement) highElement.textContent = '--';
            if (lowElement) lowElement.textContent = '--';
            if (countElement) countElement.textContent = '0 sensors';
            if (qualityElement) {
                qualityElement.className = 'epaper-quality';
                qualityElement.innerHTML = '<span>--</span>';
            }
        }
    }

    calculateTrend(values) {
        if (!values || values.length < 2) {
            return { class: 'steady', arrow: '→', text: 'Steady' };
        }
        const recent = values.slice(-3);
        const earlier = values.slice(0, 3);
        const recentAvg = recent.reduce((a, b) => a + b, 0) / recent.length;
        const earlierAvg = earlier.reduce((a, b) => a + b, 0) / earlier.length;
        const diff = recentAvg - earlierAvg;

        if (diff > 2) return { class: 'rising', arrow: '↑', text: 'Rising' };
        if (diff < -2) return { class: 'falling', arrow: '↓', text: 'Falling' };
        return { class: 'steady', arrow: '→', text: 'Steady' };
    }

    async fetchAndUpdateComparisonBadges(deviceIds) {
        if (!deviceIds || deviceIds.length === 0) return;

        try {
            const response = await fetch(`/api/comparison?devices=${deviceIds.join(',')}`);
            if (!response.ok) return;

            const comparison = await response.json();

            // Update each metric's comparison badge
            const metricMap = {
                temperature: { id: 'tempComparison', unit: '°C', decimals: 1 },
                humidity: { id: 'humidityComparison', unit: '%', decimals: 1 },
                pressure: { id: 'pressureComparison', unit: ' hPa', decimals: 2 },
                co2: { id: 'co2Comparison', unit: ' ppm', decimals: 0 },
                pm2_5: { id: 'pm25Comparison', unit: '', decimals: 1 }
            };

            for (const [metric, config] of Object.entries(metricMap)) {
                const badge = document.getElementById(config.id);
                if (!badge) continue;

                const data = comparison[metric];
                if (!data || data.yesterdayDiff === null) {
                    badge.textContent = '--';
                    badge.className = 'comparison-badge neutral';
                    continue;
                }

                const diff = data.yesterdayDiff;
                const sign = diff >= 0 ? '+' : '';
                const arrow = diff >= 0 ? '↑' : '↓';
                const value = diff.toFixed(config.decimals);

                badge.textContent = `${sign}${value}${config.unit} ${arrow}`;

                // For most metrics, positive change is neutral/informational
                // For air quality (pm2_5, co2), negative is better
                let badgeClass = 'neutral';
                if (metric === 'pm2_5' || metric === 'co2') {
                    badgeClass = diff > 0 ? 'negative' : (diff < 0 ? 'positive' : 'neutral');
                } else if (metric === 'temperature') {
                    // Temperature: just show direction, neutral color
                    badgeClass = Math.abs(diff) > 2 ? (diff > 0 ? 'positive' : 'negative') : 'neutral';
                } else {
                    badgeClass = 'neutral';
                }

                badge.className = `comparison-badge ${badgeClass}`;
            }

        } catch (error) {
            console.error('Failed to fetch comparison data:', error);
        }
    }

    renderSparklines(sensors) {
        const sparklineConfigs = [
            { id: 'tempSparkline', type: 'temperature', color: '#e74c3c' },
            { id: 'humiditySparkline', type: 'humidity', color: '#3498db' },
            { id: 'pressureSparkline', type: 'pressure', color: '#9b59b6' },
            { id: 'co2Sparkline', type: 'co2', color: '#27ae60' },
            { id: 'pm25Sparkline', type: 'pm2_5', color: '#e67e22' },
            { id: 'vocSparkline', type: 'voc', color: '#1abc9c' }
        ];

        // Sparkline timeframe labels are fixed at "Last 24 hours" in HTML
        // (preview sparklines always show 24h of data regardless of main time filter)

        // Weather/outdoor metrics should only use outdoor sensors
        const outdoorOnlyMetrics = ['temperature', 'humidity', 'pressure', 'pm2_5'];

        sparklineConfigs.forEach(config => {
            const canvas = document.getElementById(config.id);
            if (!canvas) return;

            const ctx = canvas.getContext('2d');
            const values = [];

            // Collect sparkline data from sensors
            sensors.forEach(sensor => {
                // For weather metrics, only use outdoor sensors
                if (outdoorOnlyMetrics.includes(config.type)) {
                    const deployType = (sensor.deployment_type || '').toLowerCase();
                    const isOutdoor = deployType === 'outdoor' || deployType === 'mixed';
                    if (!isOutdoor) return;
                }

                const reading = sensor.readings?.[config.type];
                if (reading?.sparklineData) {
                    // Use existing sparkline data if available
                    values.push(...reading.sparklineData);
                } else if (reading?.value != null) {
                    values.push(reading.value);
                }
            });

            this.drawSparkline(ctx, canvas, values, config.color);
        });
    }

    formatTimeRangeLabel(timeRange) {
        if (!timeRange) return 'Last 24 hours';
        const match = timeRange.match(/^(\d+)([hdwmy])$/);
        if (!match) return 'Last 24 hours';

        const value = parseInt(match[1]);
        const unit = match[2];

        const unitNames = {
            h: value === 1 ? 'hour' : 'hours',
            d: value === 1 ? 'day' : 'days',
            w: value === 1 ? 'week' : 'weeks',
            m: value === 1 ? 'month' : 'months',
            y: value === 1 ? 'year' : 'years'
        };

        return `Last ${value} ${unitNames[unit] || 'hours'}`;
    }

    drawSparkline(ctx, canvas, values, color) {
        // Set canvas size for high DPI
        const rect = canvas.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);

        const width = rect.width;
        const height = rect.height;

        ctx.clearRect(0, 0, width, height);

        if (!values || values.length < 2) {
            // Draw placeholder line
            ctx.strokeStyle = '#ddd';
            ctx.lineWidth = 1;
            ctx.setLineDash([2, 2]);
            ctx.beginPath();
            ctx.moveTo(0, height / 2);
            ctx.lineTo(width, height / 2);
            ctx.stroke();
            return;
        }

        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const padding = 2;

        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.setLineDash([]);
        ctx.beginPath();

        values.forEach((value, i) => {
            const x = (i / (values.length - 1)) * width;
            const y = height - padding - ((value - min) / range) * (height - padding * 2);

            if (i === 0) {
                ctx.moveTo(x, y);
            } else {
                ctx.lineTo(x, y);
            }
        });

        ctx.stroke();

        // Draw fill
        ctx.lineTo(width, height);
        ctx.lineTo(0, height);
        ctx.closePath();
        ctx.fillStyle = color + '20';
        ctx.fill();
    }

    updateTrendCharts(sensors, animate = false) {
        // Initialize trend charts storage if needed
        if (!this.trendCharts) {
            this.trendCharts = {};
        }

        const metrics = [
            { id: 'Temp', key: 'temperature', color: '#e74c3c', unit: '°C' },
            { id: 'Humidity', key: 'humidity', color: '#3498db', unit: '%' },
            { id: 'Pressure', key: 'pressure', color: '#9b59b6', unit: 'hPa' },
            { id: 'Co2', key: 'co2', color: '#e67e22', unit: 'ppm' },
            { id: 'Pm25', key: 'pm2_5', color: '#34495e', unit: 'µg/m³' },
            { id: 'Voc', key: 'voc_index', color: '#27ae60', unit: 'idx' }
        ];

        const timeRange = this.selectedTrendTimeRange || '24h';
        const timeRangeHours = this.getTimeRangeHours(timeRange);

        console.log('[TrendCharts] Updating for timeRange:', timeRange, 'hours:', timeRangeHours, 'sensors:', sensors.length);

        // Update timeframe labels on all chart titles
        const timeframeLabels = { '24h': '24h', '7d': '7d', '30d': '30d', '365d': '1y' };
        const timeframeText = timeframeLabels[timeRange] || timeRange;
        document.querySelectorAll('.trend-timeframe').forEach(el => {
            el.textContent = `(${timeframeText})`;
        });

        // Weather/outdoor metrics should only use outdoor sensors
        const outdoorOnlyMetrics = ['temperature', 'humidity', 'pressure', 'pm2_5'];

        metrics.forEach(metric => {
            const canvas = document.getElementById(`trendChart${metric.id}`);
            if (!canvas) return;

            const ctx = canvas.getContext('2d');

            // Destroy existing chart
            if (this.trendCharts[metric.id]) {
                this.trendCharts[metric.id].destroy();
            }

            // Filter sensors for weather metrics (outdoor only)
            let filteredSensors = sensors;
            if (outdoorOnlyMetrics.includes(metric.key)) {
                filteredSensors = sensors.filter(sensor => {
                    const deployType = (sensor.deployment_type || '').toLowerCase();
                    return deployType === 'outdoor' || deployType === 'mixed';
                });
            }

            // Collect sparkline data for this metric from filtered sensors
            const datasets = [];
            const sensorColors = [
                { border: metric.color, bg: metric.color + '20' },
                { border: this.adjustColor(metric.color, 30), bg: this.adjustColor(metric.color, 30) + '20' },
                { border: this.adjustColor(metric.color, -30), bg: this.adjustColor(metric.color, -30) + '20' }
            ];

            let maxPoints = 0;
            filteredSensors.forEach((sensor, index) => {
                const sparklineData = sensor.readings?.[metric.key]?.sparklineData;
                if (sparklineData && sparklineData.length > 0) {
                    const name = sensor.name || sensor.location || sensor.deviceId || 'Unknown';
                    const shortName = name.length > 12 ? name.substring(0, 12) + '..' : name;
                    const color = sensorColors[index % sensorColors.length];

                    datasets.push({
                        label: shortName,
                        data: sparklineData,
                        borderColor: color.border,
                        backgroundColor: color.bg,
                        borderWidth: 1.5,
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHoverRadius: 3
                    });

                    maxPoints = Math.max(maxPoints, sparklineData.length);
                }
            });

            // Generate time labels
            const labels = this.generateTimeLabels(maxPoints, timeRangeHours);

            console.log(`[TrendCharts] ${metric.key}: filteredSensors=${filteredSensors.length}, datasets=${datasets.length}, maxPoints=${maxPoints}, labels=${labels.length}`);

            // If no data, show placeholder
            if (datasets.length === 0) {
                canvas.width = canvas.offsetWidth * (window.devicePixelRatio || 1);
                canvas.height = canvas.offsetHeight * (window.devicePixelRatio || 1);
                ctx.scale(window.devicePixelRatio || 1, window.devicePixelRatio || 1);
                ctx.font = '11px sans-serif';
                ctx.fillStyle = '#999';
                ctx.textAlign = 'center';
                ctx.fillText('No data', canvas.offsetWidth / 2, canvas.offsetHeight / 2);
                return;
            }

            this.trendCharts[metric.id] = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: animate ? { duration: 400 } : false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: {
                            display: datasets.length > 1,
                            position: 'bottom',
                            labels: { boxWidth: 8, padding: 4, font: { size: 9 } }
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            grid: { display: false },
                            ticks: { font: { size: 8 }, maxTicksLimit: 4, maxRotation: 0 }
                        },
                        y: {
                            display: true,
                            grid: { color: 'rgba(0,0,0,0.05)' },
                            ticks: { font: { size: 8 }, maxTicksLimit: 4 }
                        }
                    }
                }
            });
        });
    }

    getTimeRangeHours(timeRange) {
        const match = timeRange.match(/^(\d+)([hdwmy])$/);
        if (!match) return 24;
        const value = parseInt(match[1]);
        const unit = match[2];
        const multipliers = { h: 1, d: 24, w: 168, m: 720, y: 8760 };
        return value * (multipliers[unit] || 24);
    }

    generateTimeLabels(numPoints, totalHours) {
        const labels = [];
        if (numPoints === 0) return labels;

        const hoursPerPoint = totalHours / numPoints;
        for (let i = 0; i < numPoints; i++) {
            const hoursAgo = Math.round((numPoints - 1 - i) * hoursPerPoint);
            if (hoursAgo === 0) {
                labels.push('Now');
            } else if (hoursAgo < 24) {
                labels.push(`${hoursAgo}h`);
            } else if (hoursAgo < 168) {
                labels.push(`${Math.round(hoursAgo / 24)}d`);
            } else if (hoursAgo < 720) {
                labels.push(`${Math.round(hoursAgo / 168)}w`);
            } else {
                labels.push(`${Math.round(hoursAgo / 720)}mo`);
            }
        }
        return labels;
    }

    adjustColor(hex, amount) {
        // Lighten or darken a hex color
        let color = hex.replace('#', '');
        if (color.length === 3) {
            color = color[0] + color[0] + color[1] + color[1] + color[2] + color[2];
        }
        const num = parseInt(color, 16);
        let r = Math.min(255, Math.max(0, (num >> 16) + amount));
        let g = Math.min(255, Math.max(0, ((num >> 8) & 0x00FF) + amount));
        let b = Math.min(255, Math.max(0, (num & 0x0000FF) + amount));
        return '#' + (0x1000000 + (r << 16) + (g << 8) + b).toString(16).slice(1);
    }

    setupTrendTimeframeSelector() {
        // This is for the dashboard trend charts - separate from the main sidebar time filter
        const buttons = document.querySelectorAll('#trendTimeframeSelector .timeframe-btn');
        buttons.forEach(btn => {
            btn.addEventListener('click', () => {
                buttons.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.selectedTrendTimeRange = btn.dataset.range;
                // Re-fetch data with new time range and update charts (with animation for user action)
                this.fetchAndUpdateTrends(true);
            });
        });
    }

    async fetchAndUpdateTrends(animate = false) {
        // Prevent concurrent fetches that cause double animations
        if (this._trendFetchInProgress) {
            return;
        }
        this._trendFetchInProgress = true;

        const timeRange = this.selectedTrendTimeRange || '24h';
        try {
            console.log('[TrendCharts] Fetching data for range:', timeRange);
            const response = await fetch(`/api/sensors/average?range=${timeRange}`);
            if (response.ok) {
                const data = await response.json();
                // Filter to favorited sensors
                const favorites = this.getFavorites();
                console.log('[TrendCharts] API returned', Object.keys(data).length, 'devices, favorites:', favorites.length);
                const favoritedSensors = Object.entries(data)
                    .filter(([id]) => favorites.includes(id))
                    .map(([id, sensor]) => ({ deviceId: id, ...sensor }));
                console.log('[TrendCharts] Favorited sensors with data:', favoritedSensors.length);
                if (favoritedSensors.length > 0) {
                    const sample = favoritedSensors[0];
                    console.log('[TrendCharts] Sample sensor:', sample.deviceId, 'has temperature sparkline:', sample.readings?.temperature?.sparklineData?.length || 0);
                }
                this.updateTrendCharts(favoritedSensors, animate);
            } else {
                console.error('[TrendCharts] API response not OK:', response.status);
            }
        } catch (error) {
            console.error('Failed to fetch trend data:', error);
        } finally {
            this._trendFetchInProgress = false;
        }
    }

    updateEpaperSensorStatus(sensors) {
        const statusGrid = document.getElementById('sensorStatusGrid');
        if (!statusGrid) return;

        statusGrid.innerHTML = sensors.map(sensor => {
            const name = sensor.name || sensor.location || sensor.locationName || 'Unknown';
            const location = sensor.geo_subdivision || sensor.geo_country || '';
            const temp = sensor.readings?.temperature?.value;
            const tempStr = temp != null ? `${this.formatValue(temp, 'temperature')}°C` : '--';

            // Determine status based on last reading time
            let statusClass = 'online';
            const lastReading = sensor.readings?.temperature?.timestamp;
            if (lastReading) {
                const age = Date.now() - new Date(lastReading).getTime();
                if (age > 3600000) statusClass = 'stale'; // > 1 hour
                if (age > 86400000) statusClass = 'offline'; // > 24 hours
            }

            // Deployment type badge
            const deployType = (sensor.deployment_type || '').toUpperCase();
            const deployClass = deployType ? deployType.toLowerCase() : 'unknown';
            const deployLabel = deployType || 'UNKNOWN';
            const deployIcon = this.getDeploymentIcon(deployClass);

            // Source badge (manual = trusted, inferred = classifier, unknown = not set)
            const sourceType = (sensor.deployment_type_source || '').toLowerCase();
            let sourceBadge = '';
            if (sourceType === 'manual') {
                sourceBadge = '<span class="source-badge manual" title="Manually configured"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg></span>';
            } else if (sourceType === 'inferred') {
                sourceBadge = '<span class="source-badge inferred" title="Auto-classified">?</span>';
            } else if (deployType) {
                sourceBadge = '<span class="source-badge unknown-source" title="Source unknown">~</span>';
            }

            // Warning badge for potential misclassification
            let warningBadge = '';
            const warnings = this.checkDeploymentWarnings(sensor);
            if (warnings.length > 0) {
                const warningIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2L1 21h22L12 2zm0 3.99L19.53 19H4.47L12 5.99zM11 10v4h2v-4h-2zm0 6v2h2v-2h-2z"/></svg>';
                warningBadge = `<span class="warning-badge" title="${warnings.join('; ')}">${warningIcon}</span>`;
            }

            return `
                <div class="epaper-status-card">
                    <div class="epaper-status-info">
                        <div class="epaper-status-name">${name}</div>
                        <div class="epaper-status-location">${location}</div>
                        <div class="sensor-badges">
                            <span class="deployment-badge ${deployClass}">${deployIcon}${deployLabel}</span>
                            ${sourceBadge}
                            ${warningBadge}
                        </div>
                    </div>
                    <div class="epaper-status-value">${tempStr}</div>
                    <div class="epaper-status-indicator ${statusClass}"></div>
                </div>
            `;
        }).join('');
    }

    getDeploymentIcon(type) {
        const icons = {
            indoor: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/></svg>',
            outdoor: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M14 6l-3.75 5 2.85 3.8-1.6 1.2C9.81 13.75 7 10 7 10l-6 8h22L14 6z"/></svg>',
            mixed: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/></svg>',
            unknown: '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 17h-2v-2h2v2zm2.07-7.75l-.9.92C13.45 12.9 13 13.5 13 15h-2v-.5c0-1.1.45-2.1 1.17-2.83l1.24-1.26c.37-.36.59-.86.59-1.41 0-1.1-.9-2-2-2s-2 .9-2 2H8c0-2.21 1.79-4 4-4s4 1.79 4 4c0 .88-.36 1.68-.93 2.25z"/></svg>'
        };
        return icons[type] || icons.unknown;
    }

    checkDeploymentWarnings(sensor) {
        const warnings = [];
        const deployType = (sensor.deployment_type || '').toLowerCase();
        const sourceType = (sensor.deployment_type_source || '').toLowerCase();
        const readings = sensor.readings || {};

        // Check for potential misclassification issues
        if (sourceType === 'inferred') {
            // Indoor sensor with high UV readings (unlikely indoors)
            if (deployType === 'indoor' && readings.uv_index?.value > 2) {
                warnings.push('High UV detected - may be outdoor sensor');
            }

            // Outdoor sensor with very stable temperature (might be indoor)
            // This would require historical data to check variance

            // Indoor sensor with high wind readings
            if (deployType === 'indoor' && readings.wind_speed?.value > 5) {
                warnings.push('Wind detected - may be outdoor sensor');
            }
        }

        // CO2 sanity checks
        if (readings.co2?.value != null) {
            if (deployType === 'outdoor' && readings.co2.value > 800) {
                warnings.push('High CO2 for outdoor - check sensor placement');
            }
            if (deployType === 'indoor' && readings.co2.value < 350) {
                warnings.push('Low CO2 for indoor - may be outdoor sensor');
            }
        }

        // Temperature extreme checks
        if (readings.temperature?.value != null) {
            const temp = readings.temperature.value;
            if (deployType === 'indoor' && (temp < 5 || temp > 40)) {
                warnings.push('Extreme temp for indoor - check classification');
            }
        }

        return warnings;
    }

    calculateAggregates(sensors) {
        // Helper to check if a reading is fresh
        // WESENSE sensors report every 5 minutes (10 min threshold)
        // Meshtastic sensors report every 30-60 minutes (61 min threshold)
        const now = Date.now();
        const isReadingFresh = (reading, dataSource) => {
            if (!reading?.timestamp) return false;
            const threshold = getFreshnessThreshold(dataSource);
            const readingTime = new Date(reading.timestamp).getTime();
            return (now - readingTime) < threshold;
        };

        const aggregates = {
            temperature: { values: [], avg: null, high: null, low: null, min: null, max: null, count: 0 },
            humidity: { values: [], avg: null, high: null, low: null, count: 0 },
            pressure: { values: [], avg: null, high: null, low: null, count: 0, trend: null },
            co2: { values: [], avg: null, count: 0 },
            pm25: { values: [], avg: null, count: 0 },
            voc: { values: [], avg: null, count: 0 },
            // Indoor sensor metrics (all metrics from indoor sensors)
            indoor: {
                co2: { values: [], avg: null, count: 0, sensorModels: [] },
                voc: { values: [], avg: null, count: 0, sensorModels: [] },
                pm1: { values: [], avg: null, count: 0, sensorModels: [] },
                pm25: { values: [], avg: null, count: 0, sensorModels: [] },
                pm10: { values: [], avg: null, count: 0, sensorModels: [] },
                nox: { values: [], avg: null, count: 0, sensorModels: [] },
                temperature: { values: [], avg: null, count: 0 },
                humidity: { values: [], avg: null, count: 0 },
                pressure: { values: [], avg: null, count: 0 }
            },
            // Outdoor sensor metrics (all metrics from outdoor/mixed sensors)
            outdoor: {
                pm25: { values: [], avg: null, count: 0, sensorModels: [] },
                pm10: { values: [], avg: null, count: 0, sensorModels: [] },
                nox: { values: [], avg: null, count: 0, sensorModels: [] },
                co2: { values: [], avg: null, count: 0, sensorModels: [] },
                voc: { values: [], avg: null, count: 0, sensorModels: [] },
                pm1: { values: [], avg: null, count: 0, sensorModels: [] }
            },
            // Per-room indoor data (keyed by locality/room name)
            rooms: {},
            // Outdoor sensors (keyed by sensor id) - legacy for individual sensor access
            outdoorSensors: {},
            // Outdoor areas (keyed by area name) - grouped like indoor rooms
            outdoorAreas: {}
        };

        // Debug: track outdoor sensor detection
        let outdoorCount = 0;
        let freshOutdoorCount = 0;

        sensors.forEach(sensor => {
            const readings = sensor.readings || {};
            const deployType = (sensor.deployment_type || '').toLowerCase();
            const isIndoor = deployType === 'indoor';
            const isOutdoor = deployType === 'outdoor' || deployType === 'mixed';

            // Per-sensor outdoor data collection
            if (isOutdoor) {
                outdoorCount++;
                const sensorName = sensor.name || sensor.locality || sensor.deviceId || 'Unknown Sensor';
                const sensorId = sensor.deviceId;

                // Initialize sensor if not exists
                if (!aggregates.outdoorSensors[sensorId]) {
                    aggregates.outdoorSensors[sensorId] = {
                        name: sensorName,
                        deviceId: sensorId,
                        temperature: null,
                        humidity: null,
                        pm1: null,
                        pm25: null,
                        pm10: null,
                        co2: null,
                        voc: null,
                        boardModel: sensor.board_model || null,
                        sensorModels: {},  // Per-reading-type sensor models
                        lastUpdate: null,      // Most recent FRESH reading timestamp
                        lastSeenTimestamp: null, // Most recent ANY reading timestamp (for removal logic)
                        data_source: sensor.data_source || null // For freshness threshold
                    };
                }

                const outdoorSensor = aggregates.outdoorSensors[sensorId];

                // Track lastSeenTimestamp from any reading (even stale) for sensor removal logic
                const readingTypes = ['temperature', 'humidity', 'pm1_0', 'pm2_5', 'pm10', 'co2', 'voc_index', 'pressure'];
                for (const type of readingTypes) {
                    const reading = readings[type];
                    if (reading?.timestamp) {
                        const ts = new Date(reading.timestamp).getTime();
                        const currentLastSeen = outdoorSensor.lastSeenTimestamp ? new Date(outdoorSensor.lastSeenTimestamp).getTime() : 0;
                        if (ts > currentLastSeen) {
                            outdoorSensor.lastSeenTimestamp = reading.timestamp;
                        }
                    }
                }

                // Collect outdoor sensor readings - only if fresh (threshold depends on data_source)
                const dataSource = sensor.data_source;
                let hasFreshReading = false;
                if (readings.temperature?.value != null && isReadingFresh(readings.temperature, dataSource)) {
                    outdoorSensor.temperature = readings.temperature.value;
                    outdoorSensor.lastUpdate = readings.temperature.timestamp;
                    if (readings.temperature.sensor_model) outdoorSensor.sensorModels.temperature = readings.temperature.sensor_model;
                    hasFreshReading = true;
                }
                if (readings.humidity?.value != null && isReadingFresh(readings.humidity, dataSource)) {
                    outdoorSensor.humidity = readings.humidity.value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings.humidity.timestamp;
                    if (readings.humidity.sensor_model) outdoorSensor.sensorModels.humidity = readings.humidity.sensor_model;
                    hasFreshReading = true;
                }
                if (readings['pm2_5']?.value != null && isReadingFresh(readings['pm2_5'], dataSource)) {
                    outdoorSensor.pm25 = readings['pm2_5'].value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings['pm2_5'].timestamp;
                    if (readings['pm2_5'].sensor_model) outdoorSensor.sensorModels.pm25 = readings['pm2_5'].sensor_model;
                    hasFreshReading = true;
                }
                if (readings.co2?.value != null && isReadingFresh(readings.co2, dataSource)) {
                    outdoorSensor.co2 = readings.co2.value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings.co2.timestamp;
                    if (readings.co2.sensor_model) outdoorSensor.sensorModels.co2 = readings.co2.sensor_model;
                    hasFreshReading = true;
                }
                if (readings.voc_index?.value != null && isReadingFresh(readings.voc_index, dataSource)) {
                    outdoorSensor.voc = readings.voc_index.value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings.voc_index.timestamp;
                    if (readings.voc_index.sensor_model) outdoorSensor.sensorModels.voc = readings.voc_index.sensor_model;
                    hasFreshReading = true;
                }
                if (readings['pm1_0']?.value != null && isReadingFresh(readings['pm1_0'], dataSource)) {
                    outdoorSensor.pm1 = readings['pm1_0'].value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings['pm1_0'].timestamp;
                    if (readings['pm1_0'].sensor_model) outdoorSensor.sensorModels.pm1 = readings['pm1_0'].sensor_model;
                    hasFreshReading = true;
                }
                if (readings.pm10?.value != null && isReadingFresh(readings.pm10, dataSource)) {
                    outdoorSensor.pm10 = readings.pm10.value;
                    if (!outdoorSensor.lastUpdate) outdoorSensor.lastUpdate = readings.pm10.timestamp;
                    if (readings.pm10.sensor_model) outdoorSensor.sensorModels.pm10 = readings.pm10.sensor_model;
                    hasFreshReading = true;
                }
                if (hasFreshReading) freshOutdoorCount++;

                // Debug: log stale outdoor sensor details
                if (!hasFreshReading && readings.temperature?.timestamp) {
                    const ageMs = now - new Date(readings.temperature.timestamp).getTime();
                    const threshold = getFreshnessThreshold(dataSource);
                    console.log(`[Aggregates] Outdoor sensor ${sensorId} stale: temp reading ${Math.round(ageMs/1000)}s old (threshold: ${threshold/1000}s, source: ${dataSource || 'unknown'})`);
                }

                // Also group outdoor sensors by area (like indoor rooms)
                const areaName = sensor.locality || sensor.name || sensor.deviceId || 'Unknown Area';

                // Initialize area if not exists
                if (!aggregates.outdoorAreas[areaName]) {
                    aggregates.outdoorAreas[areaName] = {
                        // Aggregated values (will be averaged from all sensors)
                        temperature: null,
                        humidity: null,
                        pm1: null,
                        pm25: null,
                        pm10: null,
                        co2: null,
                        voc: null,
                        // Individual sensors in this area (keyed by device ID)
                        sensors: {},
                        // Area metadata
                        areaType: detectAreaType(areaName),
                        lastUpdate: null,
                        lastSeenTimestamp: null,
                        data_source: null
                    };
                }

                const area = aggregates.outdoorAreas[areaName];

                // Initialize this sensor within the area if not exists
                if (!area.sensors[sensorId]) {
                    area.sensors[sensorId] = {
                        name: sensorName,
                        deviceId: sensorId,
                        temperature: null,
                        humidity: null,
                        pm1: null,
                        pm25: null,
                        pm10: null,
                        co2: null,
                        voc: null,
                        boardModel: sensor.board_model || null,
                        sensorModels: {},
                        lastUpdate: null,
                        lastSeenTimestamp: null,
                        data_source: dataSource
                    };
                }

                const areaSensor = area.sensors[sensorId];

                // Copy data from the individual outdoor sensor
                areaSensor.temperature = outdoorSensor.temperature;
                areaSensor.humidity = outdoorSensor.humidity;
                areaSensor.pm1 = outdoorSensor.pm1;
                areaSensor.pm25 = outdoorSensor.pm25;
                areaSensor.pm10 = outdoorSensor.pm10;
                areaSensor.co2 = outdoorSensor.co2;
                areaSensor.voc = outdoorSensor.voc;
                areaSensor.boardModel = outdoorSensor.boardModel;
                areaSensor.sensorModels = { ...outdoorSensor.sensorModels };
                areaSensor.lastUpdate = outdoorSensor.lastUpdate;
                areaSensor.lastSeenTimestamp = outdoorSensor.lastSeenTimestamp;

                // Update area timestamps
                if (outdoorSensor.lastSeenTimestamp) {
                    const ts = new Date(outdoorSensor.lastSeenTimestamp).getTime();
                    const areaLastSeen = area.lastSeenTimestamp ? new Date(area.lastSeenTimestamp).getTime() : 0;
                    if (ts > areaLastSeen) {
                        area.lastSeenTimestamp = outdoorSensor.lastSeenTimestamp;
                    }
                }
                if (outdoorSensor.lastUpdate) {
                    const ts = new Date(outdoorSensor.lastUpdate).getTime();
                    const areaLastUpdate = area.lastUpdate ? new Date(area.lastUpdate).getTime() : 0;
                    if (ts > areaLastUpdate) {
                        area.lastUpdate = outdoorSensor.lastUpdate;
                        area.data_source = dataSource;
                    }
                }
            }

            // Per-room indoor data collection (supports multiple sensors per room)
            if (isIndoor) {
                const roomName = sensor.locality || sensor.name || sensor.deviceId || 'Unknown Room';
                const sensorId = sensor.deviceId;
                const sensorName = sensor.name || sensor.deviceId || 'Unknown Sensor';

                // Initialize room if not exists
                if (!aggregates.rooms[roomName]) {
                    aggregates.rooms[roomName] = {
                        // Aggregated values (will be averaged from all sensors)
                        temperature: null,
                        humidity: null,
                        co2: null,
                        pressure: null,
                        voc: null,
                        pm1: null,
                        pm25: null,
                        pm10: null,
                        // Individual sensors in this room (keyed by device ID)
                        sensors: {},
                        // Room metadata
                        roomType: null,           // Auto-detected from first sensor name
                        lastUpdate: null,         // Most recent FRESH reading timestamp
                        lastSeenTimestamp: null,  // Most recent ANY reading timestamp (for removal logic)
                        // Legacy fields for backward compatibility (will use first sensor)
                        sensorId: null,
                        boardModel: null,
                        sensorModels: {},
                        data_source: null
                    };
                }

                const room = aggregates.rooms[roomName];

                // Initialize this sensor within the room if not exists
                if (!room.sensors[sensorId]) {
                    room.sensors[sensorId] = {
                        name: sensorName,
                        deviceId: sensorId,
                        temperature: null,
                        humidity: null,
                        co2: null,
                        pressure: null,
                        voc: null,
                        pm1: null,
                        pm25: null,
                        pm10: null,
                        boardModel: sensor.board_model || null,
                        sensorModels: {},
                        lastUpdate: null,
                        lastSeenTimestamp: null,
                        data_source: sensor.data_source || null
                    };

                    // Set room type from first sensor's name (or update if current is unknown)
                    if (!room.roomType || room.roomType === 'unknown') {
                        room.roomType = detectRoomType(sensorName) || detectRoomType(roomName) || 'unknown';
                    }

                    // Set legacy fields from first sensor for backward compatibility
                    if (!room.sensorId) {
                        room.sensorId = sensorId;
                        room.boardModel = sensor.board_model || null;
                        room.data_source = sensor.data_source || null;
                    }
                }

                const roomSensor = room.sensors[sensorId];

                // Track lastSeenTimestamp from any reading (even stale) for sensor removal logic
                const roomReadingTypes = ['temperature', 'humidity', 'co2', 'pressure', 'voc_index', 'pm1_0', 'pm2_5', 'pm10'];
                for (const type of roomReadingTypes) {
                    const reading = readings[type];
                    if (reading?.timestamp) {
                        const ts = new Date(reading.timestamp).getTime();
                        // Update sensor's lastSeenTimestamp
                        const sensorLastSeen = roomSensor.lastSeenTimestamp ? new Date(roomSensor.lastSeenTimestamp).getTime() : 0;
                        if (ts > sensorLastSeen) {
                            roomSensor.lastSeenTimestamp = reading.timestamp;
                        }
                        // Update room's lastSeenTimestamp
                        const roomLastSeen = room.lastSeenTimestamp ? new Date(room.lastSeenTimestamp).getTime() : 0;
                        if (ts > roomLastSeen) {
                            room.lastSeenTimestamp = reading.timestamp;
                        }
                    }
                }

                // Collect sensor-specific readings - only if fresh (threshold depends on data_source)
                const roomDataSource = sensor.data_source;
                if (readings.temperature?.value != null && isReadingFresh(readings.temperature, roomDataSource)) {
                    roomSensor.temperature = readings.temperature.value;
                    roomSensor.lastUpdate = readings.temperature.timestamp;
                    if (readings.temperature.sensor_model) roomSensor.sensorModels.temperature = readings.temperature.sensor_model;
                    aggregates.indoor.temperature.values.push(readings.temperature.value);
                }
                if (readings.humidity?.value != null && isReadingFresh(readings.humidity, roomDataSource)) {
                    roomSensor.humidity = readings.humidity.value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings.humidity.timestamp;
                    if (readings.humidity.sensor_model) roomSensor.sensorModels.humidity = readings.humidity.sensor_model;
                    aggregates.indoor.humidity.values.push(readings.humidity.value);
                }
                if (readings.co2?.value != null && isReadingFresh(readings.co2, roomDataSource)) {
                    roomSensor.co2 = readings.co2.value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings.co2.timestamp;
                    if (readings.co2.sensor_model) roomSensor.sensorModels.co2 = readings.co2.sensor_model;
                }
                if (readings.pressure?.value != null && isReadingFresh(readings.pressure, roomDataSource)) {
                    roomSensor.pressure = readings.pressure.value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings.pressure.timestamp;
                    if (readings.pressure.sensor_model) roomSensor.sensorModels.pressure = readings.pressure.sensor_model;
                    aggregates.indoor.pressure.values.push(readings.pressure.value);
                }
                if (readings.voc_index?.value != null && isReadingFresh(readings.voc_index, roomDataSource)) {
                    roomSensor.voc = readings.voc_index.value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings.voc_index.timestamp;
                    if (readings.voc_index.sensor_model) roomSensor.sensorModels.voc = readings.voc_index.sensor_model;
                }
                if (readings['pm1_0']?.value != null && isReadingFresh(readings['pm1_0'], roomDataSource)) {
                    roomSensor.pm1 = readings['pm1_0'].value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings['pm1_0'].timestamp;
                    if (readings['pm1_0'].sensor_model) roomSensor.sensorModels.pm1 = readings['pm1_0'].sensor_model;
                }
                if (readings['pm2_5']?.value != null && isReadingFresh(readings['pm2_5'], roomDataSource)) {
                    roomSensor.pm25 = readings['pm2_5'].value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings['pm2_5'].timestamp;
                    if (readings['pm2_5'].sensor_model) roomSensor.sensorModels.pm25 = readings['pm2_5'].sensor_model;
                }
                if (readings.pm10?.value != null && isReadingFresh(readings.pm10, roomDataSource)) {
                    roomSensor.pm10 = readings.pm10.value;
                    if (!roomSensor.lastUpdate) roomSensor.lastUpdate = readings.pm10.timestamp;
                    if (readings.pm10.sensor_model) roomSensor.sensorModels.pm10 = readings.pm10.sensor_model;
                }

                // Update room's lastUpdate from this sensor
                if (roomSensor.lastUpdate) {
                    const sensorTs = new Date(roomSensor.lastUpdate).getTime();
                    const roomTs = room.lastUpdate ? new Date(room.lastUpdate).getTime() : 0;
                    if (sensorTs > roomTs) {
                        room.lastUpdate = roomSensor.lastUpdate;
                    }
                }
            }

            // Temperature (outdoor sensors only - indoor temps are artificially controlled)
            // Freshness threshold depends on data_source
            const sensorDataSource = sensor.data_source;
            if (readings.temperature?.value != null && isOutdoor && isReadingFresh(readings.temperature, sensorDataSource)) {
                aggregates.temperature.values.push(readings.temperature.value);
                // Store individual sensor data for sidebar
                if (!aggregates.temperature.sensors) aggregates.temperature.sensors = [];
                aggregates.temperature.sensors.push({
                    id: sensor.deviceId,
                    name: sensor.name || sensor.deviceId,
                    value: readings.temperature.value,
                    timestamp: readings.temperature.timestamp,
                    boardModel: sensor.board_model || null,
                    sensorModel: readings.temperature.sensor_model || null
                });
            }

            // Humidity (outdoor sensors only)
            if (readings.humidity?.value != null && isOutdoor && isReadingFresh(readings.humidity, sensorDataSource)) {
                aggregates.humidity.values.push(readings.humidity.value);
                // Store individual sensor data for sidebar
                if (!aggregates.humidity.sensors) aggregates.humidity.sensors = [];
                aggregates.humidity.sensors.push({
                    id: sensor.deviceId,
                    name: sensor.name || sensor.deviceId,
                    value: readings.humidity.value,
                    timestamp: readings.humidity.timestamp,
                    boardModel: sensor.board_model || null,
                    sensorModel: readings.humidity.sensor_model || null
                });
            }

            // Pressure - now collect from ALL sensors (indoor + outdoor) for better average
            if (readings.pressure?.value != null && isReadingFresh(readings.pressure, sensorDataSource)) {
                aggregates.pressure.values.push(readings.pressure.value);
                // Store individual sensor data for debugging
                if (!aggregates.pressure.sensors) aggregates.pressure.sensors = [];
                aggregates.pressure.sensors.push({
                    id: sensor.deviceId,
                    name: sensor.name || sensor.deviceId,
                    value: readings.pressure.value,
                    isIndoor: isIndoor,
                    timestamp: readings.pressure.timestamp,
                    boardModel: sensor.board_model || null,
                    sensorModel: readings.pressure.sensor_model || null
                });
                // Collect pressure trend from first sensor that has one
                if (!aggregates.pressure.trend && readings.pressure.trend) {
                    aggregates.pressure.trend = readings.pressure.trend;
                }
            }

            // CO2 - goes to indoor OR outdoor based on sensor type
            if (readings.co2?.value != null && isReadingFresh(readings.co2, sensorDataSource)) {
                aggregates.co2.values.push(readings.co2.value);
                const co2SensorModel = readings.co2.sensor_model || null;
                if (isIndoor) {
                    aggregates.indoor.co2.values.push(readings.co2.value);
                    if (co2SensorModel) aggregates.indoor.co2.sensorModels.push(co2SensorModel);
                }
                if (isOutdoor) {
                    aggregates.outdoor.co2.values.push(readings.co2.value);
                    if (co2SensorModel) aggregates.outdoor.co2.sensorModels.push(co2SensorModel);
                }
            }

            // PM2.5 - goes to indoor OR outdoor based on sensor type
            if (readings['pm2_5']?.value != null) {
                const pm25Fresh = isReadingFresh(readings['pm2_5'], sensorDataSource);
                if (pm25Fresh) {
                    aggregates.pm25.values.push(readings['pm2_5'].value);
                    const pm25SensorModel = readings['pm2_5'].sensor_model || null;
                    if (isOutdoor) {
                        aggregates.outdoor.pm25.values.push(readings['pm2_5'].value);
                        if (pm25SensorModel) aggregates.outdoor.pm25.sensorModels.push(pm25SensorModel);
                    }
                    if (isIndoor) {
                        aggregates.indoor.pm25.values.push(readings['pm2_5'].value);
                        if (pm25SensorModel) aggregates.indoor.pm25.sensorModels.push(pm25SensorModel);
                    }
                }
            }

            // VOC - goes to indoor OR outdoor based on sensor type
            if (readings.voc_index?.value != null && isReadingFresh(readings.voc_index, sensorDataSource)) {
                aggregates.voc.values.push(readings.voc_index.value);
                const vocSensorModel = readings.voc_index.sensor_model || null;
                if (isIndoor) {
                    aggregates.indoor.voc.values.push(readings.voc_index.value);
                    if (vocSensorModel) aggregates.indoor.voc.sensorModels.push(vocSensorModel);
                }
                if (isOutdoor) {
                    aggregates.outdoor.voc.values.push(readings.voc_index.value);
                    if (vocSensorModel) aggregates.outdoor.voc.sensorModels.push(vocSensorModel);
                }
            }

            // PM1.0 - goes to indoor OR outdoor based on sensor type
            if (readings['pm1_0']?.value != null && isReadingFresh(readings['pm1_0'], sensorDataSource)) {
                if (isIndoor) {
                    aggregates.indoor.pm1.values.push(readings['pm1_0'].value);
                }
                if (isOutdoor) {
                    aggregates.outdoor.pm1.values.push(readings['pm1_0'].value);
                }
            }

            // PM10 - goes to indoor OR outdoor based on sensor type
            if (readings.pm10?.value != null && isReadingFresh(readings.pm10, sensorDataSource)) {
                if (isOutdoor) {
                    aggregates.outdoor.pm10.values.push(readings.pm10.value);
                }
                if (isIndoor) {
                    aggregates.indoor.pm10.values.push(readings.pm10.value);
                }
            }

            // NOx - goes to indoor OR outdoor based on sensor type
            if (readings.nox_index?.value != null && isReadingFresh(readings.nox_index, sensorDataSource)) {
                if (isOutdoor) {
                    aggregates.outdoor.nox.values.push(readings.nox_index.value);
                }
                if (isIndoor) {
                    aggregates.indoor.nox.values.push(readings.nox_index.value);
                }
            }
        });

        // Debug: log outdoor sensor summary
        console.log(`[Aggregates] Sensors: ${sensors.length} total, ${outdoorCount} outdoor, ${freshOutdoorCount} with fresh data`);

        // Helper function to calculate stats
        const calcStats = (obj) => {
            const values = obj.values;
            if (values.length > 0) {
                obj.count = values.length;
                obj.avg = values.reduce((a, b) => a + b, 0) / values.length;
                obj.high = Math.max(...values);
                obj.low = Math.min(...values);
                obj.min = obj.low;
                obj.max = obj.high;
            }
        };

        // Calculate stats for main metrics
        ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voc'].forEach(key => {
            calcStats(aggregates[key]);
        });

        // Calculate stats for indoor metrics (all available)
        ['co2', 'voc', 'pm1', 'pm25', 'pm10', 'nox', 'temperature', 'humidity', 'pressure'].forEach(key => {
            if (aggregates.indoor[key]) calcStats(aggregates.indoor[key]);
        });

        // Calculate stats for outdoor metrics (all available)
        ['pm25', 'pm10', 'nox', 'co2', 'voc', 'pm1'].forEach(key => {
            if (aggregates.outdoor[key]) calcStats(aggregates.outdoor[key]);
        });

        // Calculate averaged values for each room from its contributing sensors
        Object.keys(aggregates.rooms).forEach(roomName => {
            const room = aggregates.rooms[roomName];
            const sensors = Object.values(room.sensors || {});

            if (sensors.length === 0) return;

            // Helper to average non-null values
            const avgMetric = (metricName) => {
                const values = sensors
                    .map(s => s[metricName])
                    .filter(v => v != null);
                return values.length > 0
                    ? values.reduce((a, b) => a + b, 0) / values.length
                    : null;
            };

            // Calculate averaged values for the room
            room.temperature = avgMetric('temperature');
            room.humidity = avgMetric('humidity');
            room.co2 = avgMetric('co2');
            room.pressure = avgMetric('pressure');
            room.voc = avgMetric('voc');
            room.pm1 = avgMetric('pm1');
            room.pm25 = avgMetric('pm25');
            room.pm10 = avgMetric('pm10');

            // Merge sensor models from all sensors for legacy compatibility
            sensors.forEach(s => {
                if (s.sensorModels) {
                    Object.assign(room.sensorModels, s.sensorModels);
                }
            });

            // Use most recent data_source for freshness threshold
            const mostRecentSensor = sensors.reduce((latest, s) => {
                if (!latest) return s;
                const latestTs = latest.lastUpdate ? new Date(latest.lastUpdate).getTime() : 0;
                const sTs = s.lastUpdate ? new Date(s.lastUpdate).getTime() : 0;
                return sTs > latestTs ? s : latest;
            }, null);
            if (mostRecentSensor) {
                room.data_source = mostRecentSensor.data_source;
            }
        });

        // Calculate averaged values for each outdoor area from its contributing sensors
        Object.keys(aggregates.outdoorAreas).forEach(areaName => {
            const area = aggregates.outdoorAreas[areaName];
            const sensors = Object.values(area.sensors || {});

            if (sensors.length === 0) return;

            // Helper to average non-null values
            const avgMetric = (metricName) => {
                const values = sensors
                    .map(s => s[metricName])
                    .filter(v => v != null);
                return values.length > 0
                    ? values.reduce((a, b) => a + b, 0) / values.length
                    : null;
            };

            // Calculate averaged values for the area
            area.temperature = avgMetric('temperature');
            area.humidity = avgMetric('humidity');
            area.pm1 = avgMetric('pm1');
            area.pm25 = avgMetric('pm25');
            area.pm10 = avgMetric('pm10');
            area.co2 = avgMetric('co2');
            area.voc = avgMetric('voc');
        });

        // NO fallback - only show data from correctly classified sensors
        // If no indoor sensors, indoor.co2 stays empty (count=0)
        // If no outdoor sensors, outdoor.pm25 stays empty (count=0)

        return aggregates;
    }

    // Old widget functions removed - replaced by updateEpaperWidget, updateTrendChart, updateEpaperSensorStatus

    _legacyUpdateSensorStatus(sensors) {
        // Kept for reference - now replaced by updateEpaperSensorStatus
        const statusGrid = document.getElementById('sensorStatusGrid');
        if (!statusGrid) return;

        statusGrid.innerHTML = sensors.map(sensor => {
            const name = sensor.name || sensor.location || sensor.locationName;
            const location = sensor.deviceId;
            const temp = sensor.readings?.temperature?.value;
            const co2 = sensor.readings?.co2?.value;

            return `
                <div class="status-card">
                    <div>
                        <div class="status-card-name">${name || location}</div>
                        <div class="status-card-location">${location}</div>
                    </div>
                    <div>
                        <div class="status-card-value">${temp != null ? temp.toFixed(1) + '°C' : '--'}</div>
                        ${co2 != null ? `<div style="font-size: 11px; color: var(--text-secondary);">${co2.toFixed(0)} ppm CO₂</div>` : ''}
                    </div>
                </div>
            `;
        }).join('');
    }
}

// Initialise map on page load
document.addEventListener('DOMContentLoaded', () => {
    window.app = new Respiro();

    // Initialize floating tooltip for metric icons
    const tooltip = document.createElement('div');
    tooltip.id = 'iconTooltip';
    document.body.appendChild(tooltip);

    document.addEventListener('mouseover', (e) => {
        const iconWrapper = e.target.closest('.icon-tooltip');
        if (iconWrapper) {
            const text = iconWrapper.getAttribute('data-tooltip');
            if (text) {
                tooltip.textContent = text;
                const rect = iconWrapper.getBoundingClientRect();
                // Position above the icon, centered
                tooltip.style.left = `${rect.left + rect.width / 2}px`;
                tooltip.style.top = `${rect.top - 6}px`;
                tooltip.style.transform = 'translate(-50%, -100%)';
                tooltip.classList.add('visible');
            }
        }
    });

    document.addEventListener('mouseout', (e) => {
        const iconWrapper = e.target.closest('.icon-tooltip');
        if (iconWrapper) {
            tooltip.classList.remove('visible');
        }
    });
});
