#!/usr/bin/env node
/**
 * Merge multiple geoBoundaries GeoJSON files into a single file
 * with consistent properties for PMTiles generation
 */

const fs = require('fs');
const path = require('path');

const boundariesDir = __dirname;
const outputFile = path.join(boundariesDir, 'merged_boundaries.geojson');

// Find all GeoJSON files
const files = fs.readdirSync(boundariesDir)
    .filter(f => f.endsWith('.geojson') && !f.startsWith('merged'));

console.log(`Found ${files.length} GeoJSON files to merge:`);
files.forEach(f => console.log(`  - ${f}`));

const allFeatures = [];

files.forEach(file => {
    const filePath = path.join(boundariesDir, file);
    const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));

    data.features.forEach(feature => {
        // Extract admin level from shapeType (e.g., "ADM1" -> 1)
        const adminLevel = parseInt(feature.properties.shapeType?.replace('ADM', '') || '1');

        // Create consistent properties
        const newFeature = {
            type: 'Feature',
            properties: {
                region_id: feature.properties.shapeID,
                name: feature.properties.shapeName,
                iso_code: feature.properties.shapeISO,
                country_code: feature.properties.shapeGroup,
                admin_level: adminLevel,
                parent_id: feature.properties.shapeGroup // For ADM1, parent is country
            },
            geometry: feature.geometry
        };

        allFeatures.push(newFeature);
    });
});

const merged = {
    type: 'FeatureCollection',
    features: allFeatures
};

fs.writeFileSync(outputFile, JSON.stringify(merged));

console.log(`\nMerged ${allFeatures.length} features into ${outputFile}`);

// Count by admin level
const byLevel = {};
allFeatures.forEach(f => {
    const level = f.properties.admin_level;
    byLevel[level] = (byLevel[level] || 0) + 1;
});
console.log('\nFeatures by admin level:');
Object.entries(byLevel).sort().forEach(([level, count]) => {
    console.log(`  ADM${level}: ${count} regions`);
});

// Count by country
const byCountry = {};
allFeatures.forEach(f => {
    const country = f.properties.country_code;
    byCountry[country] = (byCountry[country] || 0) + 1;
});
console.log('\nFeatures by country:');
Object.entries(byCountry).sort().forEach(([country, count]) => {
    console.log(`  ${country}: ${count} regions`);
});
