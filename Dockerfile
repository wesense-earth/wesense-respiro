# Use Node.js LTS (Long Term Support) version
FROM node:20-slim

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application source
COPY src/ ./src/
COPY public/ ./public/

# Create data directory for optional database
RUN mkdir -p /app/data

# Expose the application port
EXPOSE 3000

# Set NODE_ENV to production
ENV NODE_ENV=production

# Run the application
CMD ["npm", "start"]
