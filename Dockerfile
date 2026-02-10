# Stage 1: Build tippecanoe from source
# Needed to generate PMTiles boundary overlays for the region heatmap
FROM node:20-slim AS tippecanoe-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ make libsqlite3-dev zlib1g-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TIPPECANOE_VERSION=2.79.0
RUN git clone --depth 1 --branch ${TIPPECANOE_VERSION} \
    https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j$(nproc)

# Stage 2: Download pmtiles CLI (pre-built Go binary)
FROM node:20-slim AS pmtiles-downloader

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG PMTILES_VERSION=1.30.0
RUN ARCH=$(case "${TARGETARCH}" in (amd64) echo "x86_64";; (arm64) echo "arm64";; (*) echo "x86_64";; esac) \
    && curl -fsSL "https://github.com/protomaps/go-pmtiles/releases/download/v${PMTILES_VERSION}/go-pmtiles_${PMTILES_VERSION}_Linux_${ARCH}.tar.gz" \
    | tar -xz -C /usr/local/bin pmtiles

# Stage 3: Production image
FROM node:20-slim

# Runtime libraries needed by tippecanoe
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy tool binaries from builder stages
COPY --from=tippecanoe-builder /tmp/tippecanoe/tippecanoe /usr/local/bin/tippecanoe
COPY --from=pmtiles-downloader /usr/local/bin/pmtiles /usr/local/bin/pmtiles

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm ci --only=production

# Copy application source
COPY src/ ./src/
COPY public/ ./public/

# Create data directory for boundaries and cache
RUN mkdir -p /app/data

# Expose the application port
EXPOSE 3000

# Set NODE_ENV to production
ENV NODE_ENV=production

# Run the application
CMD ["npm", "start"]
