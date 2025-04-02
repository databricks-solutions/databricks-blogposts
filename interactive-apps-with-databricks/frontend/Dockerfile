# Stage 1: Build the application
FROM node:18-alpine AS builder
WORKDIR /app

# Copy dependency definitions and install dependencies
COPY package.json package-lock.json* ./
RUN npm ci

# Copy all source files
COPY . .

# Build the Next.js application
RUN npm run build

# Stage 2: Run the application
FROM node:18-alpine
WORKDIR /app

# Set environment variable for production
ENV NODE_ENV production

# Copy only necessary files from builder
COPY --from=builder /app/package.json /app/package-lock.json* ./
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public

# Install only production dependencies
RUN npm ci --only=production

# Expose the port on which your app runs (default Next.js port)
EXPOSE 3000

# Start the Next.js application
CMD ["npm", "start"]
