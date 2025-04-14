# --- Stage 1: Build Stage ---
# Use an official Node.js LTS (Long Term Support) Alpine image for a smaller footprint.
# Specify a Node version compatible with your project (e.g., 18, 20). Adjust as needed.
FROM node:18-alpine as build
# FROM node:20-alpine as build # Alternative LTS version

# Set the working directory inside the container for build steps.
WORKDIR /app

# Copy package.json and the lock file (package-lock.json or yarn.lock) first.
# This leverages Docker's layer caching. If these files don't change,
# Docker can reuse the cached dependency installation layer, speeding up subsequent builds.
COPY package*.json ./
# Uncomment the line below and comment out the one above if using Yarn instead of npm
# COPY package.json yarn.lock ./

# Install project dependencies using npm ci (clean install from lock file)
# This is generally preferred for CI/CD and reproducible builds over 'npm install'.
RUN npm ci
# Uncomment the line below and comment out the one above if using Yarn instead of npm
# RUN yarn install --frozen-lockfile

# Copy the rest of the frontend application source code into the container.
# This includes your src/, public/, etc. directories.
COPY . .

# Build the React application for production.
# This command executes the "build" script defined in your package.json.
# It typically creates an optimized static build in the '/app/build' directory.
RUN npm run build
# Uncomment the line below and comment out the one above if using Yarn instead of npm
# RUN yarn build

# --- Stage 2: Production Stage ---
# Use an official Nginx image (Alpine version for smaller size) to serve the built files.
FROM nginx:1.25-alpine

# Copy the static build output from the 'build' stage (created by `npm run build`)
# into the default directory Nginx serves files from (/usr/share/nginx/html).
COPY --from=build /app/build /usr/share/nginx/html

# Optional: Copy a custom Nginx configuration file.
# This is often necessary for single-page applications (SPAs) like React
# to handle client-side routing correctly. Without it, refreshing a page on a
# route like /dashboard might result in a 404 error from Nginx.
# You would need to create an `nginx.conf` file in your frontend directory.
# Example `nginx.conf` contents are commented out below.
# --- Example nginx.conf ---
# server {
#   listen 80;
#   server_name localhost; # Adjust if needed
#
#   # Serve static files directly
#   location / {
#     root /usr/share/nginx/html;
#     index index.html index.htm;
#     # Fallback for SPA routing: if file/dir not found, serve index.html
#     try_files $uri $uri/ /index.html;
#   }
#
#   # Optional: Custom error pages
#   error_page 500 502 503 504 /50x.html;
#   location = /50x.html {
#     root /usr/share/nginx/html;
#   }
# }
# --- End Example nginx.conf ---
# Uncomment the line below if you have created a custom nginx.conf in the frontend directory
# COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80, the default port Nginx listens on within the container.
EXPOSE 80

# The default command for the official nginx image is to start Nginx.
# This command keeps Nginx running in the foreground, which is standard practice for containers.
CMD ["nginx", "-g", "daemon off;"]
