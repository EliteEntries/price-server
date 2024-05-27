# Use the latest Node.js image as the base
FROM node:latest

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the TypeScript source code to the working directory
COPY . .

# Expose the port that the application will listen on
EXPOSE 3000

# Set Environment Variables
ENV GOOGLE_APPLICATION_CREDENTIALS="./util/firebase.json"

# Start the application
RUN npm run start