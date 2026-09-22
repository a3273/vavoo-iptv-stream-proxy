FROM node:24-alpine

# Installa ffmpeg (richiesto per lo streaming)
RUN apk add --no-cache ffmpeg

WORKDIR /app

# Copia package files
COPY package*.json ./
RUN npm ci --only=production

# Copia il codice
COPY . .

# Configurazione porta
ENV PORT=8888
ENV NODE_ENV=production

EXPOSE 8888

# Health check endpoint
RUN echo 'const http = require("http"); http.createServer((req, res) => { res.writeHead(200); res.end("OK"); }).listen(process.env.PORT || 8888);'
