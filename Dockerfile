FROM node:24-alpine

WORKDIR /app

# Copia package files
COPY package.json package-lock.json* ./
RUN npm ci --only=production || npm install --production

# Copia il codice
COPY . .

# Variabili ambiente
ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

# Usa lo script start dal package.json
CMD ["npm", "start"]
