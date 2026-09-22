FROM node:24-alpine

WORKDIR /app

# Copia SOLO package.json (ignora lock per evitare conflitti)
COPY package.json ./
RUN npm install --production

# Copia il codice
COPY . .

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

CMD ["npm", "start"]
