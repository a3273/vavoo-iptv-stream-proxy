FROM node:24-alpine

WORKDIR /app

# Copia SOLO package.json (ignora lock per evitare conflitti)
COPY package.json ./
RUN npm install --production

# Copia il codice
COPY . .

ENV NODE_ENV=production
ENV PORT=8888

EXPOSE 8888

CMD ["npm", "start"]
