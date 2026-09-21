FROM node:24-alpine

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci --no-audit --no-fund

COPY . .

ENV PORT=8888

EXPOSE 8888

CMD ["npm", "start"]
