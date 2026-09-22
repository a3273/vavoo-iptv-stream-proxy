FROM node:24-alpine

RUN apk add --no-cache ffmpeg

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

# Health check leggero
RUN echo 'const http=require("http");http.createServer((q,s)=>{s.end("OK")}).listen(3000)' > health.js

CMD ["sh", "-c", "node health.js & node index.js"]
