FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY . .

RUN mkdir -p /app/output /app/benchmarking

EXPOSE 3000

CMD ["node", "src/services/api.js"]
