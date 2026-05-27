FROM node:20-slim AS builder
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install
COPY src/ src/
RUN npx tsc

FROM node:20-slim
WORKDIR /app
COPY --from=builder /app/dist/ dist/
COPY --from=builder /app/node_modules/ node_modules/
COPY package.json ./
ENV KAFKAJS_NO_PARTITIONER_WARNING=1
ENTRYPOINT ["node", "dist/index.js"]
