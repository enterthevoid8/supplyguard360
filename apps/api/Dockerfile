FROM node:22-alpine AS build
WORKDIR /srv
COPY package*.json tsconfig.json ./
RUN npm install --no-audit --no-fund
COPY src ./src
RUN npm run build
FROM node:22-alpine
WORKDIR /srv
ENV NODE_ENV=production
COPY --from=build /srv/node_modules ./node_modules
COPY --from=build /srv/dist ./dist
COPY package.json ./
USER node
CMD ["node", "dist/server.js"]
