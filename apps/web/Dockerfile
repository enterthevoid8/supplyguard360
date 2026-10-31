FROM node:22-alpine AS build
WORKDIR /srv
COPY package*.json ./
RUN npm install --no-audit --no-fund
COPY . ./
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build
FROM node:22-alpine
WORKDIR /srv
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=build /srv/node_modules ./node_modules
COPY --from=build /srv/.next ./.next
COPY --from=build /srv/public ./public
COPY --from=build /srv/package.json ./
USER node
EXPOSE 3000
CMD ["npm", "start"]
