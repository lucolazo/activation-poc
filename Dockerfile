# Imagen común para los servicios NestJS del monorepo: docker build --build-arg APP=billing-service .
FROM node:20-alpine
ARG APP
ENV APP=${APP}
WORKDIR /app
COPY package.json tsconfig.base.json ./
COPY libs ./libs
COPY apps/${APP} ./apps/${APP}
RUN npm install --include-workspace-root -w libs/contracts -w libs/kafka-toolkit -w apps/${APP} \
 && npm run build -w libs/contracts -w libs/kafka-toolkit -w apps/${APP}
CMD ["sh", "-c", "node apps/$APP/dist/main.js"]