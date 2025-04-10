import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { RequestController } from './controller/requests';
import { RedisService } from './services/redis';
import { Pool } from 'pg';
import DatabaseService from './services/database';
import { GrpcClient } from './services/grpc-client';

console.log('Starting DB Instance');

const PROTO_PATH = path.resolve(__dirname, './sync.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  defaults: true,
  enums: String,
  longs: String,
  oneofs: true,
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
const syncPackage = protoDescriptor.sync;


function startServer() {
  const server = new grpc.Server();

  const redisService = new RedisService(
    process.env.REDIS_HOST || 'localhost',
    parseInt(process.env.REDIS_PORT || '6379'),
    process.env.REDIS_PASSWORD || 'redis'
  );
  const pg = new Pool({
    user: process.env.DB_USER || 'postgres',
    host: process.env.DB_HOST || 'localhost',
    database: process.env.DB_NAME || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
    port: parseInt(process.env.DB_PORT || '5432'),
  });
  const databaseService = new DatabaseService(pg);
  const grpcClient = new GrpcClient();
  const requestController = new RequestController(databaseService, redisService, grpcClient);

  server.addService(syncPackage.SyncService.service, {
    ExecQueryFromLoadBalancer: async (call: any, callback: any) =>
      {
        console.log('ExecQueryFromLoadBalancer', call.request);
        const queryId = call.request.queryId;
        const result = await requestController.handleLoadBalancerQuery(queryId);
        callback(null, result);
      },

    ExecQueryFromSiblingInstance: async (call: any, callback: any) =>
      {
        const queryId = call.request.queryId;
        const result = await requestController.handleSiblingInstanceQuery(queryId);
        callback(null, result);
      },

    HealthCheckDatabase: (call: any, callback: any) => {
      console.log('✔️ HealthCheck recebido');
      callback(null, {
        healthy: true,
        message: 'Database está saudável',
      });
    },
  });

  const port = `0.0.0.0:${process.env.PORT}`;
  server.bindAsync(port, grpc.ServerCredentials.createInsecure(), () => {
    console.log(`✅ Servidor gRPC ouvindo em ${port}`);
    server.start();
  });
}

startServer();