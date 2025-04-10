import * as net from 'net';
import { PostgresDriverClient } from './driver';
import { DBInstanceController } from './controllers/db-instance';
import { GRPCService } from './services/grpc';
import { RedisService } from './services/redis';

function startProxy(): void {
  const server = net.createServer((clientConn) => {
    const grpcService = new GRPCService();
    console.log('GRPC Service created');
    const redisService = new RedisService(
      process.env.REDIS_HOST || 'localhost',
      parseInt(process.env.REDIS_PORT || '6379'),
      process.env.REDIS_PASSWORD || ''
    );
    const dbInstanceController = new DBInstanceController(grpcService, redisService);
    const postgresDriver = new PostgresDriverClient(clientConn, dbInstanceController);
    postgresDriver.handleClient();
  });

  server.listen(5432, '0.0.0.0', () => {
    console.log("📡 Mock PostgreSQL running on port 5432...");
  });

  server.on('error', (err) => {
    console.log("❌ Error starting Proxy:", err);
  });
}

startProxy();