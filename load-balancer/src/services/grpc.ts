import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

type GRPCResponse = {
  success: boolean;
  resultJson: string;
}

const PROTO_PATH = path.resolve(__dirname, './sync.proto');

// Load the proto file
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// Get the proper package and service
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
// Get the service definition from your proto package
const syncService = protoDescriptor.sync.SyncService;

export class GRPCService {
  async sendDataToServer(host: string, port: number, queryId: string): Promise<GRPCResponse> {
    return new Promise((resolve, reject) => {
      const target = `${host}:${port}`;
      // Create client with the correct service reference
      const client = new syncService(target, grpc.credentials.createInsecure());
      console.log('queryId:', queryId);
      const request = {
        queryId,
      };

      client.ExecQueryFromLoadBalancer(request, (err: any, response: any) => {
        if (err) {
          console.error('gRPC Error:', err);
          reject(err);
        } else {
          resolve(response);
        }
      });
    });
  }
}