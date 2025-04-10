import DatabaseService from "../services/database";
import { RedisService } from "../services/redis";
import { GRPCResponses } from "../utils/responses";
import { GrpcClient } from "../services/grpc-client";
const INSTANCE_ID = process.env.INSTANCE_ID || '';

export class RequestController {
  private databaseService: DatabaseService;
  private redisService: RedisService;
  private grpcClient: GrpcClient;

  constructor(databaseService: DatabaseService, redisService: RedisService, grpcClient: GrpcClient) {
    this.databaseService = databaseService;
    this.redisService = redisService;
    this.grpcClient = grpcClient;
  }

  async handleHealthCheck() {
    return this.databaseService.healthCheck();
  }

  async handleLoadBalancerQuery(queryId: string) {
    console.log('Handling load balancer query', queryId);

    // get query from redis
    // if query is a select, execute query and return result and remove query from redis
    // if query is a upsert, execute query and mark it as executed in this instance and send to other instances
    const query = await this.redisService.getQueryFromRedis(queryId);
    if (!query) {
      return GRPCResponses.error('Query not found');
    }
    console.log('Query found', query);
    if (query.type === 'select') {
      const result = await this.databaseService.executeQuery(query.query);
      return GRPCResponses.success(result);
    }

    if (query.type === 'upsert') {
      const result = await this.databaseService.executeQuery(query.query);
      const isExecutedInAllInstances = await this.redisService.setQueryAsExecutedByInstance(queryId, INSTANCE_ID);
      if (!isExecutedInAllInstances) {
        await this.grpcClient.sendQueryToAllSiblingInstances(queryId);
      }
      return GRPCResponses.success(result);
    }
    return GRPCResponses.error('Invalid query type');
  }

  async handleSiblingInstanceQuery(queryId: string) {
    console.log('Handling sibling instance query', queryId);

    const query = await this.redisService.getQueryFromRedis(queryId);
    if (!query) {
      return GRPCResponses.error('Query not found');
    }
    console.log('Query found', query);

    const executedInstances = await this.redisService.getExecutedInstancesFromQuery(queryId);
    console.log('Executed instances', executedInstances);

    const alreadyExecuted = executedInstances.includes(INSTANCE_ID);
    if (alreadyExecuted) {
      return GRPCResponses.success({message: 'Query already executed'});
    }
    const result = await this.databaseService.executeQuery(query.query);
    await this.redisService.setQueryAsExecutedByInstance(queryId, INSTANCE_ID);
    return GRPCResponses.success(result);
  }
}