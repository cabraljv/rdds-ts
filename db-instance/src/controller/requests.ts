import DatabaseService from "../services/database";
import { RedisService } from "../services/redis";
import { GRPCResponses } from "../utils/responses";

const INSTANCE_ID = process.env.INSTANCE_ID || '';

export class RequestController {
  private databaseService: DatabaseService;
  private redisService: RedisService;

  constructor(databaseService: DatabaseService, redisService: RedisService) {
    this.databaseService = databaseService;
    this.redisService = redisService;
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
        // TODO: send query to other instances
      }
      return GRPCResponses.success(result);
    }
    return GRPCResponses.error('Invalid query type');
  }

  async handleSiblingInstanceQuery(queryId: string) {
    // TODO: send query to other instances
  }
}