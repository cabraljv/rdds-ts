import { GRPCService } from "../services/grpc";
import { RedisService } from "../services/redis";
import { queryParser } from "../utils";
import DBInstances from "../utils/instances";

export class DBInstanceController {
  private grpcService: GRPCService;
  private redisService: RedisService;

  constructor(grpcService: GRPCService, redisService: RedisService) {
    this.grpcService = grpcService;
    this.redisService = redisService;
  }

  async handleLoadBalancerQuery(queryString: string, connectionId: string) {
    const queryObj = queryParser.stringQueryToObject(queryString, connectionId);
    const referencedTables = queryParser.getReferencedTablesFromQueryString(queryString);
    
    const syncedInstances = await this.redisService.getInstancesWithSyncedTable(referencedTables);
    if(syncedInstances.length === 0) {
      console.log('Not synced instances found for query', queryString);
      return;
    }
    const queryId = await this.redisService.createQuery(queryObj);

    // get random instance from syncedInstances
    const instanceToSendQuery = syncedInstances[Math.floor(Math.random() * syncedInstances.length)];
    console.log('Sending query to instance', instanceToSendQuery, 'for query', queryId);
    if(!Object.keys(DBInstances).includes(instanceToSendQuery)) {
      console.log('Instance not found in instances.json', instanceToSendQuery);
    }
    const instance = DBInstances[instanceToSendQuery as keyof typeof DBInstances];
    const response = await this.grpcService.sendDataToServer(instance.host, instance.port, queryId);
    const result = JSON.parse(response.resultJson);
    console.log('Response from instance', instanceToSendQuery, 'for query', queryId, response);
    return result;
  }
}