import { Redis } from 'ioredis';
import md5 from 'md5';
import { v4 as uuidv4 } from 'uuid';

/*
tableName -> Lista de queries em JSON
{
  query: 'SELECT * FROM users',
  connectionId: '' // Id único da conexão
  timestamp: '' // Timestamp da query
  queryId: '' // Id único da query
}

queryId -> Lista de instancias em que foi executada

Quando uma query for executada em todos os DB ela sai da lista de queries

*/
const ALL_INSTANCES_IDS = process.env.ALL_INSTANCES_IDS?.split(',') || [];

type RedisSqlQuery = {
  query: string;
  connectionId: string;
  type: 'select' | 'upsert';
  timestamp: string;
  queryId: string;
}


export class RedisService {
  private redis: Redis;

  constructor(host: string, port: number, password: string) {
    this.redis = new Redis({
      host,
      port,
      password,
    });
  }

  async createQuery(queryObj: RedisSqlQuery) {
    const queryId = uuidv4();
    await this.redis.set(md5(queryId), JSON.stringify(queryObj));
    return queryId;
  }

  async getQueriesNotExecutedInAllInstancesFromTable(tableName: string) {
    const queries = await this.redis.get(md5(tableName));
    console.log('Queries:', queries);
    if(!queries) return {};
    let queriesNotExecutedInAllInstances: { [key: string]: string[] } = {};
    for(const queryId of queries) {
      const executedInstancesList = await this.redis.smembers(md5(`${queryId}:executed`));
      if(!executedInstancesList) continue;
      queriesNotExecutedInAllInstances[queryId] = executedInstancesList
    }
    return queriesNotExecutedInAllInstances;
  }

  async getInstancesWithSyncedTable(tables: string[]) {
    let syncedInstances: string[] = ALL_INSTANCES_IDS;
    for(const table of tables) {
      const queriesAvailableForTable = await this.getQueriesNotExecutedInAllInstancesFromTable(table);
      for(const queryId in queriesAvailableForTable) {
        const executedTables = queriesAvailableForTable[queryId];
        syncedInstances = syncedInstances.filter(instance => executedTables.includes(instance));
      }
    }
    console.log('Synced instances:', syncedInstances);
    return syncedInstances;
  }
}
