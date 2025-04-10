import { Redis } from 'ioredis';
import md5 from 'md5';

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

  async getQueryFromRedis(queryId: string): Promise<RedisSqlQuery | null> {
    const query = await this.redis.get(md5(queryId));
    if (!query) return null;
    return JSON.parse(query);
  }

  getAllQueriesFromTable(tableName: string) {
  }
  addQueryToTable(tableName: string, query: RedisSqlQuery, instanceId: string) {
  }
  async setQueryAsExecutedByInstance(queryId: string, instanceId: string) {
    await this.redis.sadd(md5(`${queryId}:executed`), instanceId);
    // verify if query was executed in all instances
    const executedInstances = await this.redis.smembers(md5(`${queryId}:executed`));
    const notExecutedInstances = ALL_INSTANCES_IDS.filter(instanceId => !executedInstances.includes(instanceId));
    if (notExecutedInstances.length === 0) {
      await this.redis.del(md5(queryId));
      return true; // query was executed in all instances
    }
    return false; // query was not executed in all instances
  }
  verifyIfQueryWasExecutedInAllInstances(tableName: string, queryId: string) {
  }
  removeQueryFromQueue(tableName: string, queryId: string) {
  }
}
