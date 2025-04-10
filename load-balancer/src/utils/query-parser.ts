import { v4 as uuidv4 } from 'uuid';
import { parse } from 'pgsql-ast-parser';

type RedisSqlQuery = {
  query: string;
  connectionId: string;
  type: 'select' | 'upsert';
  timestamp: string;
  queryId: string;
};



export function stringQueryToObject(queryString: string, connectionId: string): RedisSqlQuery {
  let queryType: 'select' | 'upsert' = 'upsert';
  try {
    const ast = parse(queryString);
    // Assuming ast is an array of statements
    const mainStatement = ast[0];
    if (mainStatement.type === 'select') {
      queryType = 'select';
    } else {
      queryType = 'upsert';
    }
  } catch (err) {
    console.warn('Failed to parse SQL query, defaulting to upsert:', err);
    queryType = 'upsert'; // fallback
  }
  return {
    query: queryString,
    connectionId,
    type: queryType,
    timestamp: new Date().toISOString(),
    queryId: uuidv4(),
  };
}

export function getReferencedTablesFromQueryString(queryString: string): string[] {
  try {
    const ast = parse(queryString);
    const tables: Set<string> = new Set();

    function extractTables(node: any): void {
      if (!node) return;

      if (Array.isArray(node)) {
        node.forEach(extractTables);
        return;
      }

      if (node.from) {
        node.from.forEach((fromItem: any) => {
          if (fromItem.type === 'table') {
            tables.add(fromItem.name.name);
          } else if (fromItem.type === 'join') {
            extractTables(fromItem);
          }
        });
      }

      for (const key in node) {
        if (typeof node[key] === 'object') {
          extractTables(node[key]);
        }
      }
    }

    ast.forEach(extractTables);
    return Array.from(tables);
  } catch (err) {
    console.warn('Failed to parse SQL query:', err);
    return [];
  }
}
