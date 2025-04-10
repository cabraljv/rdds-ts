import * as net from 'net';
import { postgresParser } from '../utils';
import { DBInstanceController } from '../controllers/db-instance';
import { v4 as uuidv4 } from 'uuid';

type ActiveQuery = {
  query: string;
  queryId: string;
  bindValues: (string | null)[];
}

class PostgresDriverClient {
  private conn: net.Socket;
  private activeQuery: ActiveQuery | null = null;
  private dbInstanceController: DBInstanceController;
  private connectionId: string;
  private messageHandlers: Record<string, (query: Buffer) => Promise<void>>;

  constructor(conn: net.Socket, dbInstanceController: DBInstanceController) {
    this.conn = conn;
    this.dbInstanceController = dbInstanceController;
    this.connectionId = uuidv4();
    
    this.messageHandlers = {
      [postgresParser.POSTGRES_QUERY_TYPE_PARSE]: async (query: Buffer) => {
        let offset = 5;
        const nameEnd = query.indexOf(0, offset); // Statement name
        const queryStart = nameEnd + 1;
        const queryEnd = query.indexOf(0, queryStart); // End of SQL C-string
        const sql = query.slice(queryStart, queryEnd).toString('utf8');
        this.activeQuery = { query: sql, queryId: '', bindValues: [] };
      },

      [postgresParser.POSTGRES_QUERY_TYPE_BIND]: async (query: Buffer) => {
        let colOffset = 5;

        // Skip Portal name (C-string)
        const portalEnd = query.indexOf(0x00, colOffset);
        colOffset = portalEnd + 1;

        // Skip Statement name (C-string)
        const stmtEnd = query.indexOf(0x00, colOffset);
        colOffset = stmtEnd + 1;

        // Read parameter format codes
        const numFormats = query.readUInt16BE(colOffset);
        colOffset += 2;

        const formatCodes: number[] = [];
        for (let i = 0; i < numFormats; i++) {
          const code = query.readUInt16BE(colOffset);
          formatCodes.push(code);
          colOffset += 2;
        }

        // Number of parameters
        const numParams = query.readUInt16BE(colOffset);
        colOffset += 2;

        const bindValues: (string | null)[] = [];
        for (let i = 0; i < numParams; i++) {
          const len = query.readInt32BE(colOffset);
          colOffset += 4;
          if (len === -1) {
            bindValues.push(null); // NULL value
          } else {
            const param = query.slice(colOffset, colOffset + len);
            const format = formatCodes.length === 1 ? formatCodes[0] : (formatCodes[i] || 0);
            if (format === 0) {
              bindValues.push(param.toString('utf8'));
            } else {
              bindValues.push('(binary)');
            }
            colOffset += len;
          }
        }

        if (this.activeQuery) {
          this.activeQuery.bindValues = bindValues;
        }

        // BindComplete message
        const bindComplete = Buffer.alloc(5);
        bindComplete.write('2', 0); // '2' = BindComplete
        bindComplete.writeInt32BE(4, 1);
        this.conn.write(bindComplete);
      },

      [postgresParser.POSTGRES_QUERY_TYPE_FLUSH]: async () => {
        // No-op for this mock
      },

      [postgresParser.POSTGRES_QUERY_TYPE_QUERY]: async (query: Buffer) => {
        const length = query.readUInt32BE(1);
        const sqlString = query.slice(5, 5 + length - 5 - 1).toString('utf8').trim();

        console.log('📥 Client sent simple query:', sqlString);

        this.activeQuery = {
          query: sqlString,
          queryId: '',
          bindValues: []
        };

        const resultInstanceExecute: any = await this.dbInstanceController.handleLoadBalancerQuery(sqlString, this.connectionId);
        console.log('🔍 Result instance execute:', resultInstanceExecute);
        

        let queryColumns = postgresParser.extractColumnNamesFromSQL(sqlString) || ['id', 'name'];
        
        const firstRow = resultInstanceExecute?.[0];
        if(firstRow) {
          queryColumns = Object.keys(firstRow);
        }

        // Step 1: RowDescription
        const encodedCols = queryColumns.map(postgresParser.encodeColumn);
        const colCount = Buffer.alloc(2);
        colCount.writeUInt16BE(queryColumns.length, 0);
        const rowDesc = Buffer.concat([
          Buffer.from(['T'.charCodeAt(0), 0, 0, 0, 0]),
          colCount,
          ...encodedCols
        ]);
        rowDesc.writeUInt32BE(rowDesc.length - 1, 1);
        this.conn.write(rowDesc);

        // Step 2: DataRow
        for(const row of resultInstanceExecute) {
          const mockValues = queryColumns.map(col => row[col]?.toString() || null);
          const rowData = postgresParser.encodeValues(mockValues);
          const dataHeader = Buffer.alloc(5);
          dataHeader.write('D', 0);
          dataHeader.writeInt32BE(rowData.length + 4, 1);
          this.conn.write(Buffer.concat([dataHeader, rowData]));
        }

        // Step 3: CommandComplete
        let commandTag = 'SELECT 1';
        if (sqlString.toLowerCase().startsWith('insert')) {
          commandTag = 'INSERT 0 1';
        } else if (sqlString.toLowerCase().startsWith('update')) {
          commandTag = 'UPDATE 1';
        } else if (sqlString.toLowerCase().startsWith('delete')) {
          commandTag = 'DELETE 1';
        }
        
        const cmdMsg = Buffer.from(`${commandTag}\0`);
        const cmdHeader = Buffer.alloc(5);
        cmdHeader.write('C', 0);
        cmdHeader.writeInt32BE(cmdMsg.length + 4, 1);
        this.conn.write(Buffer.concat([cmdHeader, cmdMsg]));

        // Step 4: ReadyForQuery
        this.conn.write(postgresParser.readyForQuery());
      },

      [postgresParser.POSTGRES_QUERY_TYPE_DESCRIBE]: async (query: Buffer) => {
        const describeType = String.fromCharCode(query[5]);
        console.log(`📄 Describe ${describeType}`);
        
        if (describeType === 'S') {
          const descColumns = ['id', 'name', 'email'];
          const encodedColumns = descColumns.map(postgresParser.encodeColumn);
          const fieldCount = Buffer.alloc(2);
          fieldCount.writeUInt16BE(descColumns.length, 0);
          const rowDescBody = Buffer.concat([fieldCount, ...encodedColumns]);
          const rowDescHeader = Buffer.alloc(5);
          rowDescHeader.write('T', 0);
          rowDescHeader.writeInt32BE(rowDescBody.length + 4, 1);
          this.conn.write(Buffer.concat([rowDescHeader, rowDescBody]));
        } else {
          const noDescription = Buffer.from([0x6e, 0, 0, 0, 4]);
          this.conn.write(noDescription);
        }
      },

      [postgresParser.POSTGRES_QUERY_TYPE_EXECUTE]: async (query: Buffer) => {
        console.log('📦 Execute query:', this.activeQuery);
        const active = this.activeQuery;

        if (!active?.query) {
          console.warn('❌ No active query to execute');
          const ready = Buffer.alloc(6);
          ready.write('Z', 0);
          ready.writeInt32BE(5, 1);
          ready.write('I', 5);
          this.conn.write(ready);
          return;
        }

        const execColumns = postgresParser.extractColumnNamesFromSQL(active.query) || 
                          postgresParser.generateColumnNames(active.bindValues.length);

        // Step 1: RowDescription
        const encodedColumns = execColumns.map(postgresParser.encodeColumn);
        const fieldCount = Buffer.alloc(2);
        fieldCount.writeUInt16BE(execColumns.length, 0);
        const rowDescBody = Buffer.concat([fieldCount, ...encodedColumns]);
        const rowDescHeader = Buffer.alloc(5);
        rowDescHeader.write('T', 0);
        rowDescHeader.writeInt32BE(rowDescBody.length + 4, 1);
        this.conn.write(Buffer.concat([rowDescHeader, rowDescBody]));

        // Step 2: DataRow
        const rowBody = postgresParser.encodeValues(active.bindValues.filter((value): value is string => value !== null));
        const execHeader = Buffer.alloc(5);
        execHeader.write('D', 0);
        execHeader.writeInt32BE(rowBody.length + 4, 1);
        this.conn.write(Buffer.concat([execHeader, rowBody]));

        // Step 3: CommandComplete
        const completionText = Buffer.from("INSERT 0 1\0");
        const cmdCompleteHeader = Buffer.alloc(5);
        cmdCompleteHeader.write('C', 0);
        cmdCompleteHeader.writeInt32BE(completionText.length + 4, 1);
        this.conn.write(Buffer.concat([cmdCompleteHeader, completionText]));

        // Step 4: ReadyForQuery
        const ready = Buffer.alloc(6);
        ready.write('Z', 0);
        ready.writeInt32BE(5, 1);
        ready.write('I', 5);
        this.conn.write(ready);
      },

      [postgresParser.POSTGRES_QUERY_TYPE_SYNC]: async () => {
        const syncReady = Buffer.alloc(6);
        syncReady.write('Z', 0);
        syncReady.writeInt32BE(5, 1);
        syncReady.write('I', 5);
        this.conn.write(syncReady);
      },

      [postgresParser.POSTGRES_QUERY_TYPE_TERMINATE]: async () => {
        console.log("👋 Client requested connection termination");
        this.conn.end();
      }
    };
  }

  async sendAuthenticationOk(): Promise<void> {
    const messages = [
      // AuthenticationOk
      Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]),

      // ParameterStatus messages (S)
      postgresParser.createParameterStatus("server_version", "15.4-mock"),
      postgresParser.createParameterStatus("client_encoding", "UTF8"),
      postgresParser.createParameterStatus("DateStyle", "ISO, MDY"),

      // ReadyForQuery (Z)
      Buffer.from([0x5a, 0, 0, 0, 5, 0x49]) // 'I' = idle
    ];

    for (const msg of messages) {
      this.conn.write(msg);
    }

    console.log("✅ Responded with AuthenticationOk and setup parameters");
  }

  async handleQuery(query: Buffer): Promise<void> {
    if (query.length === 0) return;

    const msgType = String.fromCharCode(query[0]);
    console.log("Msg type:", msgType);

    const handler = this.messageHandlers[msgType];
    if (handler) {
      await handler(query);
      console.log("✅ Query processed successfully!");
    } else {
      console.log(`📥 Message type ${msgType} not handled`);
    }
  }

  async handleClient(): Promise<void> {
    try {
      // Read initial authentication
      const startupBuf = await new Promise<Buffer>((resolve) => {
        this.conn.once('data', resolve);
      });

      if (startupBuf.length < 8) {
        console.log("⚠ Client sent invalid request.");
        this.conn.end();
        return;
      }

      // Successful authentication
      await this.sendAuthenticationOk();
      console.log("✅ Authentication successful.");

      // Process queries
      this.conn.on('data', (data: Buffer) => {
        const messages = postgresParser.splitMessages(data);
        messages.forEach((msg) => this.handleQuery(msg));
      });

      this.conn.on('error', (err) => {
        console.log("❌ Connection error:", err);
      });

      this.conn.on('end', () => {
        console.log("❌ Connection closed by client.");
      });

    } catch (err) {
      console.log("❌ Error handling client:", err);
      this.conn.end();
    }
  }
}

export default PostgresDriverClient;