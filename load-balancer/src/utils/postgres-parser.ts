export const POSTGRES_QUERY_TYPE_PARSE = 'P';
export const POSTGRES_QUERY_TYPE_BIND = 'B';
export const POSTGRES_QUERY_TYPE_DESCRIBE = 'D';
export const POSTGRES_QUERY_TYPE_EXECUTE = 'E';
export const POSTGRES_QUERY_TYPE_SYNC = 'S';
export const POSTGRES_QUERY_TYPE_FLUSH = 'H';
export const POSTGRES_QUERY_TYPE_QUERY = 'Q';
export const POSTGRES_QUERY_TYPE_TERMINATE = 'X';
export function createParameterStatus(name: string, value: string): Buffer {
  const payload = Buffer.from(name + '\0' + value + '\0', 'utf8');
  const buf = Buffer.alloc(5);
  buf.write('S', 0);
  buf.writeInt32BE(payload.length + 4, 1);
  return Buffer.concat([buf, payload]);
}

export function splitMessages(buffer: Buffer): Buffer[] {
  const messages: Buffer[] = [];
  let offset = 0;

  while (offset < buffer.length) {
    const messageType = buffer[offset];
    const length = buffer.readUInt32BE(offset + 1);
    const message = buffer.slice(offset, offset + 1 + length); // includes type byte
    messages.push(message);
    offset += 1 + length;
  }

  return messages;
}

export function readyForQuery(): Buffer {
  const buf = Buffer.alloc(5);
  buf.write('Z', 0);
  buf.writeInt32BE(4, 1);
  return buf;
}

export function encodeValues(values: string[]): Buffer {
  const fieldCount = Buffer.alloc(2);
  fieldCount.writeUInt16BE(values.length, 0);
  const parts: Buffer[] = [fieldCount];

  for (const v of values) {
    const valBuf = Buffer.from(v);
    const len = Buffer.alloc(4);
    len.writeInt32BE(valBuf.length, 0);
    parts.push(len, valBuf);
  }

  return Buffer.concat(parts);
}

export function encodeColumn(name: string): Buffer {
  const nameBuf = Buffer.from(name + '\0');
  const metadata = Buffer.alloc(18);
  metadata.writeInt32BE(0, 0); // table OID
  metadata.writeInt16BE(0, 4); // column index
  metadata.writeInt32BE(25, 6); // type OID: 25 (TEXT)
  metadata.writeInt16BE(-1, 10); // type size: -1 (variable)
  metadata.writeInt32BE(0, 12); // type modifier
  metadata.writeInt16BE(0, 16); // format code: 0 (text)
  return Buffer.concat([nameBuf, metadata]);
}

export function generateColumnNames(n: number): string[] {
  return Array.from({ length: n }, (_, i) => `col${i + 1}`);
}
export function extractColumnNamesFromSQL(sql: string): string[] | null {
  if (/returning\s+\*/i.test(sql)) {
    // Use known columns
    return ['id', 'name', 'email'];
  }
  if (/returning\s+(.*)$/i.test(sql)) {
    const match = /returning\s+(.*)$/i.exec(sql);
    const cols = match?.[1]?.replace(/\s+/g, '').split(',') || [];
    return cols.map(c => c.replace(/["']/g, ''));
  }
  return null;
}