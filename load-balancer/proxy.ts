import net from 'net';

// Configuration
const LOCAL_HOST = '0.0.0.0';
const LOCAL_PORT = 5433;

const PG_HOST = 'postgres';
const PG_PORT = 5432;

const BUFFER_SIZE = 4096;

function hexdump(buffer: Buffer): string {
  const chunkSize = 16;
  let output = '';

  for (let i = 0; i < buffer.length; i += chunkSize) {
    const chunk = buffer.slice(i, i + chunkSize);
    const hex = Array.from(chunk).map(b => b.toString(16).padStart(2, '0')).join(' ');
    const ascii = Array.from(chunk).map(b => (b >= 0x20 && b < 0x7f) ? String.fromCharCode(b) : '.').join('');
    output += `${i.toString(16).padStart(4, '0')}  ${hex.padEnd(chunkSize * 3)}  ${ascii}\n`;
  }

  return output;
}


function tryDecodeUtf8Label(data: Buffer): string {
  try {
    const asText = data.toString('utf8');
    // Only return human-readable characters
    if (/[\x00-\x08\x0E-\x1F]/.test(asText)) {
      // Contains control characters → likely binary
      return '';
    }
    return asText;
  } catch {
    return '';
  }
}

function handleConnection(clientSocket: net.Socket) {
  const serverSocket = net.createConnection({ host: PG_HOST, port: PG_PORT });

  console.log(`[+] New client connected from ${clientSocket.remoteAddress}:${clientSocket.remotePort}`);

  // Pipe client → server
  clientSocket.on('data', (data) => {
    const decoded = tryDecodeUtf8Label(data);
    if (decoded) {
      console.log(decoded);
    } else {
      console.log(hexdump(data));
    }
    serverSocket.write(data);
  });

  // Pipe server → client
  serverSocket.on('data', (data) => {
    console.log(`\n[Server → Client](${data.length} bytes)`);
    const decoded = tryDecodeUtf8Label(data);
    if (decoded) {
      console.log(decoded);
    } else {
      console.log(hexdump(data));
    }
    clientSocket.write(data);
  });

  // Handle closures and errors
  const closeConnection = () => {
    clientSocket.destroy();
    serverSocket.destroy();
  };

  clientSocket.on('error', (err) => {
    console.error(`[!] Client Error: ${err.message}`);
    closeConnection();
  });

  serverSocket.on('error', (err) => {
    console.error(`[!] Server Error: ${err.message}`);
    closeConnection();
  });

  clientSocket.on('close', () => {
    console.log('[*] Client connection closed');
    closeConnection();
  });

  serverSocket.on('close', () => {
    console.log('[*] Server connection closed');
    closeConnection();
  });
}

// Start Proxy Server
const proxyServer = net.createServer(handleConnection);

proxyServer.listen(LOCAL_PORT, LOCAL_HOST, () => {
  console.log(`[+] PostgreSQL proxy listening on ${LOCAL_HOST}:${LOCAL_PORT}`);
  console.log(`[!] Forwarding to ${PG_HOST}:${PG_PORT}`);
});