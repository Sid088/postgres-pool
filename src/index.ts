import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as path from 'path';
import type { ConnectionOptions } from 'tls';

import type { Connection, QueryConfig, QueryResult, QueryResultRow } from 'pg';
import { Client } from 'pg';
import type { StrictEventEmitter } from 'strict-event-emitter-types';
import { v4 } from 'uuid';

export interface SslSettings {
  /**
   * TLS options for the underlying socket connection.
   */
  ssl?: ConnectionOptions;
}

export interface SslSettingsOrAwsRdsSsl {
  /**
   * TLS options for the underlying socket connection.
   * NOTE: `aws-rds` sets up strict tls connection details for connecting to AWS RDS instances
   */
  ssl?: ConnectionOptions | 'aws-rds';
}

export interface PoolOptionsBase {
  /**
   * Number of connections to store in the pool
   */
  poolSize: number;
  /**
   * Milliseconds until an idle connection is closed and removed from the active connection pool
   */
  idleTimeoutMillis: number;
  /**
   * Milliseconds to wait for an available connection before throwing an error that no connection is available
   */
  waitForAvailableConnectionTimeoutMillis: number;
  /**
   * Milliseconds to wait to connect to postgres
   */
  connectionTimeoutMillis: number;
  /**
   * Number of retries to attempt when there's an error matching `retryConnectionErrorCodes`. A value of 0
   * will disable connection retry.
   */
  retryConnectionMaxRetries: number;
  /**
   * Milliseconds to wait between retry connection attempts after receiving a connection error with code
   * that matches `retryConnectionErrorCodes`. A value of 0 will try reconnecting immediately.
   */
  retryConnectionWaitMillis: number;
  /**
   * Error codes to trigger a connection retry. Eg. ENOTFOUND, EAI_AGAIN
   */
  retryConnectionErrorCodes: string[];
  /**
   * If connect should be retried when the database throws "the database system is starting up"
   * NOTE: This typically happens during a fail over scenario when a read-replica is being promoted to master
   */
  reconnectOnDatabaseIsStartingError: boolean;
  /**
   * Milliseconds to wait between retry connection attempts while the database is starting up. Allows you to throttle
   * how many retries should happen until databaseStartupTimeoutMillis expires. A value of 0 will
   * retry the query immediately.
   */
  waitForDatabaseStartupMillis: number;
  /**
   * If connection attempts continually return "the database system is starting up", this is the total number of milliseconds
   * to wait until an error is thrown.
   */
  databaseStartupTimeoutMillis: number;
  /**
   * If the query should be retried when the database throws "cannot execute X in a read-only transaction"
   * NOTE: This typically happens during a fail over scenario when a read-replica is being promoted to master
   */
  reconnectOnReadOnlyTransactionError: boolean;
  /**
   * Milliseconds to wait between retry queries while the connection is marked as read-only. Allows you to throttle
   * how many retries should happen until readOnlyTransactionReconnectTimeoutMillis expires. A value of 0 will
   * try reconnecting immediately.
   */
  waitForReconnectReadOnlyTransactionMillis: number;
  /**
   * If queries continually return "cannot execute X in a read-only transaction", this is the total number of
   * milliseconds to wait until an error is thrown.
   */
  readOnlyTransactionReconnectTimeoutMillis: number;
  /**
   * If the query should be retried when the database throws "Client has encountered a connection error and is not queryable"
   * NOTE: This typically happens during a fail over scenario with the cluster
   */
  reconnectOnConnectionError: boolean;
  /**
   * Milliseconds to wait between retry queries after receiving a connection error. Allows you to throttle
   * how many retries should happen until connectionReconnectTimeoutMillis expires. A value of 0 will
   * try reconnecting immediately.
   */
  waitForReconnectConnectionMillis: number;
  /**
   * If queries continually return "Client has encountered a connection error and is not queryable", this is the total number of
   * milliseconds to wait until an error is thrown.
   */
  connectionReconnectTimeoutMillis: number;
  /**
   * Specifies the regular expression to find named parameters in a query
   */
  namedParameterFindRegExp: RegExp;
  /**
   * Returns the regular expression used to replace a named parameter in a query
   */
  getNamedParameterReplaceRegExp: (namedParameter: string) => RegExp;
  /**
   * Gets the name of a named parameter without the symbols. This should correspond to the key in the query value object
   */
  getNamedParameterName: (namedParameterWithSymbols: string) => string;
  /**
   * Throw an error if a query takes longer than the specified milliseconds
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  query_timeout?: number;
  /**
   * Abort a query statement if it takes longer than the specified milliseconds
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  statement_timeout?: number;
  /**
   * Specifies max uses of open connection.
   */
  maxUses: number;
}

export interface PoolOptionsExplicit {
  host: string;
  database: string;
  user?: string;
  password?: string | (() => Promise<string> | string) | undefined;
  port?: number;
  poolSize?: number;
  idleTimeoutMillis?: number;
  waitForAvailableConnectionTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
  retryConnectionMaxRetries?: number;
  retryConnectionWaitMillis?: number;
  retryConnectionErrorCodes?: string[];
  reconnectOnDatabaseIsStartingError?: boolean;
  waitForDatabaseStartupMillis?: number;
  databaseStartupTimeoutMillis?: number;
  reconnectOnReadOnlyTransactionError?: boolean;
  waitForReconnectReadOnlyTransactionMillis?: number;
  readOnlyTransactionReconnectTimeoutMillis?: number;
  reconnectOnConnectionError?: boolean;
  waitForReconnectConnectionMillis?: number;
  connectionReconnectTimeoutMillis?: number;
  namedParameterFindRegExp?: RegExp;
  getNamedParameterReplaceRegExp?: (namedParameter: string) => RegExp;
  getNamedParameterName?: (namedParameterWithSymbols: string) => string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  query_timeout?: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  statement_timeout?: number;
}

export interface PoolOptionsImplicit {
  connectionString: string;
  poolSize?: number;
  idleTimeoutMillis?: number;
  waitForAvailableConnectionTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
  retryConnectionMaxRetries?: number;
  retryConnectionWaitMillis?: number;
  retryConnectionErrorCodes?: string[];
  reconnectOnDatabaseIsStartingError?: boolean;
  waitForDatabaseStartupMillis?: number;
  databaseStartupTimeoutMillis?: number;
  reconnectOnReadOnlyTransactionError?: boolean;
  waitForReconnectReadOnlyTransactionMillis?: number;
  readOnlyTransactionReconnectTimeoutMillis?: number;
  reconnectOnConnectionError?: boolean;
  waitForReconnectConnectionMillis?: number;
  connectionReconnectTimeoutMillis?: number;
  namedParameterFindRegExp?: RegExp;
  getNamedParameterReplaceRegExp?: (namedParameter: string) => RegExp;
  getNamedParameterName?: (namedParameterWithSymbols: string) => string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  query_timeout?: number;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  statement_timeout?: number;
}

export type PoolClient = Client & {
  uniqueId: string;
  idleTimeoutTimer?: NodeJS.Timer;
  release: (removeConnection?: boolean) => Promise<void>;
  errorHandler: (err: Error) => void;
  poolUseCount?: number;
};

export type PoolClientWithConnection = PoolClient & {
  connection?: Connection;
};

export interface ConnectionAddedToPoolParams {
  connectionId: PoolClient['uniqueId'];
  retryAttempt: number;
  startTime: bigint;
}

interface PoolEvents {
  connectionRequestQueued: () => void;
  connectionRequestDequeued: () => void;
  connectionAddedToPool: (params: ConnectionAddedToPoolParams) => void;
  connectionRemovedFromPool: () => void;
  connectionIdle: () => void;
  connectionRemovedFromIdlePool: () => void;
  idleConnectionActivated: () => void;
  queryDeniedForReadOnlyTransaction: () => void;
  queryDeniedForConnectionError: () => void;
  waitingForDatabaseToStart: () => void;
  retryConnectionOnError: () => void;
  error: (error: Error, client?: PoolClient) => void;
}

type PoolEmitter = StrictEventEmitter<EventEmitter, PoolEvents>;

export class Pool extends (EventEmitter as new () => PoolEmitter) {
  /**
   * Gets the number of queued requests waiting for a database connection
   */
  public get waitingCount(): number {
    return this.connectionQueue.length;
  }

  /**
   * Gets the number of idle connections
   */
  public get idleCount(): number {
    return this.idleConnections.length;
  }

  /**
   * Gets the total number of connections in the pool
   */
  public get totalCount(): number {
    return this.connections.length;
  }

  protected options: PoolOptionsBase & SslSettings & (PoolOptionsExplicit | PoolOptionsImplicit);

  // Internal event emitter used to handle queued connection requests
  protected connectionQueueEventEmitter: EventEmitter;

  protected connections: string[] = [];

  // Should self order by idle timeout ascending
  protected idleConnections: PoolClient[] = [];

  protected connectionQueue: string[] = [];

  protected isEnding = false;

  public constructor(options: SslSettingsOrAwsRdsSsl & (PoolOptionsExplicit | PoolOptionsImplicit)) {
    // eslint-disable-next-line constructor-super
    super();

    const defaultOptions: PoolOptionsBase = {
      poolSize: 10,
      idleTimeoutMillis: 10000,
      waitForAvailableConnectionTimeoutMillis: 90000,
      connectionTimeoutMillis: 30000,
      retryConnectionMaxRetries: 5,
      retryConnectionWaitMillis: 100,
      retryConnectionErrorCodes: ['ENOTFOUND', 'EAI_AGAIN'],
      reconnectOnDatabaseIsStartingError: true,
      waitForDatabaseStartupMillis: 0,
      databaseStartupTimeoutMillis: 90000,
      reconnectOnReadOnlyTransactionError: true,
      waitForReconnectReadOnlyTransactionMillis: 0,
      readOnlyTransactionReconnectTimeoutMillis: 90000,
      reconnectOnConnectionError: true,
      waitForReconnectConnectionMillis: 0,
      connectionReconnectTimeoutMillis: 90000,
      namedParameterFindRegExp: /@([\w])+\b/g,
      getNamedParameterReplaceRegExp(namedParameter: string): RegExp {
        // eslint-disable-next-line security/detect-non-literal-regexp
        return new RegExp(`@${namedParameter}\\b`, 'gm');
      },
      getNamedParameterName(namedParameterWithSymbols: string): string {
        // Remove leading @ symbol
        return namedParameterWithSymbols.substring(1);
      },
      maxUses: Infinity,
    };

    const { ssl, ...otherOptions } = options;

    this.options = { ...defaultOptions, ...otherOptions };

    if (ssl === 'aws-rds') {
      this.options.ssl = {
        rejectUnauthorized: true,
        // eslint-disable-next-line security/detect-non-literal-fs-filename
        ca: fs.readFileSync(path.join(__dirname, './certs/rds-ca-2019-root.pem')),
        minVersion: 'TLSv1.2',
      };
    } else {
      this.options.ssl = ssl;
    }

    this.connectionQueueEventEmitter = new EventEmitter();
  }

  /**
   * Gets a client connection from the pool.
   * Note: You must call `.release()` when finished with the client connection object. That will release the connection back to the pool to be used by other requests.
   */
  public async connect(): Promise<PoolClient> {
    if (this.isEnding) {
      throw new Error('Cannot use pool after calling end() on the pool');
    }

    const idleConnection = this.idleConnections.shift();
    if (idleConnection) {
      if (idleConnection.idleTimeoutTimer) {
        clearTimeout(idleConnection.idleTimeoutTimer);
      }

      this.emit('idleConnectionActivated');

      return idleConnection;
    }

    const id = v4();

    if (this.connections.length < this.options.poolSize) {
      this.connections.push(id);

      try {
        const connection = await this._createConnection(id);
        return connection;
      } catch (ex) {
        // Remove the connection id since we failed to connect
        const connectionIndex = this.connections.indexOf(id);
        if (connectionIndex > -1) {
          this.connections.splice(connectionIndex, 1);
        }

        throw ex;
      }
    }

    this.emit('connectionRequestQueued');
    this.connectionQueue.push(id);
    let connectionTimeoutTimer: NodeJS.Timeout | null = null;

    return (await Promise.race([
      new Promise((resolve) => {
        this.connectionQueueEventEmitter.on(`connection_${id}`, (client: PoolClient) => {
          if (connectionTimeoutTimer) {
            clearTimeout(connectionTimeoutTimer);
          }

          this.connectionQueueEventEmitter.removeAllListeners(`connection_${id}`);

          this.emit('connectionRequestDequeued');
          resolve(client);
        });
      }),
      // eslint-disable-next-line promise/param-names
      new Promise((_resolve, reject) => {
        connectionTimeoutTimer = setTimeout(() => {
          this.connectionQueueEventEmitter.removeAllListeners(`connection_${id}`);

          // Remove this connection attempt from the connection queue
          const index = this.connectionQueue.indexOf(id);
          if (index > -1) {
            this.connectionQueue.splice(index, 1);
          }

          reject(new Error('Timed out while waiting for available connection in pool'));
        }, this.options.waitForAvailableConnectionTimeoutMillis);
      }),
    ])) as PoolClient;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public query<TRow extends QueryResultRow = any, I extends any[] = any[]>(queryConfig: QueryConfig<I>, reconnectQueryStartTime?: [number, number]): Promise<QueryResult<TRow>> {
    return this._query(queryConfig, reconnectQueryStartTime);
  }

  /**
   * Drains the pool of all active client connections. Used to shut down the pool down cleanly
   */
  public end(): Promise<void> {
    this.isEnding = true;

    return this._drainAllIdle();
  }

  private async _drainAllIdle(): Promise<void> {
    await Promise.all([...this.idleConnections].map((idleConnection: PoolClient) => this._removeConnection(idleConnection)));
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async _query<TRow extends QueryResultRow = any, I extends any[] = any[]>(queryConfig: QueryConfig<I>, reconnectQueryStartTime?: [number, number]): Promise<QueryResult<TRow>> {
    const connection = await this.connect();
    let removeConnection = false;
    let timeoutError: Error | undefined;
    let connectionError: Error | undefined;

    try {
      const results = await connection.query<TRow>(queryConfig);
      return results;
    } catch (ex) {
      const { message } = ex as Error;
      if (this.options.reconnectOnReadOnlyTransactionError && /cannot execute [\s\w]+ in a read-only transaction/giu.test(message)) {
        timeoutError = ex as Error;
        removeConnection = true;
      } else if (this.options.reconnectOnConnectionError && /Client has encountered a connection error and is not queryable/giu.test(message)) {
        connectionError = ex as Error;
        removeConnection = true;
      } else {
        throw ex;
      }
    } finally {
      await connection.release(removeConnection);
    }

    // If we get here, that means that the query was attempted with a read-only connection.
    // This can happen when the cluster fails over to a read-replica
    if (timeoutError) {
      this.emit('queryDeniedForReadOnlyTransaction');
    } else if (connectionError) {
      // This can happen when a cluster fails over
      this.emit('queryDeniedForConnectionError');
    }

    // Clear all idle connections and try the query again with a fresh connection
    await this._drainAllIdle();

    if (!reconnectQueryStartTime) {
      // eslint-disable-next-line no-param-reassign
      reconnectQueryStartTime = process.hrtime();
    }

    if (timeoutError && this.options.waitForReconnectReadOnlyTransactionMillis > 0) {
      await new Promise<void>((resolve) => {
        setTimeout(() => {
          resolve();
        }, this.options.waitForReconnectReadOnlyTransactionMillis);
      });
    }

    if (connectionError && this.options.waitForReconnectConnectionMillis > 0) {
      await new Promise<void>((resolve) => {
        setTimeout(() => {
          resolve();
        }, this.options.waitForReconnectConnectionMillis);
      });
    }

    const diff = process.hrtime(reconnectQueryStartTime);
    const timeSinceLastRun = Number((diff[0] * 1e3 + diff[1] * 1e-6).toFixed(3));

    if (timeoutError && timeSinceLastRun > this.options.readOnlyTransactionReconnectTimeoutMillis) {
      throw timeoutError;
    }

    if (connectionError && timeSinceLastRun > this.options.connectionReconnectTimeoutMillis) {
      throw connectionError;
    }

    const results = await this._query(queryConfig, reconnectQueryStartTime);
    return results;
  }

  /**
   * Creates a new client connection to add to the pool
   * @param {string} connectionId
   * @param {number} [retryAttempt=0]
   * @param {bigint} [createConnectionStartTime] - High-resolution time (in nanoseconds) for when the connection was created
   * @param {[number,number]} [databaseStartupStartTime] - hrtime when the db was first listed as starting up
   */
  private async _createConnection(
    connectionId: string,
    retryAttempt = 0,
    createConnectionStartTime: bigint = process.hrtime.bigint(),
    databaseStartupStartTime?: [number, number],
  ): Promise<PoolClient> {
    const client = new Client(this.options) as PoolClient;
    client.uniqueId = connectionId;
    /**
     * Releases the client connection back to the pool, to be used by another query.
     *
     * @param {boolean} [removeConnection=false]
     */
    client.release = async (removeConnection = false): Promise<void> => {
      client.poolUseCount = (client.poolUseCount || 0) + 1;

      if (this.isEnding || removeConnection || client.poolUseCount >= this.options.maxUses) {
        await this._removeConnection(client);
        return;
      }

      const id = this.connectionQueue.shift();

      // Return the connection to be used by a queued request
      if (id) {
        this.connectionQueueEventEmitter.emit(`connection_${id}`, client);
      } else if (this.options.idleTimeoutMillis > 0) {
        client.idleTimeoutTimer = setTimeout((): void => {
          // eslint-disable-next-line no-void
          void this._removeConnection(client);
        }, this.options.idleTimeoutMillis);

        this.idleConnections.push(client);
        this.emit('connectionIdle');
      } else {
        await this._removeConnection(client);
      }
    };

    client.errorHandler = (err: Error): void => {
      // fire and forget, we will always emit the error.
      // eslint-disable-next-line no-void
      void this._removeConnection(client).finally(() => this.emit('error', err, client));
    };

    client.on('error', client.errorHandler);
    let connectionTimeoutTimer: NodeJS.Timeout | null = null;

    try {
      await Promise.race([
        (async function connectClient(): Promise<void> {
          try {
            await client.connect();
          } finally {
            if (connectionTimeoutTimer) {
              clearTimeout(connectionTimeoutTimer);
            }
          }
        })(),
        // eslint-disable-next-line promise/param-names
        new Promise((_resolve, reject) => {
          connectionTimeoutTimer = setTimeout((): void => {
            reject(new Error('Timed out trying to connect to postgres'));
          }, this.options.connectionTimeoutMillis);
        }),
      ]);

      this.emit('connectionAddedToPool', {
        connectionId,
        retryAttempt,
        startTime: createConnectionStartTime,
      });
    } catch (ex) {
      const { connection } = client as PoolClientWithConnection;
      if (connection) {
        // Force a disconnect of the socket, if it exists.
        connection.stream.destroy();
      }

      await client.end();

      const { message, code } = ex as Error & { code: string };
      let retryConnection = false;
      if (this.options.retryConnectionMaxRetries) {
        if (code) {
          retryConnection = this.options.retryConnectionErrorCodes.includes(code);
        } else {
          for (const errorCode of this.options.retryConnectionErrorCodes) {
            if (message.includes(errorCode)) {
              retryConnection = true;
              break;
            }
          }
        }
      }

      if (retryConnection && retryAttempt < this.options.retryConnectionMaxRetries) {
        this.emit('retryConnectionOnError');

        if (this.options.retryConnectionWaitMillis > 0) {
          await new Promise<void>((resolve) => {
            setTimeout((): void => {
              resolve();
            }, this.options.retryConnectionWaitMillis);
          });
        }

        const connectionAfterRetry = await this._createConnection(connectionId, retryAttempt + 1, createConnectionStartTime, databaseStartupStartTime);
        return connectionAfterRetry;
      }

      if (this.options.reconnectOnDatabaseIsStartingError && /the database system is starting up/giu.test(message)) {
        this.emit('waitingForDatabaseToStart');

        if (!databaseStartupStartTime) {
          // eslint-disable-next-line no-param-reassign
          databaseStartupStartTime = process.hrtime();
        }

        if (this.options.waitForDatabaseStartupMillis > 0) {
          await new Promise<void>((resolve) => {
            setTimeout((): void => {
              resolve();
            }, this.options.waitForDatabaseStartupMillis);
          });
        }

        const diff = process.hrtime(databaseStartupStartTime);
        const timeSinceFirstConnectAttempt = Number((diff[0] * 1e3 + diff[1] * 1e-6).toFixed(3));

        if (timeSinceFirstConnectAttempt > this.options.databaseStartupTimeoutMillis) {
          throw ex;
        }

        const connectionAfterRetry = await this._createConnection(connectionId, 0, createConnectionStartTime, databaseStartupStartTime);
        return connectionAfterRetry;
      }

      throw ex;
    }

    return client;
  }

  /**
   * Removes the client connection from the pool and tries to gracefully shut it down
   * @param {PoolClient} client
   */
  private async _removeConnection(client: PoolClient): Promise<void> {
    client.removeListener('error', client.errorHandler);
    // Ignore any errors when ending the connection
    client.on('error', (): void => {
      // NOOP
    });

    if (client.idleTimeoutTimer) {
      clearTimeout(client.idleTimeoutTimer);
    }

    const idleConnectionIndex = this.idleConnections.findIndex((connection) => connection.uniqueId === client.uniqueId);
    if (idleConnectionIndex > -1) {
      this.idleConnections.splice(idleConnectionIndex, 1);
      this.emit('connectionRemovedFromIdlePool');
    }

    const connectionIndex = this.connections.indexOf(client.uniqueId);
    if (connectionIndex > -1) {
      this.connections.splice(connectionIndex, 1);
    }

    try {
      await client.end();
    } catch (ex) {
      const { message } = ex as Error;
      if (!/This socket has been ended by the other party/giu.test(message)) {
        this.emit('error', ex as Error);
      }
    }

    this.emit('connectionRemovedFromPool');
  }
}
