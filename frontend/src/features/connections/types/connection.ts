export interface DatabaseConnection {
  conn_id: number;
  name: string;
  connection_type: string;
  host: string;
  port: number;
  database: string;
  user: string;
  created_at: string;
}

export interface DatabaseConnectionCreate {
  name: string;
  connection_type: string;
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export interface DatabaseConnectionUpdate {
  name?: string;
  connection_type?: string;
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
}
