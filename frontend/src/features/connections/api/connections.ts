import client from '../../../shared/api/client';
import type { DatabaseConnection, DatabaseConnectionCreate, DatabaseConnectionUpdate } from '../types/connection';

export async function listConnections(): Promise<DatabaseConnection[]> {
  const { data } = await client.get('/database-connections');
  return data;
}

export async function getConnection(connId: number): Promise<DatabaseConnection> {
  const { data } = await client.get(`/database-connections/${connId}`);
  return data;
}

export async function createConnection(body: DatabaseConnectionCreate): Promise<DatabaseConnection> {
  const { data } = await client.post('/database-connections', body);
  return data;
}

export async function updateConnection(connId: number, body: DatabaseConnectionUpdate): Promise<DatabaseConnection> {
  const { data } = await client.patch(`/database-connections/${connId}`, body);
  return data;
}

export async function deleteConnection(connId: number): Promise<void> {
  await client.delete(`/database-connections/${connId}`);
}

export async function testConnection(connId: number): Promise<{ status: string; message: string }> {
  const { data } = await client.post(`/database-connections/${connId}/test`);
  return data;
}
